package manager

import (
	pkgrpc "github.com/Nextsummer/micro/pkg/grpc"
	"github.com/Nextsummer/micro/pkg/log"
	"github.com/Nextsummer/micro/pkg/queue"
	"github.com/Nextsummer/micro/pkg/utils"
	"github.com/Nextsummer/micro/server/config"
	"github.com/Nextsummer/micro/server/node/persist"
	cmap "github.com/orcaman/concurrent-map/v2"
	"sync"
)

var controllerCandidateOnce sync.Once
var controllerCandidate *ControllerCandidate

type ControllerCandidate struct {
	voteRound              int32                                           // 投票轮次
	currentVote            pkgrpc.ControllerVote                           // 当前的一个投票
	slotsAllocation        cmap.ConcurrentMap[int32, *queue.Array[string]] // 槽位分配数据
	slotsReplicaAllocation cmap.ConcurrentMap[int32, *queue.Array[string]]
	replicaNodeIds         cmap.ConcurrentMap[int32, int32]
}

func GetControllerCandidateInstance() *ControllerCandidate {
	controllerCandidateOnce.Do(func() {
		controllerCandidate = &ControllerCandidate{
			slotsAllocation:        cmap.NewWithCustomShardingFunction[int32, *queue.Array[string]](Int32HashCode),
			slotsReplicaAllocation: cmap.NewWithCustomShardingFunction[int32, *queue.Array[string]](Int32HashCode),
			replicaNodeIds:         cmap.NewWithCustomShardingFunction[int32, int32](Int32HashCode),
		}
	})
	return controllerCandidate
}

// Initiate controller election
func (c *ControllerCandidate) electController() int {
	nodeId := config.GetConfigurationInstance().NodeId

	otherControllerCandidates := GetRemoteServerNodeManagerInstance().getOtherControllerCandidates()
	log.Info.Println("Other Controller candidates include: ", utils.ToJson(otherControllerCandidates))
	// Initialize your ballot for the first round
	c.voteRound = 1
	c.currentVote = pkgrpc.ControllerVote{VoterNodeId: nodeId, ControllerNodeId: nodeId, VoteRound: c.voteRound}

	success := false
	controllerNodeId := int32(0)

	// Start a new round of voting
	controllerNodeIdTemp, ok := c.startNextRoundVote(otherControllerCandidates)
	success = ok
	controllerNodeId = controllerNodeIdTemp
	// If the first ballot doesn't figure out who the controller is
	for !success {
		controllerNodeIdTemp, ok := c.startNextRoundVote(otherControllerCandidates)
		success = ok
		controllerNodeId = controllerNodeIdTemp
	}

	// Found out who the controller is by voting
	if nodeId == controllerNodeId {
		return controller
	}
	return candidate
}

// start the next round of voting
func (c *ControllerCandidate) startNextRoundVote(otherControllerCandidates []pkgrpc.RemoteServerNode) (int32, bool) {

	log.Info.Printf("Voting begins in Round: %d", c.voteRound)
	networkManager := GetServerNetworkManagerInstance()
	messageReceiver := GetServerMessageReceiverInstance()

	// Define the number of quorum, such as three controller candidates
	// quorum = 3 / 2 + 1 = 2
	candidateCount := 1 + len(otherControllerCandidates)
	quorum := candidateCount/2 + 1

	votes := queue.NewArray[pkgrpc.ControllerVote]()
	votes.Put(c.currentVote)

	for _, remoteServerNode := range otherControllerCandidates {
		networkManager.sendMessage(remoteServerNode.NodeId, pkgrpc.MessageEntity_VOTE, utils.Encode(&c.currentVote))
		log.Info.Println("Sends the Controller to vote for the server node: ", utils.ToJson(remoteServerNode))
	}

	// In the current round of voting, start waiting for someone else's vote
	for IsRunning() {
		receivedVote, ok := messageReceiver.voteReceiveQueue.Take()
		if !ok {
			continue
		}
		// Summarize the votes received
		votes.Put(*receivedVote)
		log.Info.Println("Received the controller election vote from the server node: ", utils.ToJson(receivedVote))

		// If the total number of votes is found to be greater than or equal to the number of quorum, a decision can be made
		if votes.Size() >= quorum {
			judgedControllerNodeId, ok := getControllerFromVotes(*votes, quorum)
			if ok {
				if votes.Size() == candidateCount {
					log.Info.Println("Identify who the Controller is: ", judgedControllerNodeId)
					return judgedControllerNodeId, true
				}
				log.Info.Printf("Identify who the Controller is [%d], but not all the votes have been received", judgedControllerNodeId)
			} else {
				log.Info.Println("We don't know who the Controller is yet: ", utils.ToJson(votes.Iter()))
			}
		}

		if votes.Size() == candidateCount {
			// All candidates' ballots have been received, and the controller has not yet been decided.
			// This round of elections is lost.
			// Adjust who you're voting for in the next round and find the one with the biggest id among the current candidates.
			c.voteRound++
			betterControllerNodeId := getBetterControllerNodeId(*votes)
			c.currentVote = pkgrpc.ControllerVote{VoterNodeId: config.GetConfigurationInstance().NodeId, ControllerNodeId: betterControllerNodeId, VoteRound: c.voteRound}
			log.Info.Println("Failed on this ballot, try to create a better ballot: ", utils.ToJson(c.currentVote))
			break
		}
	}

	return 0, false
}

// Gets the id of a controller node based on the existing votes
func getControllerFromVotes(votes queue.Array[pkgrpc.ControllerVote], quorum int) (int32, bool) {
	voteCountMap := make(map[int32]int)

	for _, vote := range votes.Iter() {
		count, ok := voteCountMap[vote.GetControllerNodeId()]
		if !ok {
			count = 0
		}
		count++
		voteCountMap[vote.GetControllerNodeId()] = count
	}

	for controllerNodeId, count := range voteCountMap {
		if count >= quorum {
			return controllerNodeId, true
		}
	}
	return 0, false
}

// Get the largest controller id from the ballot
func getBetterControllerNodeId(votes queue.Array[pkgrpc.ControllerVote]) int32 {
	betterControllerNodeId := int32(0)
	for _, vote := range votes.Iter() {
		controllerNodeId := vote.GetControllerNodeId()
		if controllerNodeId > betterControllerNodeId {
			betterControllerNodeId = controllerNodeId
		}
	}
	return betterControllerNodeId
}

func requestSlotsData(controllerNodeId int32) {
	GetServerNetworkManagerInstance().sendMessage(controllerNodeId,
		pkgrpc.MessageEntity_REQUEST_SLOTS_DATA, utils.ToJsonByte(config.GetConfigurationInstance().NodeId))
}

// Block waiting for slot allocation data
func (c *ControllerCandidate) waitForSlotsAllocation() {
	for {
		slotsAllocation, ok := GetServerMessageReceiverInstance().slotsAllocationReceiveQueue.Take()
		if !ok {
			continue
		}
		c.slotsAllocation = slotsAllocation
		break
	}
	persist.Persist(utils.ToJsonByte(c.slotsAllocation), SlotsAllocationFilename)
}

func (c *ControllerCandidate) waitForSlotsReplicaAllocation() {
	for {
		slotsReplicaAllocation, ok := GetServerMessageReceiverInstance().slotsReplicaAllocationReceiveQueue.Take()
		if !ok {
			continue
		}
		c.slotsReplicaAllocation = slotsReplicaAllocation
		break
	}
	persist.Persist(utils.ToJsonByte(c.slotsReplicaAllocation), SlotsReplicaAllocationFilename)
}

func (c *ControllerCandidate) waitReplicaNodeIds() {
	for {
		replicaNodeIds, ok := GetServerMessageReceiverInstance().replicaNodeIdsQueue.Take()
		if !ok {
			continue
		}
		c.replicaNodeIds = replicaNodeIds
		break
	}
	persist.Persist(utils.ToJsonByte(c.replicaNodeIds), ReplicaNodeIdsFilename)
}
