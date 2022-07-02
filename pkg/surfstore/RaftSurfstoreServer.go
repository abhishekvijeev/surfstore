package surfstore

import (
	context "context"
	"log"
	"sync"

	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RaftSurfstore struct {
	// TODO add any fields you need
	isLeader bool
	term     int64
	log      []*UpdateOperation

	metaStore *MetaStore

	commitIndex int64
	lastApplied int64
	nextIndex   []int64
	matchIndex  []int64
	// opsToReplicate []chan bool
	opsToReplicate map[int64](chan bool)

	// Server Info
	ip       string
	ipList   []string
	serverId int64

	// Leader protection
	isLeaderMutex sync.RWMutex
	isLeaderCond  *sync.Cond
	raftMutex     sync.Mutex

	rpcConns []*grpc.ClientConn

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	notCrashedCond *sync.Cond

	UnimplementedRaftSurfstoreServer
}

func int64Min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func (s *RaftSurfstore) IsQuorumPresent() bool {
	worldSize := len(s.ipList)
	crashedServers := 0
	for i := range s.ipList {
		// log.Println("dialling", s.ipList[i])
		c := NewRaftSurfstoreClient(s.rpcConns[i])
		ctx := context.Background()
		response, err := c.IsCrashed(ctx, &emptypb.Empty{})
		CheckErr(err)
		if response.IsCrashed {
			crashedServers++
		}
	}
	aliveServers := worldSize - crashedServers
	// log.Println("alive servers:", aliveServers, "world size", worldSize)
	return ((aliveServers * 2) > worldSize)
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	log.Println("Server", s.serverId, "GetFileInfoMap")
	s.raftMutex.Lock()
	isLeader := s.isLeader
	s.raftMutex.Unlock()

	// s.isCrashedMutex.Lock()
	// isCrashed := s.isCrashed
	// s.isCrashedMutex.Unlock()
	s.notCrashedCond.L.Lock()
	isCrashed := s.isCrashed
	s.notCrashedCond.L.Unlock()

	if !isLeader {
		log.Println("Server", s.serverId, "GetFileInfoMap returning ERR_NOT_LEADER")
		return nil, ERR_NOT_LEADER
	}

	if isCrashed {
		log.Println("Server", s.serverId, "GetFileInfoMap returning ERR_SERVER_CRASHED")
		return nil, ERR_SERVER_CRASHED
	}

	for !s.IsQuorumPresent() {
		s.raftMutex.Lock()
		isLeader := s.isLeader
		s.raftMutex.Unlock()
		if !isLeader {
			log.Println("Server", s.serverId, "GetFileInfoMap returning ERR_NOT_LEADER")
			return nil, ERR_NOT_LEADER
		}

		s.notCrashedCond.L.Lock()
		isCrashed := s.isCrashed
		s.notCrashedCond.L.Unlock()
		if isCrashed {
			log.Println("Server", s.serverId, "GetFileInfoMap returning ERR_SERVER_CRASHED")
			return nil, ERR_SERVER_CRASHED
		}
	}
	return s.metaStore.GetFileInfoMap(ctx, &emptypb.Empty{})
}

func (s *RaftSurfstore) GetBlockStoreAddr(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddr, error) {

	s.raftMutex.Lock()
	log.Println("Server", s.serverId, "GetBlockStoreAddr")
	isLeader := s.isLeader
	s.raftMutex.Unlock()
	if !isLeader {
		log.Println("Server", s.serverId, "GetBlockStoreAddr returning ERR_NOT_LEADER")
		return nil, ERR_NOT_LEADER
	}
	return s.metaStore.GetBlockStoreAddr(ctx, &emptypb.Empty{})
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	log.Println("Server", s.serverId, "UpdateFile")
	s.raftMutex.Lock()
	isLeader := s.isLeader
	s.raftMutex.Unlock()

	if !isLeader {
		log.Println("Server", s.serverId, "UpdateFile returning ERR_NOT_LEADER")
		return nil, ERR_NOT_LEADER
	}

	s.notCrashedCond.L.Lock()
	isCrashed := s.isCrashed
	s.notCrashedCond.L.Unlock()

	if isCrashed {
		log.Println("Server", s.serverId, "UpdateFile returning ERR_SERVER_CRASHED")
		return nil, ERR_SERVER_CRASHED
	}

	// We have established that a quorum is present and that this
	// node is the leader
	replicated := make(chan bool)

	s.raftMutex.Lock()
	op := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}
	s.log = append(s.log, &op)
	s.opsToReplicate[int64(len(s.log)-1)] = replicated
	log.Println("Server", s.serverId, "UpdateFile appended", op, "to log")
	s.raftMutex.Unlock()

	for !s.IsQuorumPresent() {
		s.raftMutex.Lock()
		isLeader := s.isLeader
		s.raftMutex.Unlock()
		log.Println("Server", s.serverId, "waiting for quorum")
		if !isLeader {
			log.Println("Server", s.serverId, "UpdateFile returning ERR_NOT_LEADER")
			return nil, ERR_NOT_LEADER
		}
		s.notCrashedCond.L.Lock()
		isCrashed := s.isCrashed
		s.notCrashedCond.L.Unlock()
		if isCrashed {
			log.Println("Server", s.serverId, "UpdateFile returning ERR_SERVER_CRASHED")
			return nil, ERR_SERVER_CRASHED
		}
	}

	success := <-replicated
	if success {
		log.Println("Server", s.serverId, "UpdateFile applying", op, "to state machine")
		return s.metaStore.UpdateFile(ctx, filemeta)
	}

	return nil, nil
}

func (s *RaftSurfstore) BackgroundThread() {
	for {
		s.notCrashedCond.L.Lock()
		for s.isCrashed {
			log.Println("Server", s.serverId, "Background thread waiting on condvar")
			s.notCrashedCond.Wait()
			log.Println("Server", s.serverId, "Background thread condvar signalled")
		}

		s.notCrashedCond.L.Unlock()

		s.raftMutex.Lock()
		isLeader := s.isLeader
		commitIndex := s.commitIndex
		logLen := int64(len(s.log))
		s.raftMutex.Unlock()

		if !isLeader ||
			commitIndex == logLen {
			// log.Println("Spinning")
			// time.Sleep(100 * time.Millisecond)
			continue
		}

		for followerID := range s.ipList {
			if s.ip != s.ipList[followerID] {
				// log.Println("Replicating on follower", followerID)
				go s.ReplicateLogsOnFollower(followerID)
			}
		}
		// time.Sleep(100 * time.Millisecond)
	}

}

func (s *RaftSurfstore) ReplicateLogsOnFollower(followerID int) {

	s.raftMutex.Lock()
	nextIndex := s.nextIndex[followerID]
	prevLogIndex := nextIndex - 1
	prevLogTerm := int64(-1)
	if prevLogIndex >= 0 {
		prevLogTerm = s.log[prevLogIndex].Term
	}
	entries := s.log[nextIndex:]

	input := &AppendEntryInput{
		Term:         s.term,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: s.commitIndex,
	}
	s.raftMutex.Unlock()

	client := NewRaftSurfstoreClient(s.rpcConns[followerID])
	ctx := context.Background()
	log.Println("Server", s.serverId, "calling AE on server", followerID, "with term", s.term)
	output, err := client.AppendEntries(ctx, input)
	if err != nil {
		return
	}

	s.raftMutex.Lock()
	defer s.raftMutex.Unlock()

	if output.Term > s.term {
		log.Println("Server", s.serverId, "becoming follower,",
			"my term:", s.term, "output.term:", output.Term)
		s.BecomeFollower(output.Term)
	}
	if s.isLeader && s.term == output.Term {
		if output.Success {
			// log.Println("commit index:", s.commitIndex)

			s.nextIndex[followerID] = nextIndex + int64(len(entries))
			s.matchIndex[followerID] = s.nextIndex[followerID] - 1
			// log.Println("AppendEntries from follower", followerID, "successful",
			// 	"nextIndex =", s.nextIndex[followerID],
			// 	"matchIndex =", s.matchIndex[followerID],
			// 	"my term =", s.term)

			for i := s.commitIndex + 1; i < int64(len(s.log)); i++ {
				if s.log[i].Term == s.term {
					replicatedCount := 1
					for followerID := range s.ipList {
						if s.matchIndex[followerID] >= i {
							replicatedCount++
						}
					}
					if (replicatedCount * 2) > len(s.ipList) {

						s.commitIndex = i
						// log.Println("Setting commit index to", s.commitIndex)
						log.Println("commit index:", s.commitIndex)
						log.Println("i:", i)
						log.Println("Log:", s.log)
						log.Println("len(log):", len(s.log))
						// log.Println("s.opsToReplicate:", s.opsToReplicate)
						log.Println("Setting opsToReplicate[", s.log[i], "] to true")
						s.opsToReplicate[i] <- true
					}
				}
			}
		} else {
			s.nextIndex[followerID] = nextIndex - 1
			log.Println("AppendEntries from follower", followerID, "failed",
				"setting nextIndex to", s.nextIndex[followerID])
		}
	}
}

//1. Reply false if term < currentTerm (§5.1)
//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
//matches prevLogTerm (§5.3)
//3. If an existing entry conflicts with a new one (same index but different
//terms), delete the existing entry and all that follow it (§5.3)
//4. Append any new entries not already in the log
//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
//of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {

	s.notCrashedCond.L.Lock()
	isCrashed := s.isCrashed
	s.notCrashedCond.L.Unlock()
	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.raftMutex.Lock()
	defer s.raftMutex.Unlock()

	output := new(AppendEntryOutput)
	output.Success = false
	output.Term = s.term
	output.ServerId = s.serverId

	// log.Println("Server", s.serverId, " with term ", s.term, ", commitIndex",
	// 	s.commitIndex, "received AppendEntries", input, input.Entries)

	if input.Term < s.term {
		log.Println("Server", s.serverId, "returning failed AE bcos",
			"input.term =", input.Term, "my term =", s.term)
		return output, nil
	}
	if input.Term > s.term {
		log.Println("Server", s.serverId, "becoming follower,",
			"my term:", s.term, "input.Term:", input.Term)
		s.BecomeFollower(input.Term)
	}

	// If the previous log index's term matches, then we proceed
	if input.PrevLogIndex == -1 ||
		(input.PrevLogIndex < int64(len(s.log)) &&
			input.PrevLogTerm == s.log[input.PrevLogIndex].Term) {
		output.Success = true

		logInsertIndex := input.PrevLogIndex + 1
		newEntriesIndex := 0

		// Find the index at which there is a potential mismatch in terms
		for {
			if logInsertIndex >= int64(len(s.log)) ||
				newEntriesIndex >= len(input.Entries) {
				break
			}
			if s.log[logInsertIndex].Term != input.Entries[newEntriesIndex].Term {
				break
			}
			logInsertIndex++
			newEntriesIndex++
		}

		if newEntriesIndex < len(input.Entries) {
			log.Println("Server", s.serverId, "appending entries", input.Entries[newEntriesIndex:])
			s.log = append(s.log[:logInsertIndex], input.Entries[newEntriesIndex:]...)
		}

		// Set commit index
		if input.LeaderCommit > s.commitIndex {
			log.Println("Leader commit:", input.LeaderCommit, "my commit:", s.commitIndex)
			s.commitIndex = int64Min(input.LeaderCommit, int64(len(s.log)-1))
			// log.Println("Updated my commit to:", s.commitIndex)
		}
		for s.lastApplied < s.commitIndex {
			s.lastApplied++
			entry := s.log[s.lastApplied]
			log.Println("Server", s.serverId, "applying operation", entry, "to state machine")
			s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		}
		output.MatchedIndex = s.lastApplied
	}
	if output.Success == false {
		log.Println("Server", s.serverId, "returning failed AE bcos prevLogIndex doesn't match")
		log.Println("input.PrevLogIndex < int64(len(s.log)) = ", input.PrevLogIndex < int64(len(s.log)))
		log.Println("input.PrevLogTerm == s.log[input.PrevLogIndex].Term = ",
			input.PrevLogTerm == s.log[input.PrevLogIndex].Term)
	}
	return output, nil
}

// This should set the leader status and any related variables as if the node has just won an election
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {

	s.notCrashedCond.L.Lock()
	isCrashed := s.isCrashed
	s.notCrashedCond.L.Unlock()

	if isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	s.raftMutex.Lock()
	s.isLeader = true
	s.term += 1
	log.Println("Server", s.serverId, "setLeader succeded, new term =", s.term)

	for follower := range s.ipList {
		s.nextIndex[follower] = int64(len(s.log))
		s.matchIndex[follower] = -1
	}
	s.raftMutex.Unlock()

	s.SendHeartbeat(ctx, &emptypb.Empty{})
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) BecomeFollower(term int64) {
	// debug.PrintStack()
	s.isLeader = false
	s.term = term
}

// Send a 'Heartbeat" (AppendEntries with no log entries) to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	log.Println("SendHeartbeat")
	s.raftMutex.Lock()
	isLeader := s.isLeader
	s.raftMutex.Unlock()

	if !isLeader {
		log.Println("Not leader")
		return &Success{Flag: false}, ERR_NOT_LEADER
	}

	log.Println("Server", s.serverId, "SendHeartbeat")
	s.notCrashedCond.L.Lock()
	isCrashed := s.isCrashed
	s.notCrashedCond.L.Unlock()

	if isCrashed {
		log.Println("Server", s.serverId, "SendHeartbeat returning ERR_SERVER_CRASHED")
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}
	for followerID := range s.ipList {
		if s.ip != s.ipList[followerID] {
			go s.ReplicateLogsOnFollower(followerID)
		}
	}
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {

	s.notCrashedCond.L.Lock()
	log.Println("Server", s.serverId, "Crash")
	s.isCrashed = true
	s.notCrashedCond.L.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.notCrashedCond.L.Lock()
	log.Println("Server", s.serverId, "Restore")
	s.isCrashed = false
	s.notCrashedCond.Broadcast()
	s.notCrashedCond.L.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) IsCrashed(ctx context.Context, _ *emptypb.Empty) (*CrashedState, error) {
	// log.Println("got request for IsCrashed on server", s.serverId)
	s.notCrashedCond.L.Lock()
	isCrashed := s.isCrashed
	s.notCrashedCond.L.Unlock()
	return &CrashedState{IsCrashed: isCrashed}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	return &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
