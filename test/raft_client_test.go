package SurfTest

import (
	context "context"
	"cse224/proj5/pkg/surfstore"
	"log"
	"os"
	"testing"
	"time"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
	//	"time"
)

// A creates and syncs with a file. B creates and syncs with same file. A syncs again.
func TestSyncTwoClientsSameFileLeaderFailure(t *testing.T) {
	t.Logf("client1 syncs with file1. client2 syncs with file1 (different content). client1 syncs again.")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})

	worker1 := InitDirectoryWorker("test0", SRC_PATH)
	worker2 := InitDirectoryWorker("test1", SRC_PATH)
	defer worker1.CleanUp()
	defer worker2.CleanUp()

	//clients add different files
	file1 := "multi_file1.txt"
	file2 := "multi_file1.txt"
	err := worker1.AddFile(file1)
	if err != nil {
		t.FailNow()
	}
	err = worker2.AddFile(file2)
	if err != nil {
		t.FailNow()
	}
	err = worker2.UpdateFile(file2, "update text")
	if err != nil {
		t.FailNow()
	}

	//client1 syncs
	t.Logf("Client 1 syncs")
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	t.Logf("Crash server 0")
	test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	t.Logf("Set server 1 to leader")
	test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
	t.Logf("Server 1 sendHeartBeat")
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	//client2 syncs
	t.Logf("Client 2 syncs")
	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	//client1 syncs
	t.Logf("Client 1 syncs")
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	workingDir, _ := os.Getwd()

	//check client1
	_, err = os.Stat(workingDir + "/test0/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client1")
	}

	fileMeta1, err := LoadMetaFromMetaFile(workingDir + "/test0/")
	if err != nil {
		t.Fatalf("Could not load meta file for client1")
	}
	if len(fileMeta1) != 1 {
		t.Fatalf("Wrong number of entries in client1 meta file")
	}
	if fileMeta1[file1].Version != 1 {
		t.Fatalf("Wrong version for file1 in client1 metadata.")
	}

	c, e := SameFile(workingDir+"/test0/multi_file1.txt", SRC_PATH+"/multi_file1.txt")
	if e != nil {
		t.Fatalf("Could not read files in client base dirs.")
	}
	if !c {
		t.Fatalf("file1 should not change at client1")
	}

	//check client2
	_, err = os.Stat(workingDir + "/test1/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client2")
	}

	fileMeta2, err := LoadMetaFromMetaFile(workingDir + "/test1/")
	if err != nil {
		t.Fatalf("Could not load meta file for client2")
	}
	if len(fileMeta2) != 1 {
		t.Fatalf("Wrong number of entries in client2 meta file")
	}
	if fileMeta1[file1].Version != 1 {
		t.Fatalf("Wrong version for file1 in client2 metadata.")
	}

	c, e = SameFile(workingDir+"/test1/multi_file1.txt", SRC_PATH+"/multi_file1.txt")
	if e != nil {
		t.Fatalf("Could not read files in client base dirs.")
	}
	if !c {
		t.Fatalf("wrong file2 contents at client2")
	}
}

func TestGetFileInfoMapQuorum(t *testing.T) {
	t.Logf("Test behaviour of GetFileInfoMap() when a quorum is not available")

	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})

	worker1 := InitDirectoryWorker("test0", SRC_PATH)
	worker2 := InitDirectoryWorker("test1", SRC_PATH)
	defer worker1.CleanUp()
	defer worker2.CleanUp()

	//clients add different files
	file1 := "multi_file1.txt"
	file2 := "multi_file1.txt"
	err := worker1.AddFile(file1)
	if err != nil {
		t.FailNow()
	}
	err = worker2.AddFile(file2)
	if err != nil {
		t.FailNow()
	}
	t.Logf("\n\nUPDATING FILE 2")
	err = worker2.UpdateFile(file2, "update text")
	if err != nil {
		t.FailNow()
	}

	test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	test.Clients[1].Crash(test.Context, &emptypb.Empty{})

	go func() {
		time.Sleep(5 * time.Second)
		t.Logf("\n\nRESTORING CLIENT 1")
		test.Clients[1].Restore(test.Context, &emptypb.Empty{})
		test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
		test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})
		// t.Logf("\n\nRESTORING CLIENT 0")
		// test.Clients[0].Restore(test.Context, &emptypb.Empty{})
	}()

	//client1 syncs
	t.Logf("\n\nClient 1 syncs")
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}
}

func TestSyncTwoClientsFileUpdateLeaderFailure(t *testing.T) {
	t.Logf("client1 syncs with file1. client2 syncs. leader change.")
	t.Logf("client2 syncs with file1 (different content). client1 syncs again.")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})

	worker1 := InitDirectoryWorker("test0", SRC_PATH)
	worker2 := InitDirectoryWorker("test1", SRC_PATH)
	defer worker1.CleanUp()
	defer worker2.CleanUp()

	//clients add different files
	file1 := "multi_file1.txt"
	// file2 := "multi_file1.txt"
	err := worker1.AddFile(file1)
	if err != nil {
		t.FailNow()
	}

	err = worker1.UpdateFile(file1, "file1")
	if err != nil {
		t.FailNow()
	}

	//client1 syncs
	t.Logf("Client 1 syncs")
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	//client2 syncs
	t.Logf("Client 2 syncs")
	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	// Leader change
	test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
	t.Logf("Server 1 sendHeartBeat")
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	// client2 syncs with file1 (different content)
	err = worker2.UpdateFile(file1, "update text")
	if err != nil {
		t.FailNow()
	}
	t.Logf("Client 2 syncs")
	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	//client1 syncs again
	t.Logf("Client 1 syncs")
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}
}

func TestSyncTwoClientsClusterFailure(t *testing.T) {
	t.Logf("client1 syncs with file1. client2 syncs. majority of the cluster crashes. client2 syncs with file1 (different content).")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	worker1 := InitDirectoryWorker("test0", SRC_PATH)
	worker2 := InitDirectoryWorker("test1", SRC_PATH)
	defer worker1.CleanUp()
	defer worker2.CleanUp()

	file1 := "multi_file1.txt"
	// file2 := "multi_file1.txt"

	err := worker1.AddFile(file1)
	if err != nil {
		t.FailNow()
	}

	//client1 syncs with file1
	t.Logf("Client 1 syncs")
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	//client2 syncs
	t.Logf("Client 2 syncs")
	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	// majority of cluster crashes
	log.Println("\n\nCrashing majority")
	test.Clients[1].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})

	//client2 syncs with file1 (different content)
	err = worker2.UpdateFile(file1, "update text")
	if err != nil {
		t.FailNow()
	}
	t.Logf("Client 2 syncs")
	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}
}

func TestRaftLogsCorrectlyOverwritten(t *testing.T) {
	t.Logf("leader1 gets several requests while all other nodes are crashed.")
	t.Logf("leader1 crashes. all other nodes are restored. leader2 gets a request.")
	t.Logf("leader1 is restored")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)
	test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})

	worker1 := InitDirectoryWorker("test0", SRC_PATH)
	worker2 := InitDirectoryWorker("test1", SRC_PATH)
	worker3 := InitDirectoryWorker("test2", SRC_PATH)
	defer worker1.CleanUp()
	defer worker2.CleanUp()
	defer worker3.CleanUp()

	file1 := "multi_file1.txt"
	file2 := "multi_file2.txt"
	file3 := "multi_file3.txt"
	err := worker1.AddFile(file1)
	if err != nil {
		t.FailNow()
	}
	err = worker2.AddFile(file2)
	if err != nil {
		t.FailNow()
	}
	err = worker3.AddFile(file3)
	if err != nil {
		t.FailNow()
	}

	chan1 := make(chan bool)
	chan2 := make(chan bool)
	//client1 syncs
	go func(t *testing.T, channel chan bool) {
		t.Logf("Client 1 syncs")
		err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
		if err != nil {
			t.Fatalf("Sync failed")
		}
		channel <- true
	}(t, chan1)

	//client2 syncs
	go func(t *testing.T, channel chan bool) {
		t.Logf("Client 2 syncs")
		err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
		if err != nil {
			t.Fatalf("Sync failed")
		}
		channel <- true
	}(t, chan2)

	time.Sleep(5 * time.Second)
	// Leader 1 crashes
	t.Logf("Crash leader 1")
	test.Clients[1].Crash(test.Context, &emptypb.Empty{})

	// All other nodes are restored
	t.Logf("Restoring node 0 and 2")

	test.Clients[2].Restore(test.Context, &emptypb.Empty{})
	test.Clients[2].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[2].SendHeartbeat(test.Context, &emptypb.Empty{})
	test.Clients[0].Restore(test.Context, &emptypb.Empty{})

	// Leader 2 gets a request
	err = SyncClient("localhost:8080", "test2", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	// Leader 1 is restored
	test.Clients[1].Restore(test.Context, &emptypb.Empty{})
	test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	<-chan1
	<-chan2

	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})
}

func TestRaftLogsCorrectlyOverwrite2(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)

	// set the leader
	leaderIdx := 1
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// crash the servers
	test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})

	// leader1 gets several requests while all other nodes are crashed.
	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}
	go test.Clients[leaderIdx].UpdateFile(test.Context, filemeta1)

	filemeta2 := &surfstore.FileMetaData{
		Filename:      "testFile2",
		Version:       1,
		BlockHashList: nil,
	}
	go test.Clients[leaderIdx].UpdateFile(context.Background(), filemeta2)

	// leader1 crashes.
	test.Clients[1].Crash(test.Context, &emptypb.Empty{})

	// all other nodes are restored.
	test.Clients[0].Restore(test.Context, &emptypb.Empty{})
	test.Clients[2].Restore(test.Context, &emptypb.Empty{})

	// set the leader
	leaderIdx = 2
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// leader2 gets a request
	filemeta3 := &surfstore.FileMetaData{
		Filename:      "testFile3",
		Version:       1,
		BlockHashList: nil,
	}
	go test.Clients[leaderIdx].UpdateFile(test.Context, filemeta3)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// leader1 is restored

	leaderIdx = 1
	test.Clients[leaderIdx].Restore(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// end of the test
	_, err := test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})
	surfstore.CheckErr(err)

	// The log should be replicated and applied
	goldenMeta := surfstore.NewMetaStore("")
	goldenMeta.UpdateFile(test.Context, filemeta3)
	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         2,
		FileMetaData: filemeta3,
	})

	for i, server := range test.Clients {
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		t.Log(state.Log)
		if !SameLog(goldenLog, state.Log) {
			t.Log("Logs do not match for server", i)
			t.Log("Expected", goldenLog)
			t.Log("Received", state.Log)
			t.Log("\n")
			t.Fail()
		}

		t.Log(state.MetaMap.FileInfoMap)
		if !SameMeta(goldenMeta.FileMetaMap, state.MetaMap.FileInfoMap) {
			t.Log("MetaStore state is not correct for server", i)
			t.Log("Expected", goldenMeta.FileMetaMap)
			t.Log("Received", state.MetaMap.FileInfoMap)
			t.Log("\n")
			t.Fail()
		}
	}
}

func TestSyncLargeTwoClient(t *testing.T) {
	t.Logf("client1 syncs with a large file. client2 syncs")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	worker1 := InitDirectoryWorker("test0", SRC_PATH)
	InitDirectoryWorker("test1", SRC_PATH)
	// worker2 := InitDirectoryWorker("test1", SRC_PATH)
	// defer worker1.CleanUp()
	// defer worker2.CleanUp()

	// file1 := "sample.txt"
	file1 := "input-7.dat"
	err := worker1.AddFile(file1)
	if err != nil {
		t.FailNow()
	}

	//client1 syncs
	t.Logf("Client 1 syncs")
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	//client2 syncs
	t.Logf("Client 2 syncs")
	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}
}

func TestRaftLogsConsistent(t *testing.T) {
	/*
		Create 3 nodes
		0,1,2

		0 leader
		sendheartbeat
		crash 2
		sync with 0, send heartbeat
		crash 0
		make 1 the leader
		restore 2
		make another request to leader 1
		restore 0
		sendheartbeat
	*/
	t.Logf("leader1 gets a request while a minority of the cluster is down.")
	t.Logf("leader1 crashes. the other crashed nodes are restored.")
	t.Logf("leader2 gets a request. leader1 is restored")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)

	// Set node 1 to leader
	test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	// Crash minority of cluster
	test.Clients[0].Crash(test.Context, &emptypb.Empty{})

	worker1 := InitDirectoryWorker("test0", SRC_PATH)
	worker2 := InitDirectoryWorker("test1", SRC_PATH)
	defer worker1.CleanUp()
	defer worker2.CleanUp()

	file1 := "multi_file1.txt"
	file2 := "multi_file2.txt"
	err := worker1.AddFile(file1)
	if err != nil {
		t.FailNow()
	}
	err = worker2.AddFile(file2)
	if err != nil {
		t.FailNow()
	}

	// Leader1 gets a request
	t.Logf("Client 1 syncs")
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	// Leader1 crashes
	test.Clients[1].Crash(test.Context, &emptypb.Empty{})

	// Other crashed nodes are restored
	test.Clients[0].Restore(test.Context, &emptypb.Empty{})

	// Set node 2 to leader
	test.Clients[2].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[2].SendHeartbeat(test.Context, &emptypb.Empty{})

	// Leader2 gets a request
	t.Logf("Client 2 syncs")
	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	// Leader1 is restored
	test.Clients[1].Restore(test.Context, &emptypb.Empty{})
	test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})
}
