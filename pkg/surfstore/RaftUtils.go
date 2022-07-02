package surfstore

import (
	"bufio"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/grpc"
)

func LoadRaftConfigFile(filename string) (ipList []string) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	serverCount := 0

	for index := 0; ; index++ {
		lineContent, _, e := configReader.ReadLine()
		if e != nil && e != io.EOF {
			log.Fatal("SURF_CLIENT: Error During Reading Config", e)
		}

		if e == io.EOF {
			return
		}

		lineString := string(lineContent)
		splitRes := strings.Split(lineString, ": ")
		if index == 0 {
			serverCount, _ = strconv.Atoi(splitRes[1])
			ipList = make([]string, serverCount, serverCount)
		} else {
			ipList[index-1] = splitRes[1]
		}
	}

	return
}

func NewRaftServer(id int64, ips []string, blockStoreAddr string) (*RaftSurfstore, error) {
	// TODO any initialization you need to do here

	rpcClients := make([]*grpc.ClientConn, len(ips))

	for id, ip := range ips {
		conn, err := grpc.Dial(ip, grpc.WithInsecure())
		for err != nil {
			conn, err = grpc.Dial(ip, grpc.WithInsecure())
		}
		rpcClients[id] = conn
	}

	isCrashedMutex := &sync.RWMutex{}

	server := RaftSurfstore{
		// TODO initialize any fields you add here
		ip:       ips[id],
		ipList:   ips,
		serverId: id,

		commitIndex:    -1,
		lastApplied:    -1,
		nextIndex:      make([]int64, len(ips)),
		matchIndex:     make([]int64, len(ips)),
		opsToReplicate: make(map[int64]chan bool),
		// opsToReplicate: make([]chan bool, 0),
		rpcConns: rpcClients,

		isLeader:       false,
		term:           0,
		metaStore:      NewMetaStore(blockStoreAddr),
		log:            make([]*UpdateOperation, 0),
		isCrashed:      false,
		notCrashedCond: sync.NewCond(isCrashedMutex),
		isCrashedMutex: isCrashedMutex,
	}

	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {
	s := grpc.NewServer()
	RegisterRaftSurfstoreServer(s, server)

	log.Println("Starting raft server on IP address", server.ip)
	l, e := net.Listen("tcp", server.ip)
	if e != nil {
		return e
	}

	go server.BackgroundThread()

	return s.Serve(l)
}
