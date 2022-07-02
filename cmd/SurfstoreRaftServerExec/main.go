package main

import (
	"cse224/proj5/pkg/surfstore"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

func main() {
	serverId := flag.Int64("i", -1, "(required) Server ID")
	configFile := flag.String("f", "", "(required) Config file, absolute path")
	blockStoreAddr := flag.String("b", "", "(required) BlockStore address")
	debug := flag.Bool("d", false, "Output log statements")
	flag.Parse()

	addrs := surfstore.LoadRaftConfigFile(*configFile)

	// Disable log outputs if debug flag is missing
	if !(*debug) {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}
	filepath := "/home/abhishek/Desktop/p5_logs/s" + fmt.Sprint(*serverId) + ".log"
	f, err := os.OpenFile(filepath, os.O_RDWR, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()

	// log.SetOutput(f)

	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)
	log.Fatal(startServer(*serverId, addrs, *blockStoreAddr))
}

func startServer(id int64, addrs []string, blockStoreAddr string) error {
	raftServer, err := surfstore.NewRaftServer(id, addrs, blockStoreAddr)
	if err != nil {
		log.Fatal("Error creating servers")
	}

	return surfstore.ServeRaftServer(raftServer)
}
