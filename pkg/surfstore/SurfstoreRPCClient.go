package surfstore

import (
	context "context"
	"errors"
	"log"
	"strings"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	blockClient := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = blockClient.PutBlock(ctx, &Block{BlockData: block.BlockData, BlockSize: block.BlockSize})
	if err != nil {
		conn.Close()
		return err
	}

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	blockClient := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := blockClient.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = response.Hashes

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	for i := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(surfClient.MetaStoreAddrs[i], grpc.WithInsecure())
		if err != nil {
			return err
		}
		raftClient := NewRaftSurfstoreClient(conn)
		ctx := context.Background()
		log.Println("Calling GetFileInfoMap on server", i, surfClient.MetaStoreAddrs[i])
		response, err := raftClient.GetFileInfoMap(ctx, &emptypb.Empty{})
		if err != nil {
			log.Println(err)
			if strings.Contains(err.Error(), "Server is not the leader") ||
				strings.Contains(err.Error(), "Server is crashed") {
				continue
			}
			return err
		}
		*serverFileInfoMap = make(map[string]*FileMetaData)
		for fileName, fileMetaData := range response.FileInfoMap {
			(*serverFileInfoMap)[fileName] = fileMetaData
		}
		return conn.Close()
	}
	return errors.New("All servers returned ERR_NOT_LEADER")
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	for i := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(surfClient.MetaStoreAddrs[i], grpc.WithInsecure())
		if err != nil {
			return err
		}
		raftClient := NewRaftSurfstoreClient(conn)
		ctx := context.Background()
		log.Println("Calling UpdateFile for ", fileMetaData.Filename, "on server", i, surfClient.MetaStoreAddrs[i])
		response, err := raftClient.UpdateFile(ctx, fileMetaData)
		if err != nil {
			log.Println(err)
			if strings.Contains(err.Error(), "Server is not the leader") ||
				strings.Contains(err.Error(), "Server is crashed") {
				continue
			}
			return err
		}
		log.Println("setting latestVersion of file", fileMetaData.Filename, "to", response.Version)
		*latestVersion = response.Version
		if response.Version == -1 {
			return errors.New("Incompatible version")
		}
		return conn.Close()
	}
	return errors.New("All servers returned ERR_NOT_LEADER")
}

func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
	for i := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(surfClient.MetaStoreAddrs[i], grpc.WithInsecure())
		if err != nil {
			return err
		}
		raftClient := NewRaftSurfstoreClient(conn)
		ctx := context.Background()
		log.Println("Calling GetBlockStoreAddr on server", i, surfClient.MetaStoreAddrs[i])
		response, err := raftClient.GetBlockStoreAddr(ctx, &emptypb.Empty{})
		if err != nil {
			log.Println(err)
			if strings.Contains(err.Error(), "Server is not the leader") ||
				strings.Contains(err.Error(), "Server is crashed") {
				continue
			}
			return err
		}
		*blockStoreAddr = response.Addr
		return conn.Close()
	}
	return errors.New("All servers returned ERR_NOT_LEADER")
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
