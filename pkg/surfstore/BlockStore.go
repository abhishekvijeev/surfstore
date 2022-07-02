package surfstore

import (
	context "context"
	"errors"
	"log"
	sync "sync"
)

type BlockStore struct {
	BlockMap map[string]*Block
	mutex    sync.Mutex
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	log.Println("got request for GetBlock")
	bs.mutex.Lock()
	block, ok := bs.BlockMap[blockHash.Hash]
	bs.mutex.Unlock()
	if !ok {
		blockData := make([]byte, 0)
		blockSize := int32(0)
		return &Block{BlockData: blockData, BlockSize: blockSize}, errors.New("Block hash does not exist in block store")
	}
	blockData := block.BlockData
	blockSize := block.BlockSize
	return &Block{BlockData: blockData, BlockSize: blockSize}, nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	log.Println("got request for PutBlock")
	blockHash := GetBlockHashString(block.BlockData)
	bs.mutex.Lock()
	_, blockExists := bs.BlockMap[blockHash]
	if blockExists {
		bs.mutex.Unlock()
		return &Success{Flag: false}, errors.New("Block with hash" + blockHash + "already exists")
	}
	bs.BlockMap[blockHash] = block
	bs.mutex.Unlock()
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	blockHashesOut := make([]string, 0)
	bs.mutex.Lock()
	for _, hash := range blockHashesIn.Hashes {
		if _, present := bs.BlockMap[hash]; !present {
			blockHashesOut = append(blockHashesOut, hash)
		}
	}
	bs.mutex.Unlock()
	return &BlockHashes{Hashes: blockHashesOut}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
