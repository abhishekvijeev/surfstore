package surfstore

import (
	context "context"
	"log"
	sync "sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	mutex          sync.Mutex
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	log.Println("got request for GetFileInfoMap")
	m.mutex.Lock()
	fileMetaMap := m.FileMetaMap
	m.mutex.Unlock()
	log.Println("returning", fileMetaMap)
	return &FileInfoMap{FileInfoMap: fileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	log.Println("got request for UpdateFile")
	fileName := fileMetaData.Filename
	version := fileMetaData.Version
	blockHashList := fileMetaData.BlockHashList

	m.mutex.Lock()
	_, fileExists := m.FileMetaMap[fileName]
	if !fileExists {
		m.FileMetaMap[fileName] = new(FileMetaData)
	} else if version != (m.FileMetaMap[fileName].Version + 1) {
		version = m.FileMetaMap[fileName].Version
		m.mutex.Unlock()
		log.Println("I have version", m.FileMetaMap[fileName].Version, "client wants to update to", fileMetaData.Version)
		return &Version{Version: -1}, nil
	}

	m.FileMetaMap[fileName].Filename = fileName
	m.FileMetaMap[fileName].Version = version
	m.FileMetaMap[fileName].BlockHashList = blockHashList
	PrintMetaMap(m.FileMetaMap)
	m.mutex.Unlock()
	return &Version{Version: version}, nil
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	log.Println("got request for GetBlockStoreAddr")
	m.mutex.Lock()
	blockStoreAddr := m.BlockStoreAddr
	m.mutex.Unlock()
	return &BlockStoreAddr{Addr: blockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
