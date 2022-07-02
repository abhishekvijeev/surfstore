package surfstore

import (
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
)

type FileData struct {
	hashList     []string
	hashMappings map[string]Block
}

func CheckErr(e error) {
	if e != nil {
		log.Panicln(e)
	}
}

func ReadFileBlock(file *os.File, blockSize int) []byte {
	block := make([]byte, blockSize)
	_, err := io.ReadFull(file, block)
	CheckErr(err)
	return block
}

func ComputeFileData(file *os.File, fileSize int64, blockSize int) FileData {
	numFullBlocks := int(fileSize / int64(blockSize))
	lastBlockSize := int(fileSize % int64(blockSize))
	var fd FileData
	fd.hashList = make([]string, 0)
	fd.hashMappings = make(map[string]Block)
	// log.Println("file size:", fileSize)
	// log.Println("full blocks:", numFullBlocks)
	// log.Println("last block size:", lastBlockSize)
	for i := 0; i < numFullBlocks; i++ {
		block := ReadFileBlock(file, blockSize)
		hash := GetBlockHashString(block)
		// blockHashList = append(blockHashList, FileBlocks{blockHash: hash, blockData: block})
		fd.hashList = append(fd.hashList, hash)
		fd.hashMappings[hash] = Block{BlockData: block, BlockSize: int32(blockSize)}
	}
	if lastBlockSize > 0 {
		block := ReadFileBlock(file, lastBlockSize)
		hash := GetBlockHashString(block)
		// blockHashList = append(blockHashList, FileBlocks{blockHash: hash, blockData: block})
		fd.hashList = append(fd.hashList, hash)
		fd.hashMappings[hash] = Block{BlockData: block, BlockSize: int32(lastBlockSize)}
	}
	return fd
}

func ComputeBaseDirFileBlocks(baseDir string, blockSize int) map[string]FileData {
	// TODO:
	// Check whether the basedir exists or not and confirm what to do if it doesn't exist
	// baseDirFileBlocks := make(map[string][]string)
	baseDirFileBlocks := make(map[string]FileData)
	files, err := ioutil.ReadDir(baseDir)
	CheckErr(err)
	for _, fileInfo := range files {
		if !fileInfo.IsDir() {
			fileName := fileInfo.Name()
			filePath := path.Join(baseDir, fileName)
			fileSize := fileInfo.Size()
			// log.Println("file path:", filePath)
			file, err := os.Open(filePath)
			CheckErr(err)
			defer file.Close()
			fileData := ComputeFileData(file, fileSize, blockSize)
			baseDirFileBlocks[fileName] = fileData
		}
	}
	return baseDirFileBlocks
}

func UploadFileBlocks(client RPCClient, fileData FileData, blockStoreAddr string) {
	var blocksToPut []string
	fileBlockHashes := fileData.hashList
	CheckErr(client.HasBlocks(fileBlockHashes, blockStoreAddr, &blocksToPut))

	for _, blockHash := range blocksToPut {
		var succ bool
		blockData := fileData.hashMappings[blockHash].BlockData
		blockSize := fileData.hashMappings[blockHash].BlockSize
		client.PutBlock(&Block{BlockData: blockData, BlockSize: blockSize},
			blockStoreAddr, &succ)
		// if err != nil {
		// 	return err
		// }
	}
}

func DownloadFileBlocks(client RPCClient, blockHashList []string, blockStoreAddr string) []Block {
	fileBlocks := make([]Block, 0)
	for _, blockHash := range blockHashList {
		var block Block
		CheckErr(client.GetBlock(blockHash, blockStoreAddr, &block))
		fileBlocks = append(fileBlocks, Block{BlockData: block.BlockData, BlockSize: block.BlockSize})
	}
	return fileBlocks
}

func HashListsEqual(list1 []string, list2 []string) bool {
	if len(list1) != len(list2) {
		return false
	}
	for i, v := range list1 {
		if v != list2[i] {
			return false
		}
	}
	return true
}

// Line 209
// Figure out a test that would trigger the "UPDATE FAILED" control flow path

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// 1) scan the base directory, and for each file, compute that file’s hash list.
	baseDirFileBlocks := ComputeBaseDirFileBlocks(client.BaseDir, client.BlockSize)
	// for fileName := range baseDirFileBlocks {
	// 	log.Println(fileName)
	// 	for i := range baseDirFileBlocks[fileName] {
	// 		log.Println(baseDirFileBlocks[fileName][i])
	// 	}
	// }

	// 2) read local index file
	localIndex, err := LoadMetaFromMetaFile(client.BaseDir)
	CheckErr(err)
	// log.Println("localindex:", localIndex)

	// 3) connect to the server and download an updated FileInfoMap, called 'remote index'
	var remoteIndex map[string]*FileMetaData
	CheckErr(client.GetFileInfoMap(&remoteIndex))
	// log.Println("remote index:", remoteIndex)

	var blockStoreAddr string
	CheckErr(client.GetBlockStoreAddr(&blockStoreAddr))
	// log.Println("block store address:", blockStoreAddr)

	// Look through all files in the remote index
	log.Println("Iterating through remote index")
	for fileName, fileMetaData := range remoteIndex {
		// log.Println("file: ", fileName)
		filePath, err := filepath.Abs(ConcatPath(client.BaseDir, fileName))
		CheckErr(err)
		_, localIndexContainsFile := localIndex[fileName]
		_, baseDirContainsFile := baseDirFileBlocks[fileName]

		if !localIndexContainsFile && !baseDirContainsFile {
			/*
				N N Y case
				The remote index refers to a new file not present in the local
				index or in the base directory. Therefore:
					1) download the blocks associated with the file
					2) reconstitute the file in the base directory
					3) add the updated FileInfo information to the local index
			*/

			deleted := (len(fileMetaData.BlockHashList) == 1) && (fileMetaData.BlockHashList[0] == "0")
			if !deleted {
				file, err := os.Create(filePath)
				CheckErr(err)
				defer file.Close()
				log.Println("Downloading file blocks for", fileName)
				fileBlocks := DownloadFileBlocks(client, fileMetaData.BlockHashList, blockStoreAddr)
				for blockNum := range fileBlocks {
					blockSize := fileBlocks[blockNum].BlockSize
					blockData := fileBlocks[blockNum].BlockData
					bytesWritten, err := file.Write(blockData)
					CheckErr(err)
					if int32(bytesWritten) != blockSize {
						log.Fatal("Could not write to file")
					}
				}
				localIndex[fileName] = new(FileMetaData)
				localIndex[fileName].Filename = fileMetaData.Filename
				localIndex[fileName].Version = fileMetaData.Version
				localIndex[fileName].BlockHashList = fileMetaData.BlockHashList
			}
		} else if localIndexContainsFile && !baseDirContainsFile {
			/*
				N Y Y case
				File is present only in the remote and local indexes. If the remote
				and local indexes have the same version, this means that our client
				has deleted this file from its baseDir and we hence have to upload
				a tombstone version. However, if the versions don't match, this
				means that some other client uploaded a new version of the file to
				the remote server and we now have to download the new version of
				this file to bring this client's file up to date
			*/
			log.Println("file", fileName, "local version:", localIndex[fileName].Version,
				"remote version:", remoteIndex[fileName].Version)
			if localIndex[fileName].Version == remoteIndex[fileName].Version {
				// Upload tombstone to server
				newVersion := localIndex[fileName].Version + 1
				tombstoneHashList := make([]string, 1)
				tombstoneHashList[0] = "0"

				var latestVersion int32
				err = client.UpdateFile(&FileMetaData{
					Filename:      fileName,
					Version:       newVersion,
					BlockHashList: tombstoneHashList},
					&latestVersion)
				if err != nil {
					// Some other client synced this file before us - download latest version from server
					log.Println("UPDATE FILE ERROR: expected version = ", newVersion,
						"remote version = ", latestVersion)
					var newRemoteIndex map[string]*FileMetaData
					CheckErr(client.GetFileInfoMap(&newRemoteIndex))
					newHashList := newRemoteIndex[fileName].BlockHashList

					deleted := (len(newHashList) == 1) && (newHashList[0] == "0")
					if !deleted {

						log.Println("Downloading file blocks for", fileName)
						fileBlocks := DownloadFileBlocks(client, newHashList, blockStoreAddr)
						file, err := os.Create(filePath)
						CheckErr(err)
						defer file.Close()
						for blockNum := range fileBlocks {
							blockSize := fileBlocks[blockNum].BlockSize
							blockData := fileBlocks[blockNum].BlockData
							bytesWritten, err := file.Write(blockData)
							CheckErr(err)
							if int32(bytesWritten) != blockSize {
								log.Fatal("Could not write to file")
							}
						}
					}
					localIndex[fileName].Filename = fileName
					localIndex[fileName].Version = newRemoteIndex[fileName].Version
					localIndex[fileName].BlockHashList = newHashList
				} else {
					localIndex[fileName].Filename = fileMetaData.Filename
					localIndex[fileName].Version = newVersion
					localIndex[fileName].BlockHashList = tombstoneHashList
				}
				log.Println("got version", latestVersion)
			} else {
				// Download new data blocks from server if it is not a tombstone
				deleted := (len(fileMetaData.BlockHashList) == 1) && (fileMetaData.BlockHashList[0] == "0")
				if !deleted {
					file, err := os.Create(filePath)
					CheckErr(err)
					defer file.Close()
					log.Println("Downloading file blocks for", fileName)
					fileBlocks := DownloadFileBlocks(client, fileMetaData.BlockHashList,
						blockStoreAddr)

					for blockNum := range fileBlocks {
						blockSize := fileBlocks[blockNum].BlockSize
						blockData := fileBlocks[blockNum].BlockData
						bytesWritten, err := file.Write(blockData)
						CheckErr(err)
						if int32(bytesWritten) != blockSize {
							log.Fatal("Could not write to file")
						}
					}
				}
				localIndex[fileName].Filename = fileMetaData.Filename
				localIndex[fileName].Version = fileMetaData.Version
				localIndex[fileName].BlockHashList = fileMetaData.BlockHashList

			}
		} else if baseDirContainsFile && !localIndexContainsFile {
			/*
				Y N Y case
				File is present only in the remote index and baseDir. It means
				that this client has created a new file since its last sync with
				the server, but some other client has already synced a file with
				the same name to the remote server. We hence have to download the
				new version of this file to bring this client's file up to date
			*/
			deleted := (len(fileMetaData.BlockHashList) == 1) && (fileMetaData.BlockHashList[0] == "0")
			if !deleted {
				log.Println("Downloading file blocks for", fileName)
				fileBlocks := DownloadFileBlocks(client, fileMetaData.BlockHashList,
					blockStoreAddr)
				file, err := os.Create(filePath)
				CheckErr(err)
				defer file.Close()
				for blockNum := range fileBlocks {
					blockSize := fileBlocks[blockNum].BlockSize
					blockData := fileBlocks[blockNum].BlockData
					bytesWritten, err := file.Write(blockData)
					CheckErr(err)
					if int32(bytesWritten) != blockSize {
						log.Fatal("Could not write to file")
					}
				}
			} else {
				log.Println("Deleting file", fileName)
				CheckErr(os.Remove(filePath))
			}
			localIndex[fileName] = new(FileMetaData)
			localIndex[fileName].Filename = fileMetaData.Filename
			localIndex[fileName].Version = fileMetaData.Version
			localIndex[fileName].BlockHashList = fileMetaData.BlockHashList
		} else {
			/*
				Y Y Y case
				The file is present in baseDir, local and remote
			*/
			log.Println("file", fileName, "local version:", localIndex[fileName].Version,
				"remote version:", remoteIndex[fileName].Version)
			if localIndex[fileName].Version == remoteIndex[fileName].Version {
				if !HashListsEqual(remoteIndex[fileName].BlockHashList, baseDirFileBlocks[fileName].hashList) {
					// Upload
					newVersion := localIndex[fileName].Version + 1
					fileData := baseDirFileBlocks[fileName]
					log.Println("uploading data blocks")
					UploadFileBlocks(client, fileData, blockStoreAddr)
					var latestVersion int32
					log.Println("updating file", fileName, "with version", newVersion, "hashList",
						baseDirFileBlocks[fileName].hashList)

					err = client.UpdateFile(&FileMetaData{Filename: fileName,
						Version: newVersion, BlockHashList: baseDirFileBlocks[fileName].hashList},
						&latestVersion)
					if err != nil {
						// Some other client synced this file before us - download latest version from server
						log.Println("UPDATE FILE ERROR: expected version = ", newVersion,
							"remote version = ", latestVersion)
						var newRemoteIndex map[string]*FileMetaData
						CheckErr(client.GetFileInfoMap(&newRemoteIndex))
						newHashList := newRemoteIndex[fileName].BlockHashList

						deleted := (len(newHashList) == 1) && (newHashList[0] == "0")
						if !deleted {
							log.Println("Downloading file blocks for", fileName)
							fileBlocks := DownloadFileBlocks(client, newHashList, blockStoreAddr)
							file, err := os.Create(filePath)
							CheckErr(err)
							defer file.Close()
							for blockNum := range fileBlocks {
								blockSize := fileBlocks[blockNum].BlockSize
								blockData := fileBlocks[blockNum].BlockData
								bytesWritten, err := file.Write(blockData)
								CheckErr(err)
								if int32(bytesWritten) != blockSize {
									log.Fatal("Could not write to file")
								}
							}

						} else {
							log.Println("Deleting file", fileName)
							CheckErr(os.Remove(filePath))
						}
						localIndex[fileName].Filename = fileName
						localIndex[fileName].Version = newRemoteIndex[fileName].Version
						localIndex[fileName].BlockHashList = newHashList
					} else {
						localIndex[fileName].Filename = fileName
						localIndex[fileName].Version = latestVersion
						localIndex[fileName].BlockHashList = baseDirFileBlocks[fileName].hashList
					}
					log.Println("got version", latestVersion)
				}
			} else {
				// Download
				deleted := (len(fileMetaData.BlockHashList) == 1) && (fileMetaData.BlockHashList[0] == "0")
				if !deleted {
					log.Println("Downloading file blocks for", fileName)
					fileBlocks := DownloadFileBlocks(client, fileMetaData.BlockHashList,
						blockStoreAddr)
					file, err := os.Create(filePath)
					CheckErr(err)
					defer file.Close()
					for blockNum := range fileBlocks {
						blockSize := fileBlocks[blockNum].BlockSize
						blockData := fileBlocks[blockNum].BlockData
						bytesWritten, err := file.Write(blockData)
						CheckErr(err)
						if int32(bytesWritten) != blockSize {
							log.Fatal("Could not write to file")
						}
					}
				} else {
					log.Println("Deleting file", fileName)
					os.Remove(filePath)
				}
				localIndex[fileName].Filename = fileMetaData.Filename
				localIndex[fileName].Version = remoteIndex[fileName].Version
				localIndex[fileName].BlockHashList = fileMetaData.BlockHashList
			}
		}
	}
	CheckErr(client.GetFileInfoMap(&remoteIndex))
	err = WriteMetaFile(localIndex, client.BaseDir)
	baseDirFileBlocks = ComputeBaseDirFileBlocks(client.BaseDir, client.BlockSize)
	localIndex, err = LoadMetaFromMetaFile(client.BaseDir)
	CheckErr(err)

	// Handling files that are not present in the remote store
	log.Println("Iterating through baseDir")
	for fileName, fileData := range baseDirFileBlocks {
		fileBlockHashes := fileData.hashList
		if fileName != DEFAULT_META_FILENAME {
			filePath, err := filepath.Abs(ConcatPath(client.BaseDir, fileName))
			CheckErr(err)
			_, localIndexContainsFile := localIndex[fileName]
			_, remoteIndexContainsFile := remoteIndex[fileName]
			if !localIndexContainsFile && !remoteIndexContainsFile {
				/*
					Y N N case
					We have a new local file that doesn't exist in the local index or
					the remote index
				*/
				localIndex[fileName] = new(FileMetaData)
				localIndex[fileName].Filename = fileName
				UploadFileBlocks(client, fileData, blockStoreAddr)
				var latestVersion int32
				err = client.UpdateFile(&FileMetaData{Filename: fileName, Version: 1, BlockHashList: fileBlockHashes}, &latestVersion)

				if err != nil {
					// Some other client synced this file before us - download latest version from server
					log.Println("UPDATE FILE ERROR: file = ", fileName, "expected version = ", 1,
						"remote version = ", latestVersion)
					var newRemoteIndex map[string]*FileMetaData
					CheckErr(client.GetFileInfoMap(&newRemoteIndex))
					newHashList := newRemoteIndex[fileName].BlockHashList
					deleted := (len(newHashList) == 1) && (newHashList[0] == "0")
					if !deleted {
						log.Println("Downloading file blocks for", fileName)
						fileBlocks := DownloadFileBlocks(client, newHashList, blockStoreAddr)
						file, err := os.Create(filePath)
						CheckErr(err)
						defer file.Close()
						for blockNum := range fileBlocks {
							blockSize := fileBlocks[blockNum].BlockSize
							blockData := fileBlocks[blockNum].BlockData
							bytesWritten, err := file.Write(blockData)
							CheckErr(err)
							if int32(bytesWritten) != blockSize {
								log.Fatal("Could not write to file")
							}
						}
					} else {
						log.Println("Deleting file", fileName)
						CheckErr(os.Remove(filePath))
					}
					localIndex[fileName].Version = newRemoteIndex[fileName].Version
					localIndex[fileName].BlockHashList = newHashList
				} else {
					localIndex[fileName].Version = latestVersion
					localIndex[fileName].BlockHashList = fileBlockHashes
				}
				log.Println("got version", latestVersion)
			} else if localIndexContainsFile && !remoteIndexContainsFile {
				/*
					Y Y N case
					File is present only in the baseDir and local index.
					This is not possible
				*/
			} else if remoteIndexContainsFile && !localIndexContainsFile {
				/*
					Y N Y case
					File is present only in the baseDir and remote index.
				*/
				log.Println("file", fileName, "remote version:", remoteIndex[fileName].Version)

				fileMetaData := remoteIndex[fileName]
				deleted := (len(fileMetaData.BlockHashList) == 1) && (fileMetaData.BlockHashList[0] == "0")
				if !deleted {
					log.Println("Downloading file blocks for", fileName)
					fileBlocks := DownloadFileBlocks(client, fileMetaData.BlockHashList,
						blockStoreAddr)
					file, err := os.Create(filePath)
					CheckErr(err)
					defer file.Close()

					for blockNum := range fileBlocks {
						blockSize := fileBlocks[blockNum].BlockSize
						blockData := fileBlocks[blockNum].BlockData
						bytesWritten, err := file.Write(blockData)
						CheckErr(err)
						if int32(bytesWritten) != blockSize {
							log.Fatal("Could not write to file")
						}
					}
				} else {
					log.Println("Deleting file", fileName)
					CheckErr(os.Remove(filePath))
				}
				localIndex[fileName] = new(FileMetaData)
				localIndex[fileName].Filename = fileMetaData.Filename
				localIndex[fileName].Version = fileMetaData.Version
				localIndex[fileName].BlockHashList = fileMetaData.BlockHashList
			} else {
				/*
					Y Y Y case
					The file is present in baseDir, local and remote
				*/
				log.Println("file", fileName, "local version:", localIndex[fileName].Version,
					"remote version:", remoteIndex[fileName].Version)
				if localIndex[fileName].Version == remoteIndex[fileName].Version {
					if !HashListsEqual(remoteIndex[fileName].BlockHashList, baseDirFileBlocks[fileName].hashList) {
						// Upload
						newVersion := localIndex[fileName].Version + 1
						fileData := baseDirFileBlocks[fileName]
						UploadFileBlocks(client, fileData, blockStoreAddr)
						var latestVersion int32

						err = client.UpdateFile(&FileMetaData{Filename: fileName, Version: newVersion, BlockHashList: baseDirFileBlocks[fileName].hashList}, &latestVersion)
						if err != nil {
							// Some other client synced this file before us - download latest version from server
							log.Println("UPDATE FILE ERROR: expected version = ", newVersion,
								"remote version = ", latestVersion)
							var newRemoteIndex map[string]*FileMetaData
							CheckErr(client.GetFileInfoMap(&newRemoteIndex))
							newHashList := newRemoteIndex[fileName].BlockHashList
							deleted := (len(newHashList) == 1) && (newHashList[0] == "0")
							if !deleted {
								log.Println("Downloading file blocks for", fileName)
								fileBlocks := DownloadFileBlocks(client, newHashList, blockStoreAddr)
								file, err := os.Create(filePath)
								CheckErr(err)
								defer file.Close()
								for blockNum := range fileBlocks {
									blockSize := fileBlocks[blockNum].BlockSize
									blockData := fileBlocks[blockNum].BlockData
									bytesWritten, err := file.Write(blockData)
									CheckErr(err)
									if int32(bytesWritten) != blockSize {
										log.Fatal("Could not write to file")
									}
								}
							} else {
								log.Println("Deleting file", fileName)
								CheckErr(os.Remove(filePath))
							}
							localIndex[fileName].Version = newRemoteIndex[fileName].Version
							localIndex[fileName].BlockHashList = newHashList
						} else {
							localIndex[fileName].Version = latestVersion
							localIndex[fileName].BlockHashList = fileBlockHashes
						}
						log.Println("got version", latestVersion)
					}
				} else {
					// Download

					fileMetaData := remoteIndex[fileName]
					deleted := (len(fileMetaData.BlockHashList) == 1) && (fileMetaData.BlockHashList[0] == "0")
					if !deleted {
						log.Println("Downloading file blocks for", fileName)
						fileBlocks := DownloadFileBlocks(client, fileMetaData.BlockHashList,
							blockStoreAddr)
						filePath, err := filepath.Abs(ConcatPath(client.BaseDir, fileName))
						CheckErr(err)
						file, err := os.Create(filePath)
						CheckErr(err)
						defer file.Close()

						for blockNum := range fileBlocks {
							blockSize := fileBlocks[blockNum].BlockSize
							blockData := fileBlocks[blockNum].BlockData
							bytesWritten, err := file.Write(blockData)
							CheckErr(err)
							if int32(bytesWritten) != blockSize {
								log.Fatal("Could not write to file")
							}
						}
					} else {
						log.Println("Deleting file", fileName)
						CheckErr(os.Remove(filePath))
					}
					localIndex[fileName].Filename = fileMetaData.Filename
					localIndex[fileName].Version = fileMetaData.Version
					localIndex[fileName].BlockHashList = fileMetaData.BlockHashList
				}
			}
		}
	}

	err = WriteMetaFile(localIndex, client.BaseDir)
	CheckErr(err)
}
