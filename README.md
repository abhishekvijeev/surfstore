# Surfstore

Surfstore is a simple networked file storage application that lets us sync files to a from a remote file service (presumable hosted on the cloud). Multiple clients can concurrently connect to the SurfStore service to access a common, shared set of files. Clients accessing SurfStore “see” a consistent set of updates to files, but SurfStore does not offer any guarantees about operations across files, meaning that it does not support multi-file transactions (such as atomic move).

Surfstore is composed of two services:

1. Blockstore - The content of each file in SurfStore is divided up into chunks, or blocks, each of which has a unique identifier. This service stores these blocks, and when given an identifier, retrieves and returns the appropriate block. A file in SurfStore is broken into an ordered sequence of one or more blocks. Each block is of uniform size except for the last block in the file, which may be smaller (but must be at least 1 byte large). For each block, a hash value is generated using the SHA-256 hash function. This set of hash values, in order, represents the file, and is referred to as the file's hashlist. Given a block, you can compute its hash by applying the SHA-256 hash function to the block. This means that if you change the data in a block, its hash value will change as a result. To update a file, you change a subset of the bytes in the file, and recompute the hashlist. Depending on the modification, at least one, but perhaps all, of the hash values in the hashlist will change.

2. Metastore - The MetaStore service manages the metadata of files and the entire system. Most importantly, the MetaStore service holds the mapping of filenames to blocks. Furthermore, it is aware of all available BlockStores and maps blocks to particular BlockStores.

## Versioning

Each file/filename is associated with a version, which is a monotonically increasing positive integer. The version is incremented any time the file is created, modified, or deleted. The purpose of the version is so that clients can detect when they have an out-of-date view of the file hierarchy. Every time a file is modified, its version number is incremented. SurfStore only records modifications to files if the version is exactly one larger than the currently recorded version. An important aspect of surfstore's design is the fact that if multiple clients try to update the remote service with the same file having the same version number (but different contents),  whoever succeeds in syncing their changes to the cloud first wins. The other client must download changes before they are able to upload their own changes.

## Deleting Files

To delete a file, the MetaStore service records a versioned “tombstone” update. This update simply indicates that the file has been deleted. In this way, deletion events also require version numbers, which prevents race conditions that can occur when one client deletes a file concurrently with another client deleting that file. Note that this means that a deleted file must be recreated before it can be read by a client again. If a client tries to delete a file that has already been deleted, that's alright - we just handle the version numbers of these tombstone updates appropriately.
To represent a “tombstone” record, we will set the file’s hash list to a single hash value of “0” (a string with one character which is the 0 character).

## Sync Operations

Clients “sync” a local base directory with the SurfStore cloud service. When a client is invoked, the sync operation will occur, and then the client will exit. As a result of syncing, new files added to your base directory will be uploaded to the cloud, files that were sync’d to the cloud from other clients will be downloaded to your base directory, and any files which have "edit conflicts" will be resolved.

Clients create and maintain an index.txt file in the base directory which holds local, client-specific information that must be maintained between invocations of the client. In particular, the index.txt contains a copy of the server’s FileInfoMap accurate as of the last time that sync was called. The purpose of this index file is to detect files that have changed, or been added to the base directory since the last time that the client executed. The format of the index.txt file is one line per file, where each line has the filename, the version, and then the hash list. The filename is separated from the version number with a comma, and the version number is separated from the hash list with a comma. Entries in the hash list are separated by spaces. 

## Fault Tolerance

Data blocks are immutable and cannot be updated (since doing so would change their hash values, and thus they’d become entirely new blocks) - therefore, replicating blocks is quite easy. On the other hand, replicating the MetaStore service is quite challenging, because multiple clients can update the Metadata of a file in a concurrent manner. To ensure that the Metadata store is fault tolerant and stays consistent regardless of failures, we can implement it as a replicated state machine using the RAFT consensus protocol.

Each fault tolerant meta store communicates with other MetaStores via GRPC. Each server is aware of all other possible servers (from the configuration file), and new servers do not dynamically join the cluster (although existing servers can “crash”). Leaders are through the SetLeader API call, so there are no elections.

Using the protocol, if the leader can query a majority quorum of the nodes, it will reply back to the client with the correct answer.  As long as a majority of the nodes are up and not in a crashed state, the clients should be able to interact with the system successfully.  When a majority of nodes are in a crashed state, clients should block and not receive a response until a majority are restored.  Any clients that interact with a non-leader should get an error message and retry to find the leader.

Note:

1. Elections do not take place; the leader is set deterministically through a SetLeader API. The SetLeader function should emulate an election, so after calling it on a node it should set all the state as if that node had just won an election. Because elections do not take place, there are only two node states, Leader and Follower. The candidate state does not exist.

2. There is no heartbeat timer. Heartbeats are triggered through a SendHeartbeat API. There is no heartbeat countdown, so once the node is in the leader state, it stays in that state until there is another leader. A node could find out about another leader either through receiving a heartbeat from another leader with a higher term, or receiving a RPC response that has a higher term.




