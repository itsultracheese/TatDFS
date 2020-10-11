# TatDFS: The Amazing Tatar Distributed File System
Kamaliev Kamil, DS-02

Miftahova Amina, DS-01

## Architecture
![](https://i.imgur.com/j6acOnD.png)
### Client
The client application is only aware of the namenode address and external view of the filesystem. On a command, the request is sent to the namenode and the needed actions are taken. Transparetnly to the user, the client application also contacts datanodes on operations such as `get <file>`. That is done on a client side in order to decrease the load on the Namenode.

For the convenience we have provided the user of the client application with the custom terminal, that has a partial support of the Linux terminal commands.

### Namenode
The namenode is aware of the whole Distributed File System structure: datanodes addresses, external view of the filesystem and each datanode filesystem. It serves as the intermediate node between the client and datanode and either performs the operations itself, as long as they are in namenode's range of responsibilies, either redirects the clients to the datanodes. When redirecting the client, it provides all necessary information to contact the datanode and ask for the needed operation.

Another task of the namenode is to keep files replicated even if some of the datanodes fail. For that it sends a **heartbeat** of a fixed rate (1 second for testing purposes in order not to wait for a long time) to each datanode and maintains the list of the dead and alive datanodes. Moreover, on each heartbeat it tracks files missing replicas and tries to restore the replicas needed if possible.

### File System
The file system is a component responsible for mapping the external hierarchical FS representation to the flat datanodes FS. Each file stored in DFS is uniquely identified, firstly, by its hierarchical name, as it is visible to the client, second, by its unique id, which is used for storing the files in the datanodes.

For the filesystem tree we have used the `anytree` module. It provides the conevient searching and path resolution while storing a regular tree structure. Each node is either directory, either file. Each file node additionaly stores file information: `{id, size, creation_date}`.

### Datanodes
The datanodes are just responsible for storing files. They know nothing about the distributed file system, they are only aware of the files stored inside them.


## Communication protocols

For the communication we have used the REST API.

### Initialization
Initialization is done by `init` command.

During it, client contacts namenode. If the connetcion was established, client sends 
```
GET <namenode>\init
``` 
request.

While processing this request, namenode contacts each datanode and sends each of them 
```
GET <datanode>/format
```
request which makes them format their file systems.

### File create
Creation of the file is done by `touch file_name` command.

During it, client sends 
```
POST <namenode>/create?filename=<filename>&filesize=<size>
``` 
request to namenode.

While processing this request, namenode updates its File System and sends to the client addresses of datanodes where the file will be stored.

Then, client sends 
```
POST <datanode>/create?file_id=<id>
``` 
request to the corresponding datanode.

While processing this request, each datanode creates an empty file setting its name to the id of the file.

### File put
Uploading of the file to dfs is done by `put local_file_name dfs_file_name` command.

During it, client sends 
```
POST <namenode>/create?filename=<filename>&filesize=<size>
``` 
request (which was already described) to namenode

Then, client sends 
```
POST <datanode>/create?files={<id>: <file_content>}
``` 
request to the corresponding datanode.

While processing this request, each datanode downloads it from the client and saves to its file system.

### File get
Downloading of the file from dfs is done by `get filename` command.

During it, client sends 
```
GET <namenode>/get?filename=<filename>
``` 
request to namenode which then replies with the addresses of datanodes from which the file may be obtained.

Then, client sends 
```
GET <datanode>/get?file_id=<id>
``` 
request to one of the datanodes and downloads the file.

### File copy
Copying the file is done by `cp file1 file2` command.

Client sends 
```
POST <namenode>/copy?filename=<filename>&dirname=<dirname>
``` 
request to namenode which makes it update its file system and send 2 lists:
- datanodes storing original file
- datanodes that will store the copy of the file

If datanode D1 presents in both of the lists, client sends it
```
POST <datanode>/copy/existing?original_id=<old_id>&copy_id=<new_id>
``` 
request. For D1, it's enough to copy file from its fileystem and save it using another name.

If datanode D2 presents only in the 2nd list, client sends it 
```
POST <datanode>/copy/non-existing?original_id=<old_id>&copy_id=<new_id>
``` 
request. In order to create a copy of the file, D2 needs to send `/get` request to some of the node in the 1st list to obtain it.

### File move
Moving file is done by `mv filename directory` command.

Client sends 
```
POST <namenode>/move?filename=<filename>&path=<path>
``` 
request to namenode which makes it update the file system.

### File delete
Deleting file is done by `rm filename` command.

Client sends 
```
DELETE <namenode>/delete?filename=<filename>
``` 
request to namenode making it to update the file system and send back the list of datanodes storing the file.

Then, client sends 
```
DELETE <datanode>/delete?file_id=<id>
``` 
request to the corresponding datanodes making them delete the file.

### Create directory
Creating a directory is done by `mkdir dirname` command.

Client sends 
```
POST <namenode>/mkdir?dirname=<dirname>
``` 
request to namenode which makes it update the file system.

### Change directory
Changing the directoty is done by `cd dirname` command.

Client sends 
```
POST <namenode>/cd?dirname=<dirname>
``` 
request to namenode which maked it updathe the current directory.

### List directory
List directory is done by `ls dirname` command.

Client sends 
```
GET <namenode>/ls?dirname=<dirname>
``` 
request to namenode making it send back the files and folders in the stated directory.

### Remove directory
Remove directory is done by `rmdir dirname` command.

Client sends 
```
DELETE <namenode>/delete/dir-notsure?dirname=<dirname>
``` 
request to namenode. Namenode checks if the directory is empty. If so, it deletes it. Otherwise, it sends back the notification that the direcotry is not empty. 

If client wants to delete non-empty directory it sends 
```
DELETE <namenode>/delete/dir-sure?dirname=<dirname>
``` 
request to namenode. Namenode recursively goes through each folder, obtains files and sends back the list of files and datanodes where those files are located.

Then, client sends 
```
DELETE <datanode>/delete?file_id=<id>
```  
requests to corresponding datanodes.

## Team members contribution

The most part of the code was written from the 1 laptop with pair-programming practice which made debugging a lot easier. Below is the main things that each team member focused on:

**Kamil Kamaliev**
* replication
* init, create, put in client and datanodes
* get files, delete files
* copy in client, datanode
* remove dir in client, datanode

**Amina Miftahova**
* path resolution
* init, create, put in FileSystem and Namenode
* mkdir, ls, cd, move, info
* custom shell
* copy in namenode
* remove dir in namenode

## Links to DockerHub

[Namenode image](https://hub.docker.com/repository/docker/tootiredone/namenode)

[Datanode image](https://hub.docker.com/repository/docker/tootiredone/datanode)
