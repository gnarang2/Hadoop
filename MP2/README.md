# CS 425 MP 1

Group 20: Garvit Narang (gnarang2), Shubham Gangil (sgangil2), Hemang Nehra (hnehra2)

# Description

For MP2, we built a simple distributed file system with All to All failure detection tolerant upto 3 simultaneous failures.

# Build Instructions

```
To build and run, input the following command.
"javac *.java"
```

```
If port is currently being used: 
ps -ef | grep java
kill -9 PID
```

# Running Instructions

```
java Runner log
"log" is the name of the file to which the program will write its logs. 
```

```
To Join the group, input "JOIN" after the "Enter Command >" option comes. Similarly you can "LEAVE" the group.
```

```
To print current member's ID, input "ID"
To print all the members in the list, input "MEMBERS"
To put a file input "PUT lfsfileName dfsFileName"
To get a file input "GET lfsfileName dfsFileName"
To delete a file input "PUT dfsFileName"
To see all files input "SDFS" at the master
To see all files node wise input "NODES" at the master
To see all files in local distributed system input "STORE dfsName"
To see all nodes storing a file input "LS dfsFileName"
```

# Design

In any operation the client first contacts the master to get information related to where the file should be stored, or
where the file is stored depending upon the type of operation. The master maintains a list of all servers where the file is
stored and accordingly provides information to the client. The client upon receiving the information directly contacts the 
servers and requests for file, puts file, or deletes file. The master is responsible for checking periodically that there are
at least 4 replicas maintained so that up to 3 simultaneous failures are tolerated. Everytime a master detects an obselete file or
detects that the numbers of replicas aren't 4, it sends a request to a datanode not containing that file to fetch it from another data 
node. This is regarding the communication between datanodes, masters, and clients. 

Whenever a datanode/client tries to send over a file to another datanode/client at most 5Mb of data is sent in one go in order to 
allow the system to maintain the heap space efficiently. Through this we were able to achieve file transfers of 1GB in less than 25 seconds. Everytime, 50Mb of data is sent overall, files are written so that the data received so far can be flushed onto the files,
and repeated after which the files are appended.

The master maintains a SDFS structure which consists of a hashmap in which each key (file name) corresponds to the file's information. 
The file's information is also a data structure which consists of yet another hash map consisting of the ip addresses storing the file as keys and the values as FileInformation Structure. FileInformation structure contains fileName, updateCount (in order to help in replication), and status (Readable, Writeable, Replicating).

Each datanode maintains a local DFS which consists of a hashmap mapping each key (fileName) to a value of the FileInformation structure in accordance with the Master node's SDFS.

Our system is fault tolerant as it acts appropriately in datanodes crashing, clients crashing in the midst of operations, along with noisy messages. Furthermore, it also corrects incoherrencies with the Master's SDFS and DataNode's LDFS on its own periodically.

