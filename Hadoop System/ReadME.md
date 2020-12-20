# CS 425 MP 1

Group 20: Garvit Narang (gnarang2), Hemang Nehra (hnehra2)

# Description

For MP3, we built Hadoop.

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
To run Mapple: maple <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <files>
To run Juice: juice <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename>
```