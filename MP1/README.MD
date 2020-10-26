# ECE428

Distributed Failure Detection System

# How to Use

## Using CMD arguments
 * Type "help"

## Other commands
 * "python3 Node.py" to run the file


## Idea
Gossip style and all to all failure detection system which can be switched without restarting. For both the idea is same which involves sending membership list with incremented heartbeat counter to all relevant nodes (all nodes in all to all and 3 nodes in gossip style). Upon receiving the membership list the node updates it current membership list based upon the heartbeats. Another thread is simultaneously checking for failures and if it doesn't update a segment of the membership list it after a certain amount of time, it marks the 

## Things to know
 * "fa20-cs425-g20-XX.cs.illinois.edu" VM IPs
 * start service on VMs with "python3 Node.py" from the introducer which is (fa20-cs425-g20-01.cs.illinois.edu)
 * find background process pid with "jobs -l". Find background process that uses specific port number by 'sudo lsof -i:<port number>'
 * kill process with "kill PID". If this fails, use "kill -9 PID"
 * Run other machines the same way but type "join" in them to make them join the group
