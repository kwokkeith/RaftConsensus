This project implements the Raft Consensus Algorithm proposed by Ongaro and Ousterhout (2014) in Go lang.
Follow the below steps to execute this implementation.

Server Commands:
1. To start server: go run raftserver.go server-host:server-port filename
2. print: Print the current term, leader voted for, state, commitIndex, and lastApplied index, nextIndex, and matchIndex.
3. log : Print the log entries
4. resume: Switch to resume state
5. suspend: Switch a server to a suspended state. 
It will receive messages from the client and other servers. 
It will not respond to any message while in this state.
It will not send any heartbeat messages.
6. exit: stops the server

Client Commands:
1. To start client: go run raftclient.go server-host:server-port
2. <commandName>: Enter a commandName, which must be a string that is non-empty, can contain lower and upper case letters and digits and dashes and underscores only.
3. exit: stops the client


Make sure that the log files are deleted if you wish to see the results of the committed logs. The implementation does not clear the folder before execution.
- go to logs folder and delete all logs from this folder. Do not delete the logs folder

Clear the servers.txt file in raftserver folder. This text file contains the list of addresses for the servers in the network, and is automatically generated when a server 
has started. The implementation does not clear this file after every execution. 
- go to raftserver folder and either delete servers.txt or clear that file. 
- Note that when you start a server the filename indicates the file that the servers would be tracked on (in the default repositiory we used servers.txt)
