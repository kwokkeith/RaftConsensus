package main

import (
	"RAFTCONSENSUS/miniraft"
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
)

//=========================================
//=========================================
// Variables
//=========================================
//=========================================

// For serverstate
type ServerState int
const (
    Leader ServerState = iota // 0
    Follower // 1
    Candidate // 2
)

var (
	mutex sync.Mutex
	timerMutex sync.Mutex
	heartbeatTimerMutex sync.Mutex
	timerIsActive bool = false;
	serverHostPort string = ""
	serverAddr *net.UDPAddr
	timeOut int
	minTimeOut int = 10;
	maxTimeOut int = 20;
	state ServerState = Follower // Start as a follower
	suspended bool = false // To state if a state has been suspended
	serversFile string // Path to the persistent storage of the list of servers
	term int = 0
	nextLogIndex int = 1 // Next log entry for the current server
	nextIndex map[string]int // List of servers and their index of the next log entry to be sent to that server
	matchIndex map[string]int // List of servers and their index of highest log entry known to be replicated (commitIndex)
	timeOutCounter *time.Timer; 
	servers []string
	leader string = "" // IP:Port address of the leader
	leaderVotedFor string = ""
	logs []miniraft.LogEntry
	commitIndex int = 0
	lastAppliedIndex int = 0 // Similar to commitIndex in this assignment
	voteReceived int = 1// Count number of votes received for this term
	hearbeatInterval int = 5;
	heartbeatTimerIsActive bool = false;
)

//=========================================
//=========================================
// Getter/Setter Variables
//=========================================
//=========================================

// Getter and Setter for server HostPort Address
func setServerHostPort(hostPort string){
	mutex.Lock()
	defer mutex.Unlock()
	serverHostPort = hostPort 
}

func getServerHostPort() string{
	mutex.Lock()
	defer mutex.Unlock()
	return serverHostPort
}

//=========================================
//=========================================
// Server startup
//=========================================
//=========================================

// Function to start server on the defined address
func startServer(serverHostPort string){
	addr, err := net.ResolveUDPAddr("udp", serverHostPort)
	if err != nil { 
		log.Fatal(err)
	}
	serverAddr = addr

	// Generate random timeout
	timeOut = rand.Intn(maxTimeOut - minTimeOut + 1) + minTimeOut

	// Start communication (looped to keep listening until exit)
	handleCommunication()	
}

/* Checks if server is in the list of servers, else append it */
func updateServerList(serverHostPort string, new_serversFile string) {
	// Open the file in read-write mode, create it if not exists
    file, err := os.OpenFile(new_serversFile, os.O_RDWR|os.O_CREATE, 0666)
    panicCheck(err)
    defer file.Close()

	// Update the current server's knowledge of the persistent storage location
	serversFile = new_serversFile

    // Check if the current address is already in the file
    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        if scanner.Text() == serverHostPort {
			// defined serverHostPort already exist in the list
			// Throw error and leave the program
			log.Printf("Server address %s defined already exist in %s", serverHostPort, serversFile)
			os.Exit(1)
        }
    }

    err = scanner.Err()
	panicCheck(err)

    // Append the current address if it doesn't exist in the file
	_, err = file.WriteString(serverHostPort + "\n")
	panicCheck(err)
	
	// Debug: Status announce that address has been added to the file containing list of servers
	fmt.Printf("%s added into %s\n", serverHostPort, serversFile)
}

// Checks if there is error, if so, then panic
func panicCheck(e error) {
    if e != nil {
        panic(e)
    }
}

/* startCommandInterface runs the interactive 
 command interface for the Server Process */
func startCommandInterface() {
	reader := bufio.NewReader(os.Stdin)                                      // Create a reader for stdin
	fmt.Println("Server Process command interface started. Enter commands:")

	for {
		fmt.Print("> ")                     // Print the command prompt
		cmd, err := reader.ReadString('\n') // Read a command from stdin
		if err != nil {
			fmt.Println("Error reading command:", err) // Handle errors in reading commands
			continue
		}
		cmd = strings.TrimSpace(cmd) // Remove trailing newline characters from the command

		// Switch between different command keywords
		switch cmd {
			case "exit":
				// Handle the 'exit' command.
				fmt.Println("Exiting Server Process.") 
				log.Printf("[startCommandInterface] Exit command received. serverHostPort: %s", serverHostPort)
				os.Exit(0)
			case "log":
				fmt.Printf("Log Entries for %s:\n", serverHostPort)
				fmt.Printf("| %10s | %10s | %s\n", "Index", "Term", "CommandName")
				fmt.Printf("|%s|\n", strings.Repeat("-", 40))
				for _, log := range logs {
					fmt.Printf("| %10d | %10d | %s\n", log.Index, log.Term, log.CommandName)
				}
			case "print":
				fmt.Printf("Summary of %s\n", serverHostPort)
				fmt.Printf("Current Term: %d\n", term)
				fmt.Printf("Leader voted for: %s\n", string(leaderVotedFor))
				fmt.Printf("State: %d\n", state)
				fmt.Printf("Commit Index: %d\n", commitIndex)
				fmt.Printf("Last Applied Index: %d\n", lastAppliedIndex)
				fmt.Printf("Next Index:\n")
				for key, value := range nextIndex {
					fmt.Printf("%s : %d\n", key, value)
				} 
				fmt.Printf("Match Index:\n")
				for key, value := range matchIndex {
					fmt.Printf("%s : %d\n", key, value)
				}
				fmt.Printf("Timeout: %d\n", timeOut)
				fmt.Printf("Current Leader: %s\n", leader)
			case "resume":
				if suspended {
					fmt.Println("Resuming server")
					suspended = false
				} else {
					fmt.Println("Server was not in suspended state")
				}	
			case "suspend":
				if suspended {
					fmt.Println("Server is already suspended")
				} else {
					fmt.Println("Suspending Server")
					suspended = true
				}
			default:
				fmt.Println("Command is unrecognised")
		}
	}
}

//=========================================
//=========================================
// Server Timeout
//=========================================
//=========================================

/* Check timeout for client */
func handleTimeOut(){
	// Change to candidate state
	if state == Leader {
		return
	}
	state = Candidate;

	// Send out votes
	// Create an instance of RequestVoteRequest
	term++ // Update term for election
	request := &miniraft.RequestVoteRequest{
		Term:          uint64(term), // Add one to the term when becoming a candidate
		LastLogIndex:  GetLastLogIndex(), 
		LastLogTerm:   GetLastLogTerm(),
		CandidateName: serverHostPort,
	}

	// wrap the request in a raft message
	message := &miniraft.Raft{
		Message: &miniraft.Raft_RequestVoteRequest{
			RequestVoteRequest: request,
		},
	}

	// Set new random timeOut for leader
	timeOut = rand.Intn(maxTimeOut - minTimeOut + 1) + minTimeOut

	// To request for votes
	broadcastMessage(message)
}

//=========================================
//=========================================
// Server Communication
//=========================================
//=========================================

/* This function sets up the listening port for the server to receive
messages from other servers */
func handleCommunication(){
	listener, err := net.ListenUDP("udp", serverAddr)
	if err != nil { 
		log.Fatal(err)
	}

	// Start Communication
	for { 
		data := make([]byte, 65536)
		length, addr, err := listener.ReadFromUDP(data)
		if err != nil {
			log.Fatal(err)
		}

		// Stop any timer that was running
		// Start a timer for time to check timeout if current server is not a leader
		if state != Leader {
			resetTimer()
		}

		// Debug message to check message received
		log.Printf("From %s: %v\n", addr.String(), data[:length])

		// Handles the message based on its type
		handleMessage(data[:length]);
	}
}

/* Function to handle the message based on its type */
func handleMessage(data []byte){
	// Unmarshal the message
	message := &miniraft.Raft{}
	err := proto.Unmarshal(data, message)
	if err != nil {
		log.Printf("Failed to unmarshal message: %v\n", err)
	}

	// Debug message to check unmarshalled message
	log.Printf("Unmarshalled message: %v\n", data)	

	// Switch between different types of protobuf message
	mutex.Lock() 
	defer mutex.Unlock() // Release the mutex
	if suspended {
		fmt.Println("Received message but suspended.")
		return 
	}
	switch msg := message.Message.(type) {
	case *miniraft.Raft_CommandName:
		fmt.Println("Received CommandName response: ", msg.CommandName)
		handleCommandName(*msg)
	case *miniraft.Raft_AppendEntriesRequest:
		fmt.Println("Received AppendEntriesRequest: ", msg.AppendEntriesRequest)
		handleAppendEntriesRequest(*msg)
	case *miniraft.Raft_AppendEntriesResponse:
		fmt.Println("Received AppendEntriesResponse: ", msg.AppendEntriesResponse)	
	case *miniraft.Raft_RequestVoteRequest:
		fmt.Println("Received RequestVoteRequest: ", msg.RequestVoteRequest)	
		handleRequestVoteRequest(*msg)
	case *miniraft.Raft_RequestVoteResponse:
		fmt.Println("Received RequestVoteResponse: ", msg.RequestVoteResponse)
		handleRequestVoteResponse(*msg)
	default:
		log.Printf("[handleMessage] Received an unknown type of message: %T", msg)
	}
}

/* Function to broadcast msg to all servers that are in the server list */
func broadcastMessage(message *miniraft.Raft){
	updateServersKnowledge() // To update list of known servers

	// If server is suspended
	if suspended {
		return
	}

	log.Printf("Broadcasing message...\n")

	// Send vote Request to other followers
	msg, err := proto.Marshal(message)
	if err != nil {
		log.Fatal("Error when sending message")
	}

	// Iterate over the server addresses
	for _, addr := range servers {
		if addr != serverHostPort {
			conn, err := net.Dial("udp", addr)
			if err != nil {
				log.Printf("Failed to dial UDP server %s: %v", addr, err)
				continue
			}
			defer conn.Close()
	
			// Send a message to the server
			_, err = conn.Write([]byte(msg))
			if err != nil {
				log.Printf("Failed to send message to %s: %v", addr, err)
				continue
			}
	
			fmt.Printf("Message sent to %s\n", addr)
		}
	}
}

//=========================================
//=========================================
// Server Message Handling
//=========================================
//=========================================

func handleCommandName(message miniraft.Raft_CommandName) {
	// Check if current leader is this server
	if leader == "" {
		// if leader has not been established
		// drop the message (Could also buffer it)
		return 
	} else if leader == serverHostPort {
		// Send out the command name to the others		
		// Create a log object
		// TODO: Create log object	
		logEntry := miniraft.LogEntry{
			Index: uint64(len(logs)) + 1,
			Term: uint64(term),
			CommandName: message.CommandName,
		}

		// Get last log index and term before sending out AppendEntriesRequest
		var prevLogIndex = GetLastLogIndex()
		var prevLogTerm = GetLastLogTerm()

		logs = append(logs, logEntry) // Leader adds command to its log

		// Leader sends AppendEntries to  message to followers
		request := miniraft.AppendEntriesRequest{
			Term: uint64(term),
			LeaderId: serverHostPort,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm: prevLogTerm,
			Entries: []*miniraft.LogEntry{
				&logEntry,
			},
			LeaderCommit: uint64(commitIndex),
		}

		message := &miniraft.Raft{
			Message: &miniraft.Raft_AppendEntriesRequest{
				AppendEntriesRequest: &request,
			},
		}

		broadcastMessage(message)
	} else {
		// wrap the command in a raft message
		message := &miniraft.Raft{
			Message: &miniraft.Raft_CommandName{
				CommandName: message.CommandName,
			},
		}
		SendMiniRaftMessage(leader, message)
	}
}

func handleAppendEntriesRequest(message miniraft.Raft_AppendEntriesRequest) {
	var success = true

	// Set leader (Neutralise old leader too)
	// if this server thinks it is the leader
	// If this leader is older then neutralise itself and become a follower, also update its term
	if state == Leader {
		if message.AppendEntriesRequest.GetTerm() > uint64(term) {
			leader = message.AppendEntriesRequest.GetLeaderId()
			state = Follower
			term = int(message.AppendEntriesRequest.GetTerm())
		}
	} else if state == Candidate {
		// If this server is a candidate then it yields to another candidate that has became a leader
		if message.AppendEntriesRequest.GetTerm() >= uint64(term) {
			leader = message.AppendEntriesRequest.GetLeaderId()
			state = Follower
			term = int(message.AppendEntriesRequest.GetTerm())	
		}
	} else {
		leader = message.AppendEntriesRequest.GetLeaderId()
	}
	

	// Checks to get success result for Response message
	if message.AppendEntriesRequest.GetTerm() < uint64(term) {
		success = false
	} 
	
	if len(logs) >= int(message.AppendEntriesRequest.GetPrevLogIndex()) {
		if message.AppendEntriesRequest.GetPrevLogIndex() != 0 {
			// Check if logs contains an entry at prevLogIndex whose term matches prevLogTerm
			if logs[message.AppendEntriesRequest.GetPrevLogIndex()-1].GetTerm() != message.AppendEntriesRequest.GetPrevLogTerm() {
				success = false
			}
		}
	}

	// Check if there are conflicts in existing entry with new ones, delete the existing entry and all that follows it
	for _, newEntry := range message.AppendEntriesRequest.GetEntries() {
		// Check length of log
		if len(logs) < int(newEntry.GetIndex()){
			continue
		}
		if logs[newEntry.GetIndex() - 1].GetTerm() != message.AppendEntriesRequest.GetTerm() {
			// Delete the existing entries that follow it
			logs = logs[:newEntry.GetIndex() - 1]
		}
	}

	// Append any new entries not already in the log
	for _, newEntry := range message.AppendEntriesRequest.GetEntries() {
		// Check if there is an entry at the index of the new entry
		// If newEntry's index is more than the length of the log
		if len(logs) < int(newEntry.GetIndex()) {
			// append new logs (But we need to check if they have prev log)
			// ***************************************************
			// ***************************************************
			// TODO: Implement appending algorithm where there needs to be a prev entry
			// ***************************************************	
			// ***************************************************
			// message before appending to log.
		}
	}

	// Update commitIndex
	if message.AppendEntriesRequest.GetLeaderCommit() > uint64(commitIndex) {
		var lastNewEntryIndex = message.AppendEntriesRequest.GetEntries()[len(message.AppendEntriesRequest.GetEntries())-1].Index
		if message.AppendEntriesRequest.GetLeaderCommit() < lastNewEntryIndex {
			commitIndex = int(message.AppendEntriesRequest.GetLeaderCommit())
		} else {
			commitIndex = int(lastNewEntryIndex)
		}
	}

	// Update term
	term = max(term, int(message.AppendEntriesRequest.GetTerm()))

	// Server response to appendEntriesRPC
	response := &miniraft.AppendEntriesResponse{
		Term:          uint64(term), // Add one to the term when becoming a candidate
		Success:  success, 
	}
	
	responseMsg := &miniraft.Raft{
		Message: &miniraft.Raft_AppendEntriesResponse{
			AppendEntriesResponse: response,
		},
	}

	SendMiniRaftMessage(leader, responseMsg)
}

func handleAppendEntriesResponse(message miniraft.Raft_AppendEntriesResponse) {

}

// If receiving a request vote 
func handleRequestVoteRequest(message miniraft.Raft_RequestVoteRequest){
	// Check if server has the pre-requisite to be voted
	// Choose candidate with log most likely to contain all committed entries
	// This is achieved by comparing logs
	var vote miniraft.RequestVoteResponse;

	if (message.RequestVoteRequest.LastLogTerm < GetLastLogTerm() ||
	(message.RequestVoteRequest.LastLogTerm == GetLastLogTerm() &&
	message.RequestVoteRequest.LastLogIndex < GetLastLogIndex() )){
		// Do not give vote to candidate since he is out-dated
		vote = miniraft.RequestVoteResponse{
			Term:          uint64(term), // To update candidate
			VoteGranted:   false,
		}
	} else {
		// Send a vote to the candidate
		leaderVotedFor = message.RequestVoteRequest.CandidateName

		vote = miniraft.RequestVoteResponse{
			Term:          message.RequestVoteRequest.GetTerm(), // Take candidate's term
			VoteGranted:   true,
		}

		// Update own term
		term = int(message.RequestVoteRequest.GetTerm())
	}

	// Set new random timeout
	timeOut = rand.Intn(maxTimeOut - minTimeOut + 1) + minTimeOut 

	// Create Message wrapper for raft message
	voteResponse := &miniraft.Raft{
		Message: &miniraft.Raft_RequestVoteResponse{
			RequestVoteResponse: &vote,
		},
	}	

	// Send message to candidate
	SendMiniRaftMessage(message.RequestVoteRequest.GetCandidateName(), voteResponse)
}


func handleRequestVoteResponse(message miniraft.Raft_RequestVoteResponse){
	// Check if state is still candidate
	updateServersKnowledge()
	if state == Candidate {
		// Count up if vote received is granted
		if message.RequestVoteResponse.VoteGranted {
			voteReceived++
			if voteReceived > len(servers) / 2 {
				// Received majority
				// Send heartbeat to tell servers, this is the new leader
				SendHeartBeat();				
				state = Leader; // Become the new leader
				go SetHeartBeatRoutine(); // Routinely send heartbeat
				leader = serverHostPort // Update who the current leader is
				voteReceived = 1 // Reset counter (1 because leader votes for himself)
				
				// Stop timer too for timeout
				timerMutex.Lock()
				defer timerMutex.Unlock()
				if timerIsActive {
					// Stop the current timer. If Stop returns false, the timer has already fired.
					if !timeOutCounter.Stop() {
						// If the timer already expired and the handleTimeOut function might be in queue,
						// try to drain the channel to prevent handleTimeOut from executing if it hasn't yet.
						select {
						case <-timeOutCounter.C:
						default:
						}
					}
				}	
			}
		}
	}
}

//=========================================
//=========================================
// Server Utilities
//=========================================
//=========================================

// To set timer for handling timeout of servers
func resetTimer() {
    timerMutex.Lock()
    defer timerMutex.Unlock()

    if timerIsActive {
        // Stop the current timer. If Stop returns false, the timer has already fired.
        if !timeOutCounter.Stop() {
            // If the timer already expired and the handleTimeOut function might be in queue,
            // try to drain the channel to prevent handleTimeOut from executing if it hasn't yet.
            select {
            case <-timeOutCounter.C:
            default:
            }
        }
    }

    // Regardless of the previous timer's state, start a new timer.
    timerIsActive = true
    timeOutCounter = time.AfterFunc(time.Second*time.Duration(timeOut), func() {
        handleTimeOut()
        // After the timer executes, reset timerIsActive to false.
        timerMutex.Lock()
        timerIsActive = false
        timerMutex.Unlock()
    })
}


// Function to send the miniraft message
func SendMiniRaftMessage(ipPortAddr string, message *miniraft.Raft) (err error) {
	conn, err := net.ListenPacket("udp", ":0")
	if err != nil {
		log.Fatal(err)
	}	

	addr, err := net.ResolveUDPAddr("udp", ipPortAddr)
		if err != nil {
			log.Fatal(err)
		}

	// Serialize the message
	data, err := proto.Marshal(message)
	log.Printf("SendMiniRaftMessage(): sending %s (%v), %d to %s\n", message, data, len(data), addr.String())
	if err != nil {
		log.Panicln("Failed to marshal message.", err)
	}

	// Send the message over, we dont need to send the size of the message as UDP handles it,
	// but we need to specify the address of where we are sending it to as UDP is stateless and it doesnt rmb where data should be sent 
	_, err = conn.WriteTo(data, addr)
	if err != nil {
		log.Panicln("Failed to send message.", err)
	}
	return
}

func GetLastLogIndex() uint64 {
	if (len(logs) < 1) {
		return 0
	}
	return logs[len(logs) - 1].Index
}

func GetLastLogTerm() uint64 {
	if len(logs) < 1 {
		return 0
	}
	return logs[len(logs) - 1].Term
}

func updateServersKnowledge(){
	// Open the file in read-only mode
	file, err := os.Open(serversFile)
	if err != nil {
		log.Panicf("Failed to open the file: %v", err)
	}
	defer file.Close()

	// Reset servers variable
	servers = nil

	// Use bufio.Scanner to read the file line by line
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		// Add the line to the servers slice
		servers = append(servers, scanner.Text())
	}
	// Check for any errors that occurred during the scan
	if err := scanner.Err(); err != nil {
		log.Fatalf("Error occurred during file scan: %v", err)
	}
}

/* This function sets the heartbeat routine for 
leader to send heartbeat in a fixed interval */
func SetHeartBeatRoutine(){
	for state == Leader {
		heartbeatTimerMutex.Lock()
		if !heartbeatTimerIsActive {
			heartbeatTimerIsActive = true
			timeOutCounter = time.AfterFunc(time.Second*time.Duration(hearbeatInterval), SendHeartBeat);
		}
		heartbeatTimerMutex.Unlock()
	}
}

/* This function is used for the leader to send heartbeats */
func SendHeartBeat(){
	request := miniraft.AppendEntriesRequest{
		Term: uint64(term),
		LeaderId: serverHostPort,
		PrevLogIndex: GetLastLogIndex(),
		PrevLogTerm: GetLastLogTerm(),
		Entries: nil,
		LeaderCommit: uint64(commitIndex),
	}

	// wrap the request in a raft message(AppendEntriesRequest)
	message := &miniraft.Raft{
		Message: &miniraft.Raft_AppendEntriesRequest{
			AppendEntriesRequest: &request,
		},
	}
	broadcastMessage(message)

	heartbeatTimerMutex.Lock()
	heartbeatTimerIsActive = false // Set timer off
	heartbeatTimerMutex.Unlock()
}
//=========================================
//=========================================
// Server Main
//=========================================
//=========================================

// main is the entry point of the program
func main() {
	if len(os.Args) != 3 {
		fmt.Println("Server Usage: go run raftserver.go <server-host:server-port> filename_of_serverList")
		return
	}

	serverHostPort := os.Args[1] // Get the server address from command-line arguments
	serversFile := os.Args[2] // Get the filename of the file containing all the servers
	if !strings.Contains(serverHostPort, ":") {
		fmt.Println("Invalid server address. Must be in the format host:port.") // Validate the registry address format
		return
	}

	// Debugging statement to check the initial value of serverHostPort
	log.Printf("[main] raftServer connecting to serverHostPort: %s", serverHostPort)

	// Sets the server's HostPort value for this client
	setServerHostPort(os.Args[1])

	// Check if server exist in the server list and if not append it to the list
	// If server exist then throw an error and do not start new server with defined address
	updateServerList(serverHostPort, serversFile)
	
	// Starts the server on the defined address
	go startServer(getServerHostPort())

	// Start the interactive command interface
	startCommandInterface() 
}