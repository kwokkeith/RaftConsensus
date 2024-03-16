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
	timerIsActive bool = false;
	serverHostPort string = ""
	serverAddr *net.UDPAddr
	timeOut int
	state ServerState = Follower // Start as a follower
	suspended bool = false // To state if a state has been suspended
	serversFile string // Path to the persistent storage of the list of servers
	term int = 0
	nextLogIndex int = 1 // Next log entry for the current server
	nextIndex map[string]int // List of servers and their index of the next log entry to be sent to that server
	matchIndex map[string]int // List of servers and their index of highest log entry known to be replicated (commitIndex)
	lastLogTerm int = 0
	timeOutCounter time.Timer; 
	servers []string
	leader string = ""
	leaderVotedFor string = ""
	logs []miniraft.LogEntry
	commitIndex int = 0
	lastAppliedIndex int = 0
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
	minTimeOut := 100000;
	maxTimeOut := 1000000000;
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
					fmt.Printf("| %10s | %10s | %s\n", string(rune(log.Index)), string(rune(log.Term)), log.CommandName)
				}
			case "print":
				fmt.Printf("Summary of %s\n", serverHostPort)
				fmt.Printf("Current Term: %s\n", string(rune(term)))
				fmt.Printf("Leader voted for: %s\n", string(leaderVotedFor))
				fmt.Printf("State: %s\n", string(rune(state)))
				fmt.Printf("Commit Index: %s\n", string(rune(commitIndex)))
				fmt.Printf("Last Applied Index: %s\n", string(rune(lastAppliedIndex)))
				fmt.Printf("Next Index:\n")
				for key, value := range nextIndex {
					fmt.Printf("%s : %s\n", key, string(rune(value)))
				} 
				fmt.Printf("Match Index:\n")
				for key, value := range matchIndex {
					fmt.Printf("%s : %s\n", key, string(rune(value)))
				}
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
				return
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
	state = Candidate;

	// Send out votes
	// Create an instance of RequestVoteRequest
	request := &miniraft.RequestVoteRequest{
		Term:          uint64(term) + 1, // Add one to the term when becoming a candidate
		LastLogIndex:  uint64(nextLogIndex - 1), 
		LastLogTerm:   GetLastLogIndex(),
		CandidateName: serverHostPort,
	}

	// wrap the request in a raft message
	message := &miniraft.Raft{
		Message: &miniraft.Raft_RequestVoteRequest{
			RequestVoteRequest: request,
		},
	}

	// Send vote Request to other followers
	msg, err := proto.Marshal(message)
	if err != nil {
		log.Fatal("Error when sending voteRequest")
	}
	updateServersKnowledge() // Update the knowledge of which servers are up
	broadcastMessage(msg)
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
		timerMutex.Lock()
		if timerIsActive {
			if !timeOutCounter.Stop() {
				// Try to drain the channel if the timer has already expired
				select {
				case <-timeOutCounter.C:
				default:
				}
			}
			timerIsActive = false
		}

		// Start a timer for time to check timeout
		timerIsActive = true
		timeOutCounter = *time.AfterFunc(time.Duration(timeOut), handleTimeOut);
		timerMutex.Unlock()

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
	case *miniraft.Raft_AppendEntriesRequest:
		fmt.Println("Received AppendEntriesRequest: ", msg.AppendEntriesRequest)
		handleAppendEntriesRequest(*msg)
	case *miniraft.Raft_AppendEntriesResponse:
		fmt.Println("Received AppendEntriesResponse: ", msg.AppendEntriesResponse)	
	case *miniraft.Raft_RequestVoteRequest:
		fmt.Println("Received AppendEntriesResponse: ", msg.RequestVoteRequest)	
		handleRequestVoteRequest(*msg)
	case *miniraft.Raft_RequestVoteResponse:
		fmt.Println("Received AppendEntriesResponse: ", msg.RequestVoteResponse)
		handleRequestVoteResponse(*msg)
	default:
		log.Printf("[handleConnection] Received an unknown type of message: %T", msg)
	}
}

/* Function to broadcast msg to all servers that are in the server list */
func broadcastMessage(data []byte){
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
			_, err = conn.Write([]byte(data))
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
		

	} else {
		// Forward the message to the leader of the cluster
		conn, err := net.ListenPacket("udp", ":0")
		if err != nil {
			log.Fatal(err)
		}
		dst, err := net.ResolveUDPAddr("udp", leader)
		if err != nil {
			log.Fatal(err)
		}
		// wrap the command in a raft message
		message := &miniraft.Raft{
			Message: &miniraft.Raft_CommandName{
				CommandName: message.CommandName,
			},
		}

		SendMiniRaftMessage(conn, dst, message)
	}
}

func handleAppendEntriesRequest(message miniraft.Raft_AppendEntriesRequest) {
	
}

func handleAppendEntriesResponse(message miniraft.Raft_AppendEntriesResponse) {

}

func handleRequestVoteRequest(message miniraft.Raft_RequestVoteRequest){

}

func handleRequestVoteResponse(message miniraft.Raft_RequestVoteResponse){

}
//=========================================
//=========================================
// Server Utilities
//=========================================
//=========================================

/*
Function update the current server's knowledge of other servers 
*/
func updateServersKnowledge(){
	// Open the file in read-only mode
	file, err := os.Open(serversFile)
	if err != nil {
		log.Fatalf("Failed to open the file: %v", err)
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

// Function to send the miniraft message
func SendMiniRaftMessage(conn net.PacketConn, addr *net.UDPAddr, message *miniraft.Raft) (err error) {
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

func GetLastLogIndex() uint64{
	if len(logs) < 1 {
		return 0
	}
	return logs[len(logs) - 1].Term
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