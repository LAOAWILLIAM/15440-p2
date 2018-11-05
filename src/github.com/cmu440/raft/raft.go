//
// raft.go
// =======
// Write your code in this file
// We will use the original version of all other
// files for testing
//

package raft

//
// API
// ===
// This is an outline of the API that your raft implementation should
// expose.
//
// rf = Make(...)
//   Create a new Raft peer.
//
// rf.Start(command interface{}) (index, term, isleader)
//   Start agreement on a new log entry
//
// rf.GetState() (me, term, isLeader)
//   Ask a Raft peer for "me" (see line 58), its current term, and whether it thinks it
//   is a leader
//
// ApplyMsg
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (e.g. tester) on the
//   same peer, via the applyCh channel passed to Make()
//

import (
	"math/rand"
	"sync"
	"time"
)
import "github.com/cmu440/rpc"

//
// ApplyMsg
// ========
//
// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same peer, via the applyCh passed to Make()
//
type ApplyMsg struct {
	Index   int
	Command interface{}
}

//
// Raft struct
// ===========
//
// A Go object implementing a single Raft peer
//
type Raft struct {
	mux   sync.Mutex       // Lock to protect shared access to this peer's state
	peers []*rpc.ClientEnd // RPC end points of all peers
	me    int              // this peer's index into peers[]
	state int
	// Persistent States
	log         map[int]*LogEntries
	currentTerm int
	votedFor    int
	// Volatile States
	commitIndex int
	lastApplied int
	// Leader States
	nextIndex  []int
	matchIndex []int
	//Channels
	timeoutchan   chan bool
	votereplychan chan *RequestVoteReply
	heatbeat      chan *AppendEntriesArgs
	// Your data here (2A, 2B).
	// Look at the Raft paper's Figure 2 for a description of what
	// state a Raft peer should maintain
}

const (
	Followers = iota + 1
	Leader
	Candidates
)

type LogEntries struct {
	term  int
	index int
}

//
// GetState()
// ==========
//
// Return "me", current term and whether this peer
// believes it is the leader
//
func (rf *Raft) GetState() (int, int, bool) {
	var me int
	var term int
	var isleader bool
	rf.mux.Lock()
	me = rf.me
	term = rf.currentTerm
	isleader = false

	if rf.state == Leader {
		isleader = true
	}
	rf.mux.Unlock()
	// Your code here (2A)
	return me, term, isleader
}

//
// RequestVoteArgs
// ===============
//
// Example RequestVote RPC arguments structure
//
// Please note
// ===========
// Field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B)
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// RequestVoteReply
// ================
//
// Example RequestVote RPC reply structure.
//
// Please note
// ===========
// Field names must start with capital letters!
//
//
type RequestVoteReply struct {
	// Your data here (2A)
	Term        int
	VoteGranted bool
}

//
// RequestVote
// ===========
//
// Example RequestVote RPC handler
//

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B)
	//fmt.Println("11111")
	rf.mux.Lock()
	defer rf.mux.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
			if rf.currentTerm == args.Term {
				//fmt.Println("000")
				reply.VoteGranted = true
				rf.votedFor = args.CandidateID
				reply.Term = args.Term
				return
			} else { //args.Term > rf.currentTerm
				//fmt.Println("111")
				rf.currentTerm = args.Term
				reply.VoteGranted = true
				rf.votedFor = args.CandidateID
				reply.Term = args.Term
				return

			}
		} else { // rf.votedFor!=-1&&rf.votedFor!=args.CandidateID
			if rf.currentTerm == args.Term {
				//fmt.Println("222")
				reply.VoteGranted = false
				reply.Term = args.Term
				return

			} else { //args.Term > rf.currentTerm
				//fmt.Println("333")
				rf.currentTerm = args.Term
				reply.VoteGranted = true
				rf.votedFor = args.CandidateID
				reply.Term = args.Term
				return

			}
		}

	}

}

//
// sendRequestVote
// ===============
//
// Example code to send a RequestVote RPC to a peer
//
// peer int -- index of the target peer in
// rf.peers[]
//
// args *RequestVoteArgs -- RPC arguments in args
//
// reply *RequestVoteReply -- RPC reply
//
// The types of args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers)
//
// The rpc package simulates a lossy network, in which peers
// may be unreachable, and in which requests and replies may be lost
//
// Call() sends a request and waits for a reply
//
// If a reply arrives within a timeout interval, Call() returns true;
// otherwise Call() returns false
//
// Thus Call() may not return for a while
//
// A false return can be caused by a dead peer, a live peer that
// can't be reached, a lost request, or a lost reply
//
// Call() is guaranteed to return (perhaps after a delay)
// *except* if the handler function on the peer side does not return
//
// Thus there
// is no need to implement your own timeouts around Call()
//
// Please look at the comments and documentation in ../rpc/rpc.go
// for more details
//
// If you are having trouble getting RPC to work, check that you have
// capitalized all field names in the struct passed over RPC, and
// that the caller passes the address of the reply struct with "&",
// not the struct itself
//
func (rf *Raft) sendRequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[peer].Call("Raft.RequestVote", args, reply)
	rf.votereplychan <- reply
	return ok
}

type AppendEntriesArgs struct {
	// Your data here (2A)
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entry        string
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Your data here (2A)
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//fmt.Println("AppendEntries")
	rf.mux.Lock()
	defer rf.mux.Unlock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
	}
	if args.Entry == "" {
		reply.Term = rf.currentTerm
		reply.Success = true
		rf.heatbeat <- args
	}
}

func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//fmt.Println("SendingAppendRPC")
	ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
	//rf.heatbeat <- args
	return ok
}

//
// Start
// =====
//
// The service using Raft (e.g. a k/v peer) wants to start
// agreement on the next command to be appended to Raft's log
//
// If this peer is not the leader, return false
//
// Otherwise start the agreement and return immediately
//
// There is no guarantee that this command will ever be committed to
// the Raft log, since the leader may fail or lose an election
//
// The first return value is the index that the command will appear at
// if it is ever committed
//
// The second return value is the current term
//
// The third return value is true if this peer believes it is
// the leader
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B)

	return index, term, isLeader
}

//
// Kill
// ====
//
// The tester calls Kill() when a Raft instance will not
// be needed again
//
// You are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance
//
func (rf *Raft) Kill() {
	// Your code here, if desired
}

//
// Make
// ====
//
// The service or tester wants to create a Raft peer
//
// The port numbers of all the Raft peers (including this one)
// are in peers[]
//
// This peer's port is peers[me]
//
// All the peers' peers[] arrays have the same order
//
// applyCh
// =======
//
// applyCh is a channel on which the tester or service expects
// Raft to send ApplyMsg messages
//
// Make() must return quickly, so it should start Goroutines
// for any long-running work
//
func Make(peers []*rpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me
	// Your initialization code here (2A, 2B)
	rf.state = Followers
	rf.log = make(map[int]*LogEntries)
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.heatbeat = make(chan *AppendEntriesArgs)
	rf.timeoutchan = make(chan bool)
	rf.votereplychan = make(chan *RequestVoteReply)
	go rf.MainRoutine()
	return rf
}

func (rf *Raft) MainRoutine() {
	for {
		switch rf.state {
		case Followers:
			//fmt.Println(rf.me,"Followers")
			quit := false
			FollowerTimer := time.NewTimer(500 * time.Duration(time.Millisecond))
			for {
				select {
				case heartbeat := <-rf.heatbeat:
					rf.mux.Lock()
					if heartbeat.Term > rf.currentTerm {
						rf.currentTerm = heartbeat.Term
					}
					rf.mux.Unlock()
					FollowerTimer.Reset(500 * time.Duration(time.Millisecond))
				case <-FollowerTimer.C:
					rf.mux.Lock()
					rf.state = Candidates
					rf.mux.Unlock()
					quit = true
					break
				}
				if quit == true {
					break
				}
			}

		case Leader:
			//fmt.Println(rf.me,"Leader")
			//LeaderTimer:=time.NewTimer(500*time.Duration(time.Millisecond))
			heartbeat := &AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
				Entry:    "",
			}
			//rf.mux.Lock()
			for index := 0; index < len(rf.peers); index++ {
				if index != rf.me {
					//fmt.Println("Sending heartbeat1111")
					var reply AppendEntriesReply
					//fmt.Println("Leader",rf.me,"Sending heartbeat to",index)
					go rf.sendAppendEntries(index, heartbeat, &reply)
				}
			}
			//rf.mux.Unlock()
			time.Sleep(100 * time.Millisecond)

		case Candidates:

			rf.mux.Lock()
			//fmt.Println(rf.me,"Candidate")

			rf.currentTerm++ //Increment Current Term
			numpeers := len(rf.peers)
			self := rf.me
			rf.votedFor = rf.me
			rf.mux.Unlock()

			vote := 1 //Vote for Self!

			RequestVoteArgs := &RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateID: rf.me,
			}
			//Reset Timer
			var tau int
			tau = rand.Intn(200) + 400
			CandidatesTimer := time.NewTimer(time.Duration(tau) * time.Millisecond)
			quit := false

			for index := 0; index < numpeers; index++ {
				if index != self {
					var RequestVoteReply RequestVoteReply
					go rf.sendRequestVote(index, RequestVoteArgs, &RequestVoteReply)
				}
			}

			for {
				select {
				case heartbeat := <-rf.heatbeat:
					//fmt.Println("Candidate",rf.me,"receives heartbeat")
					rf.mux.Lock()
					rf.currentTerm = heartbeat.Term
					rf.state = Followers
					quit = true
					rf.mux.Unlock()
					break

				case RequestVoteReply := <-rf.votereplychan:
					rf.mux.Lock()
					currentTerm := rf.currentTerm
					rf.mux.Unlock()
					//fmt.Println("Candidate",rf.me,"receives vote")
					if RequestVoteReply.Term > currentTerm {
						rf.mux.Lock()
						rf.state = Followers
						rf.currentTerm = RequestVoteReply.Term
						rf.mux.Unlock()
						quit = true
						break
					} else {
						if RequestVoteReply.VoteGranted {
							vote++
						}
						//fmt.Println("Candidate",rf.me,"has vote",vote)
						if vote > len(rf.peers)/2 {
							rf.mux.Lock()
							rf.state = Leader
							rf.mux.Unlock()
							quit = true
							break
						}
					}
				case <-CandidatesTimer.C:
					//Timeout
					//vote=1
					quit = true
					break
				}
				if quit == true {
					break
				}
			}
		}

	}
}
