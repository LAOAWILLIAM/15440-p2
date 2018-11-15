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
	"fmt"
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
	log          []LogEntries
	currentTerm  int
	currentIndex int
	//Candidate:
	votedFor int
	// Volatile States
	commitIndex int
	lastApplied int
	// Leader States
	nextIndex  []int
	matchIndex []int
	//Channels
	appendReplychan      chan *AppendEntriesReply
	timeoutchan          chan bool
	votereplychan        chan *RequestVoteReply
	heatbeat             chan *AppendEntriesArgs
	FollowerReceiveReply chan int
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
	Term  int
	Index int
	Entry interface{}
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
	defer rf.mux.Unlock()

	me = rf.me
	term = rf.currentTerm
	isleader = false

	if rf.state == Leader {
		isleader = true
	}

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
	//fmt.Println("Server",rf.me,"with Term:",rf.currentTerm,"receives from",args.CandidateID,"with Term:",args.Term)
	rf.mux.Lock()
	defer rf.mux.Unlock()
	if args.Term < rf.currentTerm {
		//fmt.Println(1)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else {
		uptodate := true
		if args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex < len(rf.log)-1 {
			//fmt.Println(4)
			uptodate = false
		}
		if args.LastLogTerm < rf.log[len(rf.log)-1].Term {
			//fmt.Println(5)
			uptodate = false
		}

		if args.Term == rf.currentTerm {
			if uptodate && rf.state == Followers {
				if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
					//fmt.Println("Server",rf.me,"with Term:",rf.currentTerm,"B_VOTE",args.CandidateID)
					reply.VoteGranted = true
					rf.votedFor = args.CandidateID

					rf.FollowerReceiveReply <- 1
				}
			}
			return
		}

		if args.Term > rf.currentTerm {
			//fmt.Println("Server",rf.me,"Changed into Followers")
			rf.state = Followers
			rf.currentTerm = args.Term

			if !uptodate {
				reply.VoteGranted = false
				rf.votedFor = -1
				//fmt.Println("Server",rf.me,"with Term:",rf.currentTerm,"A_VOTE",args.CandidateID)
			} else {
				reply.VoteGranted = true
				rf.votedFor = args.CandidateID

			}
			rf.FollowerReceiveReply <- 1
			return
		}
		return
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
	//fmt.Println("Replying")
	rf.votereplychan <- reply
	return ok
}

type AppendEntriesArgs struct {
	// Your data here (2A)
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entry        []LogEntries
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Your data here (2A)
	Term      int
	Success   bool
	nextIndex int
}

func remove(slice []LogEntries, s int) []LogEntries {

	return append(slice[:s], slice[s+1:]...)
}

func Min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func (rf *Raft) findFirstIndex(initialindex int, term int) int {
	for i := initialindex; i > 0; i-- {
		if rf.log[i].Term <= term {
			return i
		}
	}
	return 1
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mux.Lock()
	defer rf.mux.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.nextIndex = -1
		return
	}

	if args.Entry == nil {
		//fmt.Println(rf.me,"receives empty heartbeats")
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.heatbeat <- args
		reply.nextIndex = -1
		//fmt.Println(rf.me,"receives empty heartbeats reply done")
		return
	}

	//rf.FollowerReceiveReply<-1
	if args.Term >= rf.currentTerm {
		rf.state = Followers
		if len(rf.log) <= args.PrevLogIndex {
			//fmt.Println(rf.me,"Does not contain PrevLogIndex")
			//Does Not Conatin
			reply.Term = rf.currentTerm
			reply.Success = false
			reply.nextIndex = rf.findFirstIndex(len(rf.log)-1, args.PrevLogTerm)
			//fmt.Println(reply.nextIndex)
			//return
		}
		if len(rf.log) > args.PrevLogIndex {
			//Contain!
			if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
				//fmt.Println(rf.me,"with log:",rf.log,"Miss Match PrevLogIndex")
				reply.Success = false
				reply.Term = rf.currentTerm
				reply.nextIndex = rf.findFirstIndex(len(rf.log)-1, args.PrevLogTerm)
				fmt.Println(reply.nextIndex)
				//delete
				rf.log = rf.log[:args.PrevLogIndex]
			} else { //args.PrevLogTerm==rf.log[args.PrevLogIndex].term
				//Found the point where the two logs agree
				//fmt.Println(rf.me,"with log:",rf.log,"Found the Sync point ")
				reply.Term = rf.currentTerm
				reply.Success = true
				reply.nextIndex = -1
				//Append Everything!
				rf.log = append(rf.log[:args.PrevLogIndex], args.Entry[args.PrevLogIndex:]...)
			}
		}
		//fmt.Println(rf.me,"now has log:",rf.log,"with leaderCommit",args.LeaderCommit,"commitIndex:",rf.commitIndex)
		//If leaderCommit > commitIndex, set commitIndex
		if reply.Success == true && args.LeaderCommit > rf.commitIndex {
			//index of last new entry
			index := len(rf.log) - 1
			//fmt.Println(rf.state,rf.me,"Changed commitIndex to",rf.commitIndex)
			//rf.mux.Lock()
			rf.state = Followers
			//rf.FollowerReceiveReply<-1
			//fmt.Println("LeadrCommit:",args.LeaderCommit,"index",index)
			rf.commitIndex = Min(args.LeaderCommit, index)
			//rf.mux.Unlock()
		}
		rf.FollowerReceiveReply <- 1
	}
	return

}

func (rf *Raft) sendAppendEntries(applyCh chan ApplyMsg, peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//fmt.Println("SendingAppendRPC")

	ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)

	if reply.Success == false { //If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
		if reply.Term <= args.Term {
			rf.mux.Lock()
			//if rf.nextIndex[peer] > 0 {
			//	if reply.nextIndex!=-1{
			if reply.nextIndex != -1 {
				rf.nextIndex[peer] = reply.nextIndex
			}
			//}else{
			//	rf.nextIndex[peer]--
			//}
			//
			//}
			rf.mux.Unlock()
		}
	} else { //reply.Success==true
		//If successful: update nextIndex and matchIndex for follower (§5.3)
		if args.Entry != nil {
			rf.mux.Lock()
			//fmt.Println(rf.state,rf.me,"Changed Next&Match")
			rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entry[args.PrevLogIndex:])
			rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entry[args.PrevLogIndex:]) - 1
			//fmt.Println(rf.state,rf.me,"Changed nextIndex of",peer,"to",rf.nextIndex[peer])
			rf.mux.Unlock()
		}

	}

	rf.mux.Lock()
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		info := rf.log[rf.lastApplied]
		msg := ApplyMsg{
			Index:   rf.lastApplied,
			Command: info.Entry,
		}
		//fmt.Println("Candidate",rf.me,"sending Applymsg:",rf.lastApplied)
		applyCh <- msg

	}
	rf.mux.Unlock()
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
	rf.mux.Lock()
	defer rf.mux.Unlock()
	index := -1
	term := -1
	isLeader := false

	if rf.state != Leader {
		return index, term, false
	}
	//fmt.Println("<<<<<<<<Client CAll Command:",command,"to ",rf.me,">>>>>>>>>>>>>>>>>>")
	newLog := LogEntries{
		Term:  rf.currentTerm,
		Index: len(rf.log),
		Entry: command,
	}
	rf.currentIndex++
	rf.log = append(rf.log, newLog)
	//fmt.Println(rf.me,"now Has log:",rf.log)
	index = newLog.Index
	term = newLog.Term
	isLeader = true
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

func (rf *Raft) waitForReply(applyCh chan ApplyMsg, index int, args *AppendEntriesArgs, reply AppendEntriesReply) {
	rf.mux.Lock()
	//fmt.Println(rf.me,"a")
	go rf.sendAppendEntries(applyCh, index, args, &reply)
	//fmt.Println(rf.me,"b")
	rf.mux.Unlock()

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
	rf.log = append(rf.log, LogEntries{Index: 0, Term: 0, Entry: nil})
	rf.currentIndex = 1
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.appendReplychan = make(chan *AppendEntriesReply)
	rf.heatbeat = make(chan *AppendEntriesArgs, 5)
	rf.timeoutchan = make(chan bool)
	rf.votereplychan = make(chan *RequestVoteReply)
	rf.FollowerReceiveReply = make(chan int, 1)
	go rf.MainRoutine(applyCh)
	return rf
}

func (rf *Raft) MainRoutine(applyCh chan ApplyMsg) {
	for {
		switch rf.state {
		case Followers:
			//fmt.Println(rf.me,"is a Followers now")
			rf.mux.Lock()
			rf.votedFor = -1
			rf.mux.Unlock()
			quit := false

			for {
				rf.mux.Lock()
				if rf.commitIndex > rf.lastApplied {
					rf.lastApplied++
					info := rf.log[rf.lastApplied]
					msg := ApplyMsg{
						Index:   rf.lastApplied,
						Command: info.Entry,
					}
					//fmt.Println("Followers",rf.me,"sending Applymsg:",rf.lastApplied,"Command:",msg.Command)
					applyCh <- msg
				}
				rf.mux.Unlock()
				tau := rand.Intn(300) + 500
				FollowerTimer := time.NewTimer(time.Duration(tau) * time.Millisecond)
				select {
				case heartbeat := <-rf.heatbeat:
					//fmt.Println("Followers",rf.me,"Term:",rf.currentTerm,"Receives Heartbeat")
					rf.mux.Lock()
					if heartbeat.Term > rf.currentTerm {
						//fmt.Println("Changed")
						rf.currentTerm = heartbeat.Term
					}
					rf.state = Followers
					rf.mux.Unlock()
					tau = rand.Intn(300) + 400
					FollowerTimer = time.NewTimer(time.Duration(tau) * time.Millisecond)
				case <-rf.FollowerReceiveReply:
					//fmt.Println("Follower",rf.me,"Continue life")
					tau = rand.Intn(300) + 400
					FollowerTimer = time.NewTimer(time.Duration(tau) * time.Millisecond)

				case <-FollowerTimer.C:
					rf.mux.Lock()
					//fmt.Println("---------Followers",rf.me,"Term:",rf.currentTerm,"Become Candidate----------")
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
			for {
				quit := false
				select {
				case heartbeat := <-rf.heatbeat:
					//fmt.Println("Leader",rf.me,"becoming followers FIRST")
					rf.mux.Lock()
					if heartbeat.Term > rf.currentTerm {
						//fmt.Println("Leader",rf.me,"becoming followers IN")
						rf.state = Followers
						rf.currentTerm = heartbeat.Term
						quit = true
					}
					rf.mux.Unlock()
					if quit {
						//fmt.Println("1")
						break
					}
				case AppendEntriesReply := <-rf.appendReplychan:
					//fmt.Println(rf.me,":quit0")
					//fmt.Println(rf.me,"receives 2")
					rf.mux.Lock()
					currentTerm := rf.currentTerm
					rf.mux.Unlock()
					if AppendEntriesReply.Term > currentTerm {
						//fmt.Println(rf.me,":quit1")
						rf.mux.Lock()
						rf.state = Followers
						rf.currentTerm = AppendEntriesReply.Term
						rf.mux.Unlock()
						quit = true
						break
					}


				default:
					rf.mux.Lock()
					state := rf.state
					currentTerm := rf.currentTerm
					leadID := rf.me
					length := len(rf.peers)
					rf.mux.Unlock()
					if state == Followers {
						quit = true
						break
					}

					for index := 0; index < length; index++ {

						if index != rf.me {
							heartbeat := &AppendEntriesArgs{
								Term:         currentTerm,
								LeaderId:     leadID,
								PrevLogTerm:  0,
								PrevLogIndex: 0,
								LeaderCommit: 0,
								Entry:        nil,
							}
							var reply AppendEntriesReply
							//fmt.Println(len(rf.log)-1)
							rf.mux.Lock()

							if rf.nextIndex[index] <= len(rf.log)-1 && len(rf.log)-1 != 0 {

								heartbeat.LeaderCommit = rf.commitIndex
								heartbeat.PrevLogIndex = rf.nextIndex[index]
								heartbeat.PrevLogTerm = rf.log[heartbeat.PrevLogIndex].Term
								heartbeat.Entry = rf.log
							}

							go rf.waitForReply(applyCh, index, heartbeat, reply)
							rf.mux.Unlock()
						}
					}

					rf.mux.Lock()
					for N := rf.commitIndex + 1; N < len(rf.log); N++ {
						count := 0
						for index := 0; index < len(rf.peers); index++ {
							if index != rf.me && rf.matchIndex[index] >= N && rf.log[N].Term == rf.currentTerm {
								count++
							}
						}
						//fmt.Println("count:",count)
						if count >= (len(rf.peers)-1)/2 {
							//fmt.Println("leader changed CommitedIndex to",N)
							rf.commitIndex = N
						}
					}
					//fmt.Println(rf.me,"2")
					rf.mux.Unlock()
					rf.mux.Lock()
					if rf.commitIndex > rf.lastApplied {
						rf.lastApplied++
						info := rf.log[rf.lastApplied]
						msg := ApplyMsg{
							Index:   rf.lastApplied,
							Command: info.Entry,
						}
						//fmt.Println("leader",rf.me,"sending Applymsg:",rf.lastApplied)
						applyCh <- msg

					}
					//fmt.Println(rf.me,"3")
					rf.mux.Unlock()
					//for i:=rf.lastApplied+1;i<=rf.commitIndex;i++ {
					//	msg := ApplyMsg{
					//		Index:   i,
					//		Command: rf.log[i].Entry,
					//	}
					//	fmt.Println("Leader",rf.me,"Sending ApplyMSG with Index:",i)
					//	applyCh<-msg
					//}
					time.Sleep(80 * time.Millisecond)
					//fmt.Println(rf.me,"4")
				}

				if quit {
					//fmt.Println("Leader",rf.me,"becoming followers")
					//fmt.Println(rf.me,"5")
					break
				}
			}
			//fmt.Println(rf.me,"Leader")
			//LeaderTimer:=time.NewTimer(500*time.Duration(time.Millisecond))

		case Candidates:

			//Reset Timer

			var tau int
			tau = rand.Intn(300) + 500
			CandidatesTimer := time.NewTimer(time.Duration(tau) * time.Millisecond)
			quit := false
			election := false
			vote := 1
			for {
				//fmt.Println(222)

				select {
				case heartbeat := <-rf.heatbeat:
					//fmt.Println("Candidate",rf.me,"receives heartbeat")
					rf.mux.Lock()
					rf.currentTerm = heartbeat.Term
					rf.state = Followers
					quit = true
					rf.mux.Unlock()
					break

					//case <-rf.FollowerReceiveReply:
					//	fmt.Println("Candidate",rf.me,"becoming followers")
					//
					//	rf.mux.Lock()
					//	quit=true
					//	rf.state=Followers
					//	break
					//	rf.mux.Unlock()
				case RequestVoteReply := <-rf.votereplychan:
					//fmt.Println("Candidate",rf.me,"with Term:",rf.currentTerm,"receives Vote Reply")
					if election == true {
						rf.mux.Lock()
						currentTerm := rf.currentTerm
						rf.mux.Unlock()
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
							//fmt.Println("Candidate",rf.me,"with Term:",rf.currentTerm,"has vote",vote)
							if vote*2 > len(rf.peers) {
								//fmt.Println("-------------Candidate",rf.me,"Term:",rf.currentTerm,"Becoming Leader------------")
								rf.mux.Lock()
								rf.state = Leader
								rf.matchIndex = make([]int, len(rf.peers))
								rf.nextIndex = make([]int, len(rf.peers))
								for i := range rf.peers {
									rf.matchIndex[i] = 0
									rf.nextIndex[i] = len(rf.log)
								}

								rf.mux.Unlock()
								quit = true
								break
							}
						}
					}

				case <-CandidatesTimer.C:
					//Timeout
					//rf.mux.Lock()
					//
					//rf.mux.Unlock()

					//fmt.Println("Candidate",rf.me,"with Term:",rf.currentTerm,"timeout")
					vote = 1
					election = false
					tau = rand.Intn(300) + 500
					CandidatesTimer = time.NewTimer(time.Duration(tau) * time.Millisecond)
					//quit = true
					//break
				default:
					//fmt.Println("Candidate",rf.me,"Sending Vote Request")
					if election == false {
						rf.mux.Lock()
						state := rf.state
						//fmt.Println(rf.me,"Candidate")

						rf.currentTerm++ //Increment Current Term
						numpeers := len(rf.peers)
						self := rf.me
						rf.votedFor = rf.me

						//vote := 1 //Vote for Self!

						RequestVoteArgs := &RequestVoteArgs{
							Term:         rf.currentTerm,
							CandidateID:  rf.me,
							LastLogIndex: len(rf.log) - 1,
							LastLogTerm:  rf.log[len(rf.log)-1].Term,
						}
						rf.mux.Unlock()
						if state == Followers {
							//fmt.Println("Candidate",rf.me,"with Term:",rf.currentTerm,"Become Followers")
							quit = true
							break
						}
						for index := 0; index < numpeers; index++ {
							if index != self {
								var RequestVoteReply RequestVoteReply
								//fmt.Println("Candidate",rf.me,"with Term:",rf.currentTerm,"Sending Vote Request to",index)

								go rf.sendRequestVote(index, RequestVoteArgs, &RequestVoteReply)
							}
						}
						election = true
					}
				} //End of select

				if quit == true {
					//fmt.Println("2")
					break
				}
			}
		}

	}
}
