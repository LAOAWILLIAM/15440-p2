package raft

import "fmt"

func (rf *Raft)Print_info(){
	rf.mux.Lock()
	defer rf.mux.Unlock()
	if rf.state==Leader{
		fmt.Println("Leader",rf.me,"Term",rf.currentTerm)
	}
	if rf.state==Candidates{
		fmt.Println("Candidate",rf.me,"Term",rf.currentTerm)
	}
	if rf.state==Followers{
		fmt.Println("Follower",rf.me,"Term",rf.currentTerm)
	}

	fmt.Println("CommitIndex",rf.commitIndex,"LastAppiled",rf.lastApplied)
	//fmt.Println("")
}