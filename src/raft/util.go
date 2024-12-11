package raft

import "fmt"

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		fmt.Printf(format, a...)
	}
}
