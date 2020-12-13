package raft

import (
    "fmt"
    "log"
    flatbuffers "github.com/google/flatbuffers/go"
    "schema"
)


// Primitives

type Network struct {
    N int
    broken []bool
    nodes []Node
}

type Node struct {
    id int
    network *Network
    rx chan Msg
    tx chan Msg
}

type Msg struct {
    Sender int
    Receiver int
    Payload interface{}
}

func MakeNode(id int, bufferSize int, network *Network) Node {
    return Node{
        id,
        network,
        make(chan Msg, bufferSize),
        make(chan Msg, bufferSize)}
}

func (n *Node) Close() {
    close(n.rx)
    close(n.tx)
}

func MakeNetwork(N int, bufferSize int) *Network {
    instance := new(Network)
    instance.N = N
    instance.broken = make([]bool, N*N)
    for i := 0; i < N; i++ {
        instance.nodes = append(instance.nodes, MakeNode(i, bufferSize, instance))
    }
    return instance
}

func (n *Network) Close() {
    for _, node := range n.nodes {
        node.Close()
    }
}

func (n *Network) Send(msg Msg) bool {
    if n.broken[n.N * msg.Sender + msg.Receiver] {
        return false
    }
    n.nodes[msg.Receiver].rx <- msg
    return true
}

func (n *Network) GetNode(id int) *Node {
    return &n.nodes[id]
}

func (n *Node) Send(id int, payload interface{}) {
    n.tx <- Msg{n.id, id, payload}
}

func (n *Node) TxLoop() {
    for msg := range n.tx {
        n.network.Send(msg)
    }
}

func (n *Node) RxLoop(fn callback) {
    for msg := range n.rx {
        if n.id != msg.Receiver {
            panic(fmt.Sprintf("%d is not the receiver of %d", n.id, msg.Receiver))
        }
        callback(msg)
    }
}


// Protocol

type LogRecord struct {
    term int
    key string
    value string
}

type State int
const (
    FOLLOWER State = iota
    CANDIDATE
    LEADER
)

type RaftNode struct {
    node Node
    state State
    builder *flatbuffers.Builder

    // Non-volatile state
    currentTerm int
    votedFor int
    log []LogRecord

    // Volatile state
    commitIndex int
    lastApplied int

    // Leader volatile state
    nextIndex []int
    matchIndex []int
}

type AppendEntries struct {
    term int
    prevLogIndex int
    prevLogTerm int
    entries []LogRecord
    leaderCommit int
}

type RequestVote struct {
    term int
    lastLogIndex int
    lastLogTerm int
}

type Result struct {
    term int
    success bool
}

func MakeRaftNode(node Node, int N) {
    // TODO: Make support initialization from log
    rn := RaftNode{
        node,
        FOLLOWER,
        flatbuffers.NewBuilder(1024), 0, -1}
    for i := 0; i < N; i++ {
        rn.nextIndex = append(rn.nextIndex, 1)
        rn.matchIndex = append(rn.matchIndex, 0)
    }
}

func(n *RaftNode) TxLoop(msg Msg) {
    n.node.TxLoop()
}

func(n *RaftNode) RxLoop(msg Msg) {
    n.node.RxLoop(n.Process)
}

func (n *RaftNode) Process(msg Msg) { 
    switch payload := msg.Payload.(type) {
        case AppendEntries:
            log.Print(fmt.Sprintf("processing AE (%d -> %d)", msg.Sender, msg.Receiver))
            response := n.HandleAppendEntries(payload)
        case RequestVote:
            log.Print(fmt.Sprintf("processing RV (%d -> %d)", msg.Sender, msg.Receiver))
            response := n.HandleRequestVote(payload)
        default:
            panic(fmt.Sprint("Unknown event type"))
    }
    n.node.Send(msg.Sender, response)
}

func (n *RaftNode) HandleAppendEntries(ae AppendEntries) Result {
    if n.state == LEADER {
        return Result{n.currentTerm, false}
    }
    if n.state == CANDIDATE {
        n.state = FOLLOWER
    }

    if ae.term < n.currentTerm {
        return Result{n.currentTerm, false}
    }
    if n.log[ae.prevLogIndex].term != ae.prevLogTerm {
        return Result{n.currentTerm, false}
    }

    insertIndex := ae.prevLogIndex + 1
    if insertIndex > len(n.log) {
        panic(fmt.Sprintf("Can't append entry at index %d to log of length %d", insertIndex))
    }
    for _, entry := range ae.entries {
        if insertIndex < len(n.log) {
            n.log[insertIndex] = entry
        } else {
            n.log = append(n.log, entry)
        }
        insertIndex++
    }

    if ae.leaderCommit > n.commitIndex {
        n.commitIndex = min(ae.leaderCommit, insertIndex)
    }
    return Result{n.currentTerm, true}
}

func (n *RaftNode) HandleRequestVote(rv RequestVote) {
    if rv.term < n.currentTerm {
        return Result{n.currentTerm, false}
    }
    if n.votedFor == -1 || n.votedFor == rv.candidateId {
        // TODO: Implement the rest of RequestVote
    }
}
