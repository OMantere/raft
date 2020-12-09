package raft

import (
    "fmt"
    "log"
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
    Key string
    Value string
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

func (n *Node) Send(id int, msg Msg) {
    msg.Sender = n.id
    msg.Receiver = id
    n.tx <- msg
}

func (n *Node) RxLoop() {
    for msg := range n.rx {
        if n.id != msg.Receiver {
            panic(fmt.Sprintf("%d is not the receiver of %d", n.id, msg.Receiver))
        }
        n.process(msg)
    }
}

func (n *Node) TxLoop() {
    for msg := range n.tx {
        n.network.Send(msg)
    }
}


// Protocol

func (n *Node) process(msg Msg) {
    log.Print(fmt.Sprintf("processing %s", msg.Key))
}
