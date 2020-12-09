package raft_test

import (
    "raft"
    "testing"
    "fmt"
    "log"
    "bytes"
    "os"
    "sync"
    "github.com/stretchr/testify/assert"
)

func captureOutput(f func()) string {
    var buf bytes.Buffer
    log.SetOutput(&buf)
    f()
    log.SetOutput(os.Stderr)
    return buf.String()
}

func TestRaft(t *testing.T) {
    fmt.Println("starting test")
    log.SetFlags(0)

    output := captureOutput(func() {
        network := raft.MakeNetwork(5, 100)
        network.Send(raft.Msg{0, 0, "xyz", "abc"})

        var wg sync.WaitGroup
        wg.Add(1)
        go func() {
            defer wg.Done()
            network.GetNode(0).RxLoop()
        }()

        network.Close()
        wg.Wait()
    })
    assert.Equal(t, "processing xyz\n", output)
}
