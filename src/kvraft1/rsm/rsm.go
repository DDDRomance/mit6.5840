package rsm

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Req any
	Id  uint64
	Me  int
}

type result struct {
	err   rpc.Err
	value any
}

type waiter struct {
	op   Op
	term int
	ch   chan result
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	pending      map[int]*waiter
	totalcommand uint64
	// Your definitions here.
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		pending:      make(map[int]*waiter),
	}
	if persister.SnapshotSize() > 0 {
		rsm.sm.Restore(persister.ReadSnapshot())
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.ReadMessage()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) ReadMessage() {
	for {
		message, ok := <-rsm.applyCh
		if !ok {
			rsm.mu.Lock()
			for index, w := range rsm.pending {
				delete(rsm.pending, index)
				w.ch <- result{err: rpc.ErrWrongLeader}
			}
			rsm.mu.Unlock()
			return
		}

		if message.SnapshotValid {
			rsm.sm.Restore(message.Snapshot)
			continue
		}
		if !message.CommandValid {
			continue
		}

		op, valid := message.Command.(Op)
		if !valid {
			rsm.mu.Lock()
			if w := rsm.pending[message.CommandIndex]; w != nil {
				delete(rsm.pending, message.CommandIndex)
				w.ch <- result{err: rpc.ErrWrongLeader}
			}
			rsm.mu.Unlock()
			continue
		}

		value := rsm.sm.DoOp(op.Req)
		rsm.maybeSnapshot(message.CommandIndex)

		rsm.mu.Lock()
		if w := rsm.pending[message.CommandIndex]; w != nil {
			delete(rsm.pending, message.CommandIndex)
			if w.op.Me == op.Me && w.op.Id == op.Id {
				w.ch <- result{err: rpc.OK, value: value}
			} else {
				w.ch <- result{err: rpc.ErrWrongLeader}
			}
		}
		rsm.mu.Unlock()
	}
}

func (rsm *RSM) maybeSnapshot(index int) {
	if rsm.maxraftstate < 0 ||
		rsm.rf.PersistBytes() < rsm.maxraftstate*8/9 {
		return
	}

	rsm.rf.Snapshot(index, rsm.sm.Snapshot())
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.
	rsm.mu.Lock()
	rsm.totalcommand++
	op := Op{
		Req: req,
		Id:  rsm.totalcommand,
		Me:  rsm.me,
	}
	index, term, isleader := rsm.rf.Start(op)
	if !isleader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}

	w := &waiter{
		op:   op,
		term: term,
		ch:   make(chan result, 1),
	}
	rsm.pending[index] = w
	rsm.mu.Unlock()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case res := <-w.ch:
			return res.err, res.value
		case <-ticker.C:
		}

		currentTerm, stillLeader := rsm.rf.GetState()
		if stillLeader && currentTerm == w.term {
			continue
		}

		// Prefer an apply result if it arrived at the same time as the
		// leadership change.
		select {
		case res := <-w.ch:
			return res.err, res.value
		default:
		}

		rsm.mu.Lock()
		if rsm.pending[index] == w {
			delete(rsm.pending, index)
			rsm.mu.Unlock()
			return rpc.ErrWrongLeader, nil
		}
		rsm.mu.Unlock()

		// The reader removed this waiter and queued its result.
		res := <-w.ch
		return res.err, res.value
	}
}
