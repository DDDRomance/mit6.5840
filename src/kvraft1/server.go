package kvraft

import (
	"bytes"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type Entry struct {
	Version rpc.Tversion
	Value   string
}

type ReturnValue struct {
	err   rpc.Err
	entry Entry
}

type KVServer struct {
	me  int
	rsm *rsm.RSM

	// Your definitions here.
	mu   sync.Mutex
	data map[string]Entry
}

type Request struct {
	Type    string
	Key     string
	Value   string
	Version rpc.Tversion
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	request := req.(Request)
	var reply ReturnValue
	switch request.Type {
	case "Get":
		entry, exists := kv.data[request.Key]
		if !exists {
			reply.err = rpc.ErrNoKey
		} else {
			reply.err = rpc.OK
			reply.entry = entry
		}
	case "Put":
		entry, exists := kv.data[request.Key]
		if !exists {
			if request.Version == 0 {
				kv.data[request.Key] = Entry{Value: request.Value, Version: request.Version + 1}
				reply.err = rpc.OK
			} else {
				reply.err = rpc.ErrNoKey
			}
		} else {
			if request.Version == entry.Version {
				kv.data[request.Key] = Entry{Value: request.Value, Version: request.Version + 1}
				reply.err = rpc.OK
			} else {
				reply.err = rpc.ErrVersion
			}
		}
	}

	return reply
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	data := make(map[string]Entry, len(kv.data))
	for key, entry := range kv.data {
		data[key] = entry
	}
	kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(data)

	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var restored map[string]Entry
	if d.Decode(&restored) != nil {
		return
	}
	if restored == nil {
		restored = make(map[string]Entry)
	}

	kv.mu.Lock()
	kv.data = restored
	kv.mu.Unlock()
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, rtn := kv.rsm.Submit(Request{Key: args.Key, Type: "Get"})
	if err != rpc.OK {
		reply.Err = err
		return
	}
	value := rtn.(ReturnValue)
	reply.Version = value.entry.Version
	reply.Value = value.entry.Value
	reply.Err = value.err
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, rtn := kv.rsm.Submit(Request{Key: args.Key,
		Value:   args.Value,
		Version: args.Version,
		Type:    "Put"})
	if err != rpc.OK {
		reply.Err = err
		return
	}
	value := rtn.(ReturnValue)
	reply.Err = value.err
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(Request{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me, data: make(map[string]Entry)}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// You may need initialization code here.
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
