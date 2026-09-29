package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

func WorkerIntermediate(MapID int, nReduce int, kva []KeyValue) {
	files := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)

	for ReduceID := 0; ReduceID < nReduce; ReduceID++ {
		filename := fmt.Sprintf("mr-%d-%d", MapID, ReduceID)

		file, err := os.Create(filename)
		if err != nil {
			log.Fatalf("cannot create: %v\n", filename)
		}

		files[ReduceID] = file
		encoders[ReduceID] = json.NewEncoder(file)
	}

	for _, kv := range kva {
		ReduceID := ihash(kv.Key) % nReduce

		err := encoders[ReduceID].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode kv: %v\n", err)
		}
	}

	for _, file := range files {
		err := file.Close()
		if err != nil {
			log.Fatalf("cannot close %v\n", err)
		}
	}
}

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	for {
		request := TaskRequest{}
		reply := TaskReply{}
		ok := call("Coordinator.GetTask", &request, &reply)
		if ok {
			if reply.TaskType == "Map" {
				filename := reply.MapTaskFileName
				content, err := os.ReadFile(filename)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				kva := mapf(filename, string(content))

				WorkerIntermediate(reply.MapID, reply.NReduce, kva)

				args := TaskDoneArgs{"Map", reply.MapID}
				doneReply := TaskDoneReply{}
				if !call("Coordinator.TaskDone", &args, &doneReply) {
					continue
				}

			} else if reply.TaskType == "Reduce" {
				ReduceID := reply.ReduceID

				intermediate := []KeyValue{}
				for MapID := 0; MapID < reply.MaxMapTaskID; MapID++ {
					filename := fmt.Sprintf("mr-%d-%d", MapID, ReduceID)
					file, err := os.Open(filename)
					if err != nil {
						log.Fatalf("cannot Open %v\n", err)
					}

					dec := json.NewDecoder(file)

					for {
						var kv KeyValue
						err := dec.Decode(&kv)
						if err == io.EOF {
							break
						}
						if err != nil {
							log.Fatalf("cannot json decode %v\n", err)
						}

						intermediate = append(intermediate, kv)
					}
					if err := file.Close(); err != nil {
						log.Fatalf("cannot close %v: %v", filename, err)
					}
				}

				sort.Slice(intermediate, func(i, j int) bool {
					return intermediate[i].Key < intermediate[j].Key
				})

				filename := fmt.Sprintf("mr-out-%d", ReduceID)
				file, err := os.Create(filename)
				if err != nil {
					log.Fatalf("cannot Create %v\n", err)
				}

				i := 0
				for i < len(intermediate) {
					j := i + 1
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					output := reducef(intermediate[i].Key, values)

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)

					i = j
				}

				file.Close()

				args := TaskDoneArgs{"Reduce", ReduceID}
				doneReply := TaskDoneReply{}
				if !call("Coordinator.TaskDone", &args, &doneReply) {
					continue
				}

			} else if reply.TaskType == "Wait" {
				time.Sleep(time.Second)
			} else if reply.TaskType == "Exit" {
				return
			} else {
				log.Fatalf("Illegal Task Type: %q\n", reply.TaskType)
			}
		} else {
			return
		}
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Printf("dialing: %v", err)
		return false
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
