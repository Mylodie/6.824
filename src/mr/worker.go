package mr

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

func randomID(len int) string {
	bytes := make([]byte, len)
	if _, err := rand.Read(bytes); err != nil {
		return ""
	}
	return hex.EncodeToString(bytes)
}

func pollCoordinator() (*PollingResponse, bool) {
	response := &PollingResponse{}
	ok := call("Coordinator.DoPoll", &PollingRequest{}, response)
	return response, ok
}

func doMapTask(mapF func(string, string) []KeyValue, response *PollingResponse) {
	log.Println("Begin map task", response.MapKey)
	file, err := os.Open(response.MapKey)
	defer file.Close()
	if err != nil {
		log.Printf("fail to open %v\n", response.MapKey)
		return
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Printf("fail to read %v\n", response.MapKey)
		return
	}

	kva := mapF(response.MapKey, string(content))

	ImFile := fmt.Sprintf("mr-im-%d.json", randomID(8))
	if f, err := os.Create(ImFile); err == nil {
		enc := json.NewEncoder(f)
		enc.Encode(&kva)
	} else {
		log.Printf("fail to create %v\n", ImFile)
		return
	}

	request := &ReportRequest{TaskType: MapType, MapKey: response.MapKey, ImFile: ImFile}
	call("Coordinator.DoReport", request, &ReportResponse{})

	log.Println("Finished map task", response.MapKey)
}

func doReduceTask(reduceF func(string, []string) string, response *PollingResponse) {
	mp := make(map[string][]string)

	//log.Println("reply.ImFiles:", reply.ImFiles, reflect.TypeOf(reply.ImFiles))

	//for i,imFile := range response.ImFiles{
	//	log.Println("ImFiles: ", i,imFile)
	//}

	for _, ImFile := range response.ImFiles {
		log.Println(ImFile)
		f, err := os.Open(ImFile)
		if err != nil {
			log.Println("fail to open", ImFile)
			continue
		}
		dec := json.NewDecoder(f)
		kva := []KeyValue{}
		if err := dec.Decode(&kva); err != nil {
			break
		}
		for _, kv := range kva {
			if ihash(kv.Key)%response.NReduce == response.ReduceKey {
				mp[kv.Key] = append(mp[kv.Key], kv.Value)
			}
		}

	}

	outFile := fmt.Sprintf("mr-out-%d.txt", response.ReduceKey)
	f, err := os.Create(outFile)
	defer f.Close()
	if err != nil {
		log.Printf("fail to create %v\n", f)
		return
	}
	for k, v := range mp {
		f.WriteString(fmt.Sprintf("%v %v\n", k, reduceF(k, v)))
	}

	request := &ReportRequest{TaskType: ReduceType, ReduceKey: response.ReduceKey}
	call("Coordinator.DoReport", request, &ReportResponse{})
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	os.MkdirAll("/var/tmp/6.824/mr", 0755)
	logfile, _ := os.Create(fmt.Sprintf("/var/tmp/6.824/mr/worker-%v.log", randomID(8)))
	log.SetOutput(logfile)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	defer logfile.Close()

	log.Println("Worker launched.")
	for {
		if response, ok := pollCoordinator(); ok {
			switch response.TaskType {
			case MapType:
				doMapTask(mapf, response)
			case ReduceType:
				doReduceTask(reducef, response)
			case IdleType:
				time.Sleep(1 * time.Second)
			default:
				panic("Got unexpected task type.")
			}
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	return false
}
