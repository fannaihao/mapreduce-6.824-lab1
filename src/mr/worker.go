package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "encoding/json"
import "time"

var worker_id int64
const WORKER_WAIT_TIME int64 = 500 //millisecond

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	register(&worker_id)
	fmt.Printf("Got an id: %v form master\n", worker_id)

	for {
		var task_info RequestTaskReply
		if ret := requestTask(&task_info); !ret {
			log.Fatal("Request task from master failed, so just quit...")
		}
		//log.Printf("Request a task from master, task type is: %v, input file name is : %v\n",
		//	task_info.Task_type, task_info.Input_file_name)

		switch task_info.Task_type {
		case MAP_TASK:
			log.Printf("Get a map task from master, input file name is: %v", task_info.Input_file_name)
			if ret := do_map(mapf, task_info); !ret {
				log.Fatalf("Map task: %v fail.", task_info.Task_id)
			}
			reportMapTaskDone(task_info)
			log.Printf("Map task: %v has finished.", task_info.Task_id)
		case REDUCE_TASK:
			log.Printf(`Get a reduce task from master, 
			task id: %d, corresponding map task ids are: %v`, task_info.Task_id, task_info.Map_task_ids)
			if ret := do_reduce(reducef, task_info); !ret {
				log.Fatalf("Reduce Task: %v fail", task_info.Task_id)
			}
			reportReduceTaskDone(task_info)
			log.Printf("Reduce Task: %v has finished.", task_info.Task_id)
		case WAIT_TASK:
			//log.Printf("Receive master's waiting signal, so start to wait %d ms...", WORKER_WAIT_TIME)
			time.Sleep(time.Millisecond*time.Duration(WORKER_WAIT_TIME))
		case QUIT_TASK:
			log.Printf("Receive master's quit signal, quiting...")
			break
		default:
			log.Fatalf("Unknown job type: %v", task_info.Task_type)
	    }
    }
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//Register the worker itself in Master to get an id.
func register(worker_id *int64) bool {
	args := RegisterArgs{}
	reply := RegisterReply{}

	ret := call("Master.Register", &args, &reply)

	*worker_id = reply.Worker_id
	return ret
}

//request a task from master
func requestTask(task_info *RequestTaskReply) bool {
	args := RequestTaskArgs{
		Worker_id: worker_id,
	}
	reply := RequestTaskReply{}

	ret := call("Master.RequestTask", &args, &reply)
	*task_info = reply
	return ret
}

func reportMapTaskDone(task_info RequestTaskReply) bool {
	args := ReportMapTaskDoneArgs{
		Worker_id: worker_id,
		Input_file_name: task_info.Input_file_name,
		Task_id: task_info.Task_id,	
	}
	reply := ReportMapTaskDoneReply{}

	ret := call("Master.ReportMapTaskDone", &args, &reply)
	return ret
}

func reportReduceTaskDone(task_info RequestTaskReply) bool {
	args := ReportReduceTaskDoneArgs {
		Worker_id: worker_id,
		Task_id: task_info.Task_id,
	}
	reply := ReportReduceTaskDoneReply{}

	ret := call("Master.ReportReduceTaskDone", &args, &reply)
	return ret
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//hash intermediate in nReduce files
func hash_intermediate(intermediate []KeyValue, nReduce int64, task_id int64) bool {
	intermediate_files := make(map[int64][]KeyValue)

	for _, v := range intermediate {
		tmp_slice := intermediate_files[int64(ihash(v.Key))%nReduce]
		intermediate_files[int64(ihash(v.Key))%nReduce] = append(tmp_slice, v)
	}

	for i, slice := range intermediate_files {
		oname := fmt.Sprintf("mr-%d-%d", task_id, i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range slice {
			if err := enc.Encode(&kv); err != nil {
				log.Fatalf("Error occurs when encode: %v", err)
			}
		}
		ofile.Close()
	}

	return true
}

//handle map task
func do_map(mapf func(string, string) []KeyValue,
	task_info RequestTaskReply) bool {
	//open file and do map
	intermediate := []KeyValue{}
	file, err := os.Open(task_info.Input_file_name)
	if err != nil {
		log.Fatalf("cannot open %v", task_info.Input_file_name)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task_info.Input_file_name)
	}
	file.Close()
	kva := mapf(task_info.Input_file_name, string(content))
	intermediate = append(intermediate, kva...)

	//hash intermediate in nReduce files.
	hash_intermediate(intermediate, task_info.Reduce_num, task_info.Task_id)

	return true
}

//handle reduce task
func do_reduce(reducef func(string, []string) string,
    task_info RequestTaskReply) bool {
	//load KeyValues
	kv_map := make(map[string][]string)
	for _, map_task_id := range task_info.Map_task_ids {
		intermediate_file_name := fmt.Sprintf("mr-%d-%d", map_task_id, task_info.Task_id)
		file, err := os.Open(intermediate_file_name)
		if err != nil {
			log.Fatalf("cannot open %v", intermediate_file_name)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				//log.Println(err)
				break
			}
			kv_map[kv.Key] = append(kv_map[kv.Key], kv.Value)
		}
		file.Close()
	}

    out_file_name := fmt.Sprintf("mr-out-%d", task_info.Task_id)
	tmp_out_file, err := ioutil.TempFile("", out_file_name)
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(tmp_out_file.Name())
	
	for key, values := range kv_map {
		output := reducef(key, values)
        // this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmp_out_file, "%v %v\n", key, output)
	}
    
	if err := os.Rename(tmp_out_file.Name(), out_file_name); err != nil {
		log.Fatalf("Rename file: %v fail, err: %v.", out_file_name, err)
	}
    tmp_out_file.Close()

	return true
}
