package mr

import "fmt"
import "time"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "sync/atomic"

var task_id int64

const TASK_DEAD_TIME int64 = 10 //second
const TASK_CHECK_BREAK int64 = 500 //millisecond

type JobStage int64
const (
	MAP_STAGE JobStage = iota
	REDUCE_STAGE
)

type TaskType int64
const (
	MAP_TASK TaskType = iota
	REDUCE_TASK 
	WAIT_TASK
	QUIT_TASK
)

type WorkerInfo struct {
	Id            int64
	Is_alive      bool
	Register_time time.Time
}

type TaskInfo struct {
	Task_type       TaskType //1 for map task, 2 for reduce task, other values is invaild
	Task_id         int64 //the id of the task, used in MxN bucket
	Is_running      bool  //If it is handled by a worked.
	Is_over         bool  //If the task is over.
	Worker_id       int64 //the worker handling the task, -1 for no worker
	Input_file_name string

	//only has meaning for running Tasks
	Start_time   time.Time
	End_time     time.Time
	Failed_times int32 //the times of changing worker due to overtime.
}

type MapJob struct {
	Tasks map[string]TaskInfo //key is input file name
}

type ReduceJob struct {
	Task_ids []int64 //task ids of map task, one file slice has a id.
	Tasks map[int64]TaskInfo //key is nReduce id
}

type Master struct {
	// Your definitions here.
	Is_done           bool
	Current_worker_id int64
	mu                sync.Mutex
	Worker_map        map[int64]WorkerInfo

	Job_stage  JobStage //current job stage: 1 for map stage, 2 for reduce stage
	Reduce_num int64 //reduce task number
	Map_job    MapJob
	Reduce_job ReduceJob
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) Register(args *RegisterArgs, reply *RegisterReply) error {
	reply.Worker_id = atomic.AddInt64(&m.Current_worker_id, 1)
	//add in worker map
	m.mu.Lock()
	m.Worker_map[reply.Worker_id] = WorkerInfo{
		Id:            reply.Worker_id,
		Is_alive:      true,
		Register_time: time.Now(),
	}
	m.mu.Unlock()
	fmt.Printf("Register a new worker in master, the worker Id is: %v\n", reply.Worker_id)

	return nil
}

func (m *Master) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	m.mu.Lock()
	switch m.Job_stage {
	case MAP_STAGE:
		//log.Printf("Start to assign a map task.")
		is_find_job := false
		for key, task := range m.Map_job.Tasks {
			if task.Is_running || task.Is_over {
				//log.Printf("Task: %d is not qualified to assign.", task.Task_id)
				continue
			}
			if task.Task_type != MAP_TASK {
				log.Panic("task.Task_type should be 1 (map type)!")
			}
			is_find_job = true
			//record the assgined job
			task.Is_running = true
			task.Worker_id = args.Worker_id
			task.Start_time = time.Now()
			m.Map_job.Tasks[key] = task
			//reply to worker
			reply.Task_type = MAP_TASK
			reply.Input_file_name = key
			reply.Reduce_num = m.Reduce_num
			reply.Task_id = m.Map_job.Tasks[key].Task_id
			break
		}
		if is_find_job {
			log.Printf("Has found a map job for worker-%d.", args.Worker_id)
			log.Printf("Map task assigned: key: %v, info: %+v", reply.Input_file_name, m.Map_job.Tasks[reply.Input_file_name])
		} else {
			//log.Printf("No map job to assign, so tell worker-%d to wait.", args.Worker_id)
			reply.Task_type = WAIT_TASK
		}
	case REDUCE_STAGE:
		//log.Printf("Start to assign a reduce task.")
		is_find_job := false
		for task_id, task_info := range m.Reduce_job.Tasks {
            if task_info.Is_running || task_info.Is_over {
				continue
			}
			is_find_job = true
			task_info.Is_running = true
			task_info.Worker_id = args.Worker_id
			task_info.Start_time = time.Now()
            m.Reduce_job.Tasks[task_id] = task_info

			reply.Task_type = REDUCE_TASK
			reply.Task_id = task_id
			reply.Map_task_ids = m.Reduce_job.Task_ids
			break
		}
        if is_find_job {
			log.Printf("Has found a reduce job for worker-%d.", args.Worker_id)
			log.Printf("Reduce task assigned: task_id: %d, info %+v", reply.Task_id, m.Reduce_job.Tasks[reply.Task_id])
		} else {
			//log.Printf("No reduce job to assign, so tell worker-%d to wait.", args.Worker_id)
			reply.Task_type = WAIT_TASK
		}	
	default:
		log.Fatalf("Wrong job stage")
	}
	m.mu.Unlock()
	return nil
}

func (m *Master) ReportMapTaskDone(args *ReportMapTaskDoneArgs, reply *ReportMapTaskDoneReply) error {
	m.mu.Lock()
	tmp := m.Map_job.Tasks[args.Input_file_name]
	if tmp.Is_over {
		//this happens when a woker finish the job beyond the deadline,
		//or due to a worker beyond the deadline, the task is reassigned, but the beyond-deadline worker finish first,
		//the reassigned worker found task is already finished.
		//Ignore these cases here is ok.
		//We handle these two cases by update the task_id to show which worker does the job.
		return nil
	}
    tmp.Task_id = args.Task_id
	tmp.Is_running = false
	tmp.Is_over = true
	tmp.Worker_id = args.Worker_id
	tmp.End_time = time.Now()
	m.Map_job.Tasks[args.Input_file_name] = tmp
	m.mu.Unlock()

	return nil
}

func (m *Master) ReportReduceTaskDone (args *ReportReduceTaskDoneArgs, 
	reply *ReportReduceTaskDoneReply) error {
	m.mu.Lock()
	tmp := m.Reduce_job.Tasks[args.Task_id]
	if tmp.Is_over {
		//same as ReportMapTaskDone
		return nil
	}
	tmp.Is_running = false
	tmp.Is_over = true
	tmp.Worker_id = args.Worker_id
	tmp.End_time = time.Now()
	m.Reduce_job.Tasks[args.Task_id] = tmp
	m.mu.Unlock()
    return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {

	// Your code here.
	return m.Is_done
}

func (m *Master) map_tasks_check_cron() error {
	for {
		time.Sleep(time.Millisecond * time.Duration(TASK_CHECK_BREAK))
		//log.Println("Map task check")
		m.mu.Lock()
		var is_all_over bool = true
		for input_file_name, task_info := range m.Map_job.Tasks {
			if task_info.Is_over {
				continue
			}
			//check if task is overtime, if it's true, reassign it.
			if task_info.Is_running &&
				time.Since(task_info.Start_time).Seconds() > float64(TASK_DEAD_TIME) {
				task_info.Is_running = false
				task_id++
				task_info.Task_id = task_id
				task_info.Worker_id = -1 //-1 for no worker
				task_info.Failed_times++
				m.Map_job.Tasks[input_file_name] = task_info
			}
			is_all_over = false
		}
		m.mu.Unlock()
		if is_all_over {
			if !m.build_reduce_tasks() {
				log.Fatalf("Build reduce tasks fail.")
			}
			m.Job_stage = REDUCE_STAGE //transfer to reduce stage
			log.Println("Map phase is over, now start Reduce phase.")
			break
		}
	}

	return nil
}

func (m *Master) reduce_tasks_check_cron() {
    for{
		time.Sleep(time.Millisecond * time.Duration(TASK_CHECK_BREAK))
		if m.Job_stage != REDUCE_STAGE {
			continue
		}
		m.mu.Lock()
		var is_all_over = true
		for task_id, task_info := range m.Reduce_job.Tasks {
			if task_info.Is_over {
				continue
			}
			is_all_over = false
			//check if task is overtime, if it's true, reassign it.
			if task_info.Is_running &&
			   time.Since(task_info.Start_time).Seconds() > float64(TASK_DEAD_TIME) {
                task_info.Is_running = false
				task_info.Worker_id = -1
				task_info.Failed_times++
				m.Reduce_job.Tasks[task_id] = task_info
			}
		}
		m.mu.Unlock()
        
		if is_all_over {
			log.Printf("All reduce tasks is done, master start to quit.")
			m.Is_done = true
			break
		}
	}
}

//When all map tasks is done, call this function
//to build reduce tasks.
func (m *Master) build_reduce_tasks () bool {
	log.Printf("Start to build reduce tasks.")
    for _, task_info := range m.Map_job.Tasks {
		m.Reduce_job.Task_ids = append(m.Reduce_job.Task_ids, task_info.Task_id)
	}
	log.Printf("All task ids are: %+v", m.Reduce_job.Task_ids)
	for i := 0; int64(i) < m.Reduce_num; i++ {
		m.Reduce_job.Tasks[int64(i)] = TaskInfo {
            Task_type: REDUCE_TASK,
			Task_id: int64(i),
			Is_running: false,
			Is_over: false,
			Worker_id: -1,
		}
	}
	log.Printf("Build reduce tasks done: %+v", m.Reduce_job.Tasks)
	return true
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{Is_done: false,
		Current_worker_id: 1,
		Worker_map:        make(map[int64]WorkerInfo),
		Job_stage:         MAP_STAGE,
		Map_job:           MapJob{Tasks: make(map[string]TaskInfo)},
		Reduce_job:        ReduceJob{Tasks: make(map[int64]TaskInfo)},
		Reduce_num:        int64(nReduce),
	}

	//initialize map Tasks.
	for _, v := range files {
		task_id++
		m.Map_job.Tasks[v] = TaskInfo{
			Task_type:       MAP_TASK,
			Task_id:         task_id,
			Worker_id:       -1,
			Is_running:      false,
			Is_over:         false,
			Input_file_name: v,
			Failed_times:    0,
		}
	}

	fmt.Printf("%+v\n", m.Map_job)

	go m.map_tasks_check_cron()
	go m.reduce_tasks_check_cron()
	m.server()
	return &m
}
