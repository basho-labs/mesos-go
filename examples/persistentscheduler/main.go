/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"strings"

	"code.google.com/p/go-uuid/uuid"
	"github.com/gogo/protobuf/proto"
	log "github.com/golang/glog"
	"github.com/mesos/mesos-go/auth"
	"github.com/mesos/mesos-go/auth/sasl"
	"github.com/mesos/mesos-go/auth/sasl/mech"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	sched "github.com/mesos/mesos-go/scheduler"
	"golang.org/x/net/context"
)

const (
	CPUS_PER_TASK       = 0.1
	MEM_PER_TASK        = 32
	DISK_PER_TASK       = 16
	defaultArtifactPort = 12345
)

var (
	address      = flag.String("address", "127.0.0.1", "Binding address for artifact server")
	artifactPort = flag.Int("artifactPort", defaultArtifactPort, "Binding port for artifact server")
	authProvider = flag.String("mesos_authentication_provider", sasl.ProviderName,
		fmt.Sprintf("Authentication provider to use, default is SASL that supports mechanisms: %+v", mech.ListSupported()))
	master              = flag.String("master", "127.0.0.1:5050", "Master address <ip:port>")
	executorPath        = flag.String("executor", "./executor", "Path to test executor")
	taskCount           = flag.String("task-count", "5", "Total task count to run.")
	mesosUser           = flag.String("user", "root", "Framework User")
	mesosRole           = flag.String("role", "*", "Framework Role")
	mesosAuthPrincipal  = flag.String("mesos_authentication_principal", "", "Mesos authentication principal.")
	mesosAuthSecretFile = flag.String("mesos_authentication_secret_file", "", "Mesos authentication secret file.")
)

type ExamplePersistentScheduler struct {
	executor      *mesos.ExecutorInfo
	role          *string
	principal     *string
	tasksLaunched int
	tasksFinished int
	totalTasks    int
	myTasks       []*MyTask
}

type MyTaskState int

const (
	IsUnknown MyTaskState = iota
	HasReservation
	HasPersistentVolume
	IsStarting
	IsStarted
	IsShutdown
	IsFailed
)

type MyTask struct {
	Name             string
	CurrentState     MyTaskState
	DestinationState MyTaskState
	TaskID           *mesos.TaskID
	VolumeID         string
	SlaveID          *mesos.SlaveID
	ContainerPath    string
	// TaskStatus       *mesos.TaskStatus
	// LastTaskInfo     *mesos.TaskInfo
	// LastOfferUsed    *mesos.Offer
	// TaskData         common.TaskData
	// FrameworkName    string
	// ClusterName      string
}

func newExamplePersistentScheduler(exec *mesos.ExecutorInfo, role *string, principal *string) *ExamplePersistentScheduler {
	total, err := strconv.Atoi(*taskCount)
	if err != nil {
		total = 5
	}

	tasks := make([]*MyTask, total)
	for i := range tasks {
		tasks[i].Name = "mytask-" + strconv.Itoa(-42)
		tasks[i].CurrentState = IsUnknown
		tasks[i].DestinationState = IsStarted
		tasks[i].ContainerPath = "volume"
	}

	return &ExamplePersistentScheduler{
		executor:      exec,
		role:          role,
		principal:     principal,
		tasksLaunched: 0,
		tasksFinished: 0,
		totalTasks:    total,
		myTasks:       tasks,
	}
}

func shardInitialResources(role *string) []*mesos.Resource {
	cpus := util.NewScalarResource("cpus", 0.1)
	cpus.Role = role
	mem := util.NewScalarResource("mem", 32)
	mem.Role = role
	disk := util.NewScalarResource("disk", 16)
	disk.Role = role

	var resources []*mesos.Resource
	resources = make([]*mesos.Resource, 3)
	resources[0] = cpus
	resources[1] = mem
	resources[2] = disk

	return resources
}

func taskPersistentVolume(persistenceID *string, containerPath *string) *mesos.Resource {

	mode := mesos.Volume_RW
	volume := &mesos.Volume{
		ContainerPath: containerPath,
		Mode:          &mode,
	}

	persistence := &mesos.Resource_DiskInfo_Persistence{
		Id: persistenceID,
	}

	info := &mesos.Resource_DiskInfo{
		Persistence: persistence,
		Volume:      volume,
	}

	resource := util.NewScalarResource("disk", DISK_PER_TASK)
	resource.Disk = info

	return resource
}

func taskReservations(role *string, principal *string, persistenceId *string, containerPath *string) []*mesos.Resource {
	resources := []*mesos.Resource{}

	reservation := &mesos.Resource_ReservationInfo{
		Principal: principal,
	}

	cpus := util.NewScalarResource("cpus", CPUS_PER_TASK)
	cpus.Role = role
	cpus.Reservation = reservation

	mem := util.NewScalarResource("mem", MEM_PER_TASK)
	mem.Role = role
	mem.Reservation = reservation

	disk := taskPersistentVolume(persistenceId, containerPath)
	disk.Role = role
	disk.Reservation = reservation

	resources = append(resources, cpus)
	resources = append(resources, mem)
	resources = append(resources, disk)

	return resources
}

func reserveOperation(reservations []*mesos.Resource) *mesos.Offer_Operation {
	reserve := &mesos.Offer_Operation_Reserve{
		Resources: reservations,
	}
	operationType := mesos.Offer_Operation_RESERVE
	operation := &mesos.Offer_Operation{
		Type:    &operationType,
		Reserve: reserve,
	}

	return operation
}

func createOperation(volumes []*mesos.Resource) *mesos.Offer_Operation {
	create := &mesos.Offer_Operation_Create{
		Volumes: volumes,
	}
	operationType := mesos.Offer_Operation_CREATE
	operation := &mesos.Offer_Operation{
		Type:   &operationType,
		Create: create,
	}

	return operation
}

func launchOperation(tasks []*mesos.TaskInfo) *mesos.Offer_Operation {
	launch := &mesos.Offer_Operation_Launch{
		TaskInfos: tasks,
	}
	operationType := mesos.Offer_Operation_LAUNCH
	operation := &mesos.Offer_Operation{
		Type:   &operationType,
		Launch: launch,
	}

	return operation
}

func (sched *ExamplePersistentScheduler) Registered(driver sched.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	log.Infoln("Framework Registered with Master ", masterInfo)
}

func (sched *ExamplePersistentScheduler) Reregistered(driver sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	log.Infoln("Framework Re-Registered with Master ", masterInfo)
}

func (sched *ExamplePersistentScheduler) Disconnected(sched.SchedulerDriver) {
	log.Fatalf("disconnected from master, aborting")
}

func (sched *ExamplePersistentScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {

	if sched.tasksLaunched >= sched.totalTasks {
		log.Info("decline all of the offers since all of our tasks are already launched")
		ids := make([]*mesos.OfferID, len(offers))
		for i, offer := range offers {
			ids[i] = offer.Id
		}
		driver.LaunchTasks(ids, []*mesos.TaskInfo{}, &mesos.Filters{RefuseSeconds: proto.Float64(120)})
		return
	}
	for _, offer := range offers {
		// Operations to perform on the offer
		operations := []*mesos.Offer_Operation{}

		cpuResources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
			return res.GetName() == "cpus"
		})
		cpus := 0.0
		for _, res := range cpuResources {
			cpus += res.GetScalar().GetValue()
		}
		memResources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
			return res.GetName() == "mem"
		})
		mems := 0.0
		for _, res := range memResources {
			mems += res.GetScalar().GetValue()
		}
		diskResources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
			return res.GetName() == "disk"
		})
		disks := 0.0
		for _, res := range diskResources {
			disks += res.GetScalar().GetValue()
		}

		remainingCpus := cpus
		remainingMems := mems
		remainingDisks := disks

		log.Infoln("Received Offer <", offer.Id.GetValue(), "> with cpus=", cpus, " mem=", mems, " disk=", disks)

		for _, myTask := range sched.myTasks {

			if sched.tasksLaunched >= sched.totalTasks ||
				CPUS_PER_TASK > remainingCpus ||
				MEM_PER_TASK > remainingMems ||
				DISK_PER_TASK > remainingDisks {

				log.Infoln("Not enough resources available to schedule additional tasks")
				break
			}

			switch myTask.CurrentState {
			case IsUnknown:
				persistenceId := uuid.NewRandom().String()
				reservations := taskReservations(
					sched.role,
					sched.principal,
					&persistenceId,
					&myTask.ContainerPath)

				operation := reserveOperation(reservations)
				operations = append(operations, operation)

				myTask.CurrentState = HasReservation
				myTask.VolumeID = *reservations[2].Disk.Persistence.Id
				myTask.SlaveID = offer.SlaveId

				log.Infoln("Task had IsUnknown state")
			case HasReservation:
				if offer.SlaveId != myTask.SlaveID {
					continue
				}

				volumes := taskReservations(
					sched.role,
					sched.principal,
					&myTask.VolumeID,
					&myTask.ContainerPath)

				operation := createOperation(volumes)
				operations = append(operations, operation)

				myTask.CurrentState = HasPersistentVolume

				log.Infoln("Task had HasReservation state")
			case HasPersistentVolume:
				if offer.SlaveId != myTask.SlaveID {
					continue
				}

				resources := taskReservations(
					sched.role,
					sched.principal,
					&myTask.VolumeID,
					&myTask.ContainerPath)

				taskId := &mesos.TaskID{
					Value: proto.String(strconv.Itoa(sched.tasksLaunched)),
				}

				taskInfo := &mesos.TaskInfo{
					Name:      proto.String(myTask.Name),
					TaskId:    taskId,
					SlaveId:   offer.SlaveId,
					Executor:  sched.executor,
					Resources: resources,
				}
				tasks := []*mesos.TaskInfo{}
				tasks = append(tasks, taskInfo)

				operation := launchOperation(tasks)
				operations = append(operations, operation)

				myTask.CurrentState = IsStarting
				myTask.TaskID = taskId

				sched.tasksLaunched++
				remainingCpus -= CPUS_PER_TASK
				remainingMems -= MEM_PER_TASK

				log.Infof("Prepared task: %s with offer %s for launch\n", taskInfo.GetName(), offer.Id.GetValue())
				log.Infoln("Task had HasPersistentVolume state")
			case IsStarting:
				log.Infoln("Task had IsStarting state")
			case IsStarted:
				log.Infoln("Task had IsStarted state")
			case IsShutdown:
				log.Infoln("Task had IsShutdown state")
			case IsFailed:
				log.Infoln("Task had IsFailed state")
			default:
				log.Infoln("Task had undefined state")
			}
		}

		log.Infoln("Executing ", len(operations), " operations for offer", offer.Id.GetValue())
		driver.AcceptOffers([]*mesos.OfferID{offer.Id}, operations, &mesos.Filters{RefuseSeconds: proto.Float64(5)})
	}
}

func (sched *ExamplePersistentScheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	log.Infoln("Status update: task", status.TaskId.GetValue(), " is in state ", status.State.Enum().String())
	if status.GetState() == mesos.TaskState_TASK_FINISHED {
		sched.tasksFinished++
	}

	if sched.tasksFinished >= sched.totalTasks {
		log.Infoln("Total tasks completed, stopping framework.")
		driver.Stop(false)
	}

	if status.GetState() == mesos.TaskState_TASK_LOST ||
		status.GetState() == mesos.TaskState_TASK_KILLED ||
		status.GetState() == mesos.TaskState_TASK_FAILED ||
		status.GetState() == mesos.TaskState_TASK_ERROR {
		log.Infoln(
			"Aborting because task", status.TaskId.GetValue(),
			"is in unexpected state", status.State.String(),
			"with message", status.GetMessage(),
		)
		driver.Abort()
	}
}

func (sched *ExamplePersistentScheduler) OfferRescinded(_ sched.SchedulerDriver, oid *mesos.OfferID) {
	log.Errorf("offer rescinded: %v", oid)
}
func (sched *ExamplePersistentScheduler) FrameworkMessage(_ sched.SchedulerDriver, eid *mesos.ExecutorID, sid *mesos.SlaveID, msg string) {
	log.Errorf("framework message from executor %q slave %q: %q", eid, sid, msg)
}
func (sched *ExamplePersistentScheduler) SlaveLost(_ sched.SchedulerDriver, sid *mesos.SlaveID) {
	log.Errorf("slave lost: %v", sid)
}
func (sched *ExamplePersistentScheduler) ExecutorLost(_ sched.SchedulerDriver, eid *mesos.ExecutorID, sid *mesos.SlaveID, code int) {
	log.Errorf("executor %q lost on slave %q code %d", eid, sid, code)
}
func (sched *ExamplePersistentScheduler) Error(_ sched.SchedulerDriver, err string) {
	log.Errorf("Scheduler received error:", err)
}

// ----------------------- func init() ------------------------- //

func init() {
	flag.Parse()
	log.Infoln("Initializing the Example Scheduler...")
}

// returns (downloadURI, basename(path))
func serveExecutorArtifact(path string) (*string, string) {
	serveFile := func(pattern string, filename string) {
		http.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			http.ServeFile(w, r, filename)
		})
	}

	// Create base path (http://foobar:5000/<base>)
	pathSplit := strings.Split(path, "/")
	var base string
	if len(pathSplit) > 0 {
		base = pathSplit[len(pathSplit)-1]
	} else {
		base = path
	}
	serveFile("/"+base, path)

	hostURI := fmt.Sprintf("http://%s:%d/%s", *address, *artifactPort, base)
	log.V(2).Infof("Hosting artifact '%s' at '%s'", path, hostURI)

	return &hostURI, base
}

func prepareExecutorInfo() *mesos.ExecutorInfo {
	executorUris := []*mesos.CommandInfo_URI{}
	uri, executorCmd := serveExecutorArtifact(*executorPath)
	executorUris = append(executorUris, &mesos.CommandInfo_URI{Value: uri, Executable: proto.Bool(true)})

	// forward the value of the scheduler's -v flag to the executor
	v := 0
	if f := flag.Lookup("v"); f != nil && f.Value != nil {
		if vstr := f.Value.String(); vstr != "" {
			if vi, err := strconv.ParseInt(vstr, 10, 32); err == nil {
				v = int(vi)
			}
		}
	}
	executorCommand := fmt.Sprintf("./%s -logtostderr=true -v=%d", executorCmd, v)

	go http.ListenAndServe(fmt.Sprintf("%s:%d", *address, *artifactPort), nil)
	log.V(2).Info("Serving executor artifacts...")

	// Create mesos scheduler driver.
	return &mesos.ExecutorInfo{
		ExecutorId: util.NewExecutorID("default"),
		Name:       proto.String("Test Executor (Go)"),
		Source:     proto.String("go_test"),
		Command: &mesos.CommandInfo{
			Value: proto.String(executorCommand),
			Uris:  executorUris,
		},
	}
}

func parseIP(address string) net.IP {
	addr, err := net.LookupIP(address)
	if err != nil {
		log.Fatal(err)
	}
	if len(addr) < 1 {
		log.Fatalf("failed to parse IP from address '%v'", address)
	}
	return addr[0]
}

// ----------------------- func main() ------------------------- //

func main() {

	// build command executor
	exec := prepareExecutorInfo()

	// the framework
	fwinfo := &mesos.FrameworkInfo{
		User: proto.String(*mesosUser), // Mesos-go will fill in user.
		Name: proto.String("Test Persistence Framework (Go)"),
		Role: proto.String(*mesosRole),
	}

	cred := (*mesos.Credential)(nil)
	if *mesosAuthPrincipal != "" {
		fwinfo.Principal = proto.String(*mesosAuthPrincipal)
		secret, err := ioutil.ReadFile(*mesosAuthSecretFile)
		if err != nil {
			log.Fatal(err)
		}
		cred = &mesos.Credential{
			Principal: proto.String(*mesosAuthPrincipal),
			Secret:    secret,
		}
	}
	bindingAddress := parseIP(*address)
	config := sched.DriverConfig{
		Scheduler:      newExamplePersistentScheduler(exec, mesosRole, mesosAuthPrincipal),
		Framework:      fwinfo,
		Master:         *master,
		Credential:     cred,
		BindingAddress: bindingAddress,
		WithAuthContext: func(ctx context.Context) context.Context {
			ctx = auth.WithLoginProvider(ctx, *authProvider)
			ctx = sasl.WithBindingAddress(ctx, bindingAddress)
			return ctx
		},
	}
	driver, err := sched.NewMesosSchedulerDriver(config)

	if err != nil {
		log.Errorln("Unable to create a SchedulerDriver ", err.Error())
	}

	if stat, err := driver.Run(); err != nil {
		log.Infof("Framework stopped with status %s and error: %s\n", stat.String(), err.Error())
	}

}
