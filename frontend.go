package distkvs

import (
	"fmt"
	"github.com/DistributedClocks/tracing"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type StorageAddr string

// this matches the config file format in config/frontend_config.json
type FrontEndConfig struct {
	ClientAPIListenAddr  string
	StorageAPIListenAddr string
	Storage              StorageAddr
	TracerServerAddr     string
	TracerSecret         []byte
}

type FrontEndStorageStarted struct {
	StorageID string
}

type FrontEndStorageFailed struct {
	StorageID string
}

type KvslibPut struct {
	ClientId string
	OpId     uint32
	Key      string
	Value    string
	Trace    tracing.TracingToken
}

type KvslibGet struct {
	ClientId string
	OpId     uint32
	Key      string
	Value    string
	Trace    tracing.TracingToken
}

type KvsReply struct {
	Key   string
	Value *string
	Err   bool
	Trace tracing.TracingToken
}

//For the first Initialize <- storage
type StorageDial struct {
	StorageId string
	Addr      string
}

type ResultStruct struct {
	OpId        uint32
	StorageFail bool
	Result      *string
}

type FrontEndPut struct {
	Key   string
	Value string
}

type FrontEndPutResult struct {
	Err bool
}

type FrontEndGet struct {
	Key string
}

type FrontEndGetResult struct {
	Key   string
	Value *string
	Err   bool
}

type FrontEndStorageJoined struct {
	StorageIds []string
}

type FrontEnd struct {
	// state may go here
	ftrace             *tracing.Trace
	storageClients     map[string]*rpc.Client
	storageJoinMutex   sync.Mutex
	storageUnJoinMutex sync.Mutex
	joinedNodes        []string
	unJoinedNodes      []string
	//joining mode channel
	joining   chan bool
	timeout   uint8
	client    *rpc.Client
	storeAddr string

	//request map channel (tasks)

	//toStorage

	//fromClient

	//storageTimeout

	//tracer

	//trace
}

func (f *FrontEnd) Start(clientAPIListenAddr string, storageAPIListenAddr string, storageTimeout uint8, ftrace *tracing.Tracer) error {
	var trace *tracing.Trace

	if ftrace != nil {
		trace = ftrace.CreateTrace()
	}

	handler := &FrontEnd{
		ftrace:             trace,
		client:             nil,
		storageClients:     make(map[string]*rpc.Client),
		joining:            make(chan bool, 1),
		timeout:            storageTimeout,
		storageJoinMutex:   sync.Mutex{},
		storageUnJoinMutex: sync.Mutex{},
	}
	server := rpc.NewServer()
	err := server.Register(handler) // publish Coordinator<->worker procs
	if err != nil {
		return fmt.Errorf("format of frontend RPCs aren't correct: %s", err)
	}

	clientListener, e := net.Listen("tcp", clientAPIListenAddr)
	if e != nil {
		return fmt.Errorf("failed to listen on %s: %s", clientAPIListenAddr, e)
	}

	storageAPIListener, e := net.Listen("tcp", storageAPIListenAddr)
	if e != nil {
		return fmt.Errorf("failed to listen on %s: %s", storageAPIListenAddr, e)
	}

	go server.Accept(clientListener)

	server.Accept(storageAPIListener)

	return nil

	//start trace

	//try to connect to client
	//if fail return error

	//try to connect to storage
	//if fail return error

	//build FrontEndRPC
	//register Front End
	//post success
	//keep running
	//return errors.New("not implemented")
}

//Helper that helps to communicate a joining effort/normal effort
func handleJoin(f *FrontEnd, client *rpc.Client, storageID string, reply *Joining) {
	f.storageJoinMutex.Lock()
	if len(f.joinedNodes) == 0 {
		reply.J = false
		f.joinedNodes = append(f.joinedNodes, storageID)
		f.storageClients[storageID] = client
		if f.ftrace != nil {
			f.ftrace.RecordAction(FrontEndStorageJoined{f.joinedNodes})
		}
		f.storageJoinMutex.Unlock()
	} else {
		reply.J = true
		select {
		case f.joining <- true:
			break
		default:
			break
		}
		f.storageJoinMutex.Unlock()
		go recoverState(f, storageID, client)
	}
}

func checkUnjoin(f *FrontEnd, storageID string) bool {
	f.storageUnJoinMutex.Lock()
	if len(f.unJoinedNodes) == 0 {
		f.storageUnJoinMutex.Unlock()
		return false
	}

	for _, m := range f.unJoinedNodes {
		if storageID == m {
			f.storageUnJoinMutex.Unlock()
			return true
		}
	}
	f.storageUnJoinMutex.Unlock()
	return false
}

//Helper that removes a failed storage id
//places it in the unjoined space
func failedStorageId(f *FrontEnd, storageID string) {
	f.storageJoinMutex.Lock()
	e := -1
	for i, l := range f.joinedNodes {
		if l == storageID {
			e = i
			break
		}
	}

	if e != -1 {
		h := f.joinedNodes[e]
		if f.ftrace != nil {
			f.ftrace.RecordAction(FrontEndStorageFailed{StorageID: storageID})
		}
		copy(f.joinedNodes[e:], f.joinedNodes[e+1:]) // Shift a[i+1:] left one index.
		f.joinedNodes[len(f.joinedNodes)-1] = ""     // Erase last element (write zero value).
		f.joinedNodes = f.joinedNodes[:len(f.joinedNodes)-1]
		f.storageUnJoinMutex.Lock()
		f.unJoinedNodes = append(f.unJoinedNodes, h)
		f.storageUnJoinMutex.Unlock()
	}
	f.storageJoinMutex.Unlock()
}

//Helper that removes the item from the off list,if present
func redoneStorageId(f *FrontEnd, storageID string) {
	f.storageUnJoinMutex.Lock()
	e := -1
	for i, l := range f.unJoinedNodes {
		if l == storageID {
			e = i
			break
		}
	}

	if e != -1 {
		copy(f.unJoinedNodes[e:], f.unJoinedNodes[e+1:]) // Shift a[i+1:] left one index.
		f.unJoinedNodes[len(f.unJoinedNodes)-1] = ""     // Erase last element (write zero value).
		f.unJoinedNodes = f.unJoinedNodes[:len(f.unJoinedNodes)-1]
	}
	f.storageUnJoinMutex.Unlock()
}

//function will communicate with existing storage nodes to find a potential node it can work with
func recoverState(f *FrontEnd, storageID string, client *rpc.Client) {
	f.storageJoinMutex.Lock()
	drep := StateFile{}
	dumm := Dummy{}
	for _, m := range f.joinedNodes {
		if checkUnjoin(f, m) {
			f.storageUnJoinMutex.Unlock()
			continue
		}
		if e, ok := f.storageClients[m]; ok {
			darg := Dummy{Check: "we"}
			err := e.Call("Storage.SendState", darg, &drep)
			if err != nil {
				sleep := time.Duration(f.timeout)
				time.Sleep(sleep * time.Second)
				err := e.Call("Storage.SendState", darg, &drep)
				if err != nil {
					f.storageJoinMutex.Unlock()
					failedStorageId(f, m)
					f.storageJoinMutex.Lock()
				} else {
					break
				}
			} else {
				break
			}
		}
	}

	err := client.Call("Storage.ReceiveState", drep, &dumm)
	if err != nil {
		sleep := time.Duration(f.timeout)
		time.Sleep(sleep * time.Second)
		err := client.Call("Storage.ReceiveState", drep, &dumm)
		if err != nil {
			f.storageJoinMutex.Unlock()
			failedStorageId(f, storageID)
		} else {
			f.joinedNodes = append(f.joinedNodes, storageID)
			redoneStorageId(f, storageID)
			f.storageClients[storageID] = client
			if f.ftrace != nil {
				f.ftrace.RecordAction(FrontEndStorageJoined{f.joinedNodes})
				select {
				case <-f.joining:
					break
				default:
					break
				}
			}
		}
	} else {
		f.joinedNodes = append(f.joinedNodes, storageID)
		redoneStorageId(f, storageID)
		f.storageClients[storageID] = client
		if f.ftrace != nil {
			f.ftrace.RecordAction(FrontEndStorageJoined{f.joinedNodes})
			select {
			case <-f.joining:
				break
			default:
				break
			}
		}
	}
	f.storageJoinMutex.Unlock()
}

func (f *FrontEnd) Initialize(args StorageDial, reply *Joining) error {
	f.storeAddr = args.Addr
	if f.ftrace != nil {
		f.ftrace.RecordAction(FrontEndStorageStarted{StorageID: args.StorageId})
	}
	client, err := rpc.Dial("tcp", args.Addr)
	if err != nil {
		sleep := time.Duration(f.timeout)
		time.Sleep(sleep * time.Second)
		cliente, erre := rpc.Dial("tcp", args.Addr)
		if erre != nil {
			f.storageUnJoinMutex.Lock()
			f.unJoinedNodes = append(f.unJoinedNodes, args.StorageId)
			f.storageUnJoinMutex.Unlock()
			if f.ftrace != nil {
				f.ftrace.RecordAction(FrontEndStorageFailed{StorageID: args.StorageId})
			}
		} else {
			handleJoin(f, cliente, args.StorageId, reply)
		}
	} else {
		handleJoin(f, client, args.StorageId, reply)
	}
	return nil
}

//HELPER FUNCTION
func get(f *FrontEnd, key string, value string, trace *tracing.Trace) (string, *string, bool, tracing.TracingToken) {
	for {
		if len(f.joining) != cap(f.joining) { //cap is 1, len 0 means empty
			break
		}
	}
	var token tracing.TracingToken
	if trace != nil {
		token = trace.GenerateToken()
	}
	args := struct {
		Key   string
		Value *string
		Token tracing.TracingToken
	}{
		Key:   key,
		Value: &value,
		Token: token,
	}
	freply := struct {
		Key         string
		Value       *string
		StorageFail bool
		Token       tracing.TracingToken
	}{
		Key:         key,
		Value:       &value,
		StorageFail: true,
	}
	f.storageJoinMutex.Lock()
	if len(f.joinedNodes) == 0 {
		f.storageJoinMutex.Unlock()
		return key, nil, true, nil
	}
	channelSize := len(f.joinedNodes)
	temp := make([]string, channelSize)
	copy(temp, f.joinedNodes)
	chanBool := make(chan struct {
		Key         string
		Value       *string
		StorageFail bool
		Token       tracing.TracingToken
	}, channelSize)
	f.storageJoinMutex.Unlock()

	for _, p := range temp {
		go goGet(f, key, value, p, chanBool, args, token)
	}
	for {
		if len(chanBool) == cap(chanBool) {
			break
		}
	}
	for i := 0; i < len(chanBool); i++ {
		stream := <-chanBool
		if trace != nil {
			if token != nil {
				if freply.Token == nil {
					freply.Token = token
				} else {
					f.ftrace.Tracer.ReceiveToken(token)
				}
			}
		}
		if stream.StorageFail == false {
			freply.Value = stream.Value
			freply.StorageFail = stream.StorageFail
		}
	}
	return key, freply.Value, freply.StorageFail, freply.Token
}

func goGet(f *FrontEnd, key string, value string, p string, repChan chan struct {
	Key         string
	Value       *string
	StorageFail bool
	Token       tracing.TracingToken
}, arg struct {
	Key   string
	Value *string
	Token tracing.TracingToken
}, token tracing.TracingToken) {
	reply := struct {
		Key         string
		Value       *string
		StorageFail bool
		Token       tracing.TracingToken
	}{
		Key:   key,
		Value: &value,
	}
	//if it's unjoined, skip
	w := checkUnjoin(f, p)
	if w {
		reply.StorageFail = true
		reply.Token = token
		repChan <- reply
		return
	}

	task := f.storageClients[p].Go("Storage.Read", arg, &reply, nil)
	<-task.Done
	if task.Error != nil || reply.StorageFail {
		sleep := time.Duration(f.timeout) //retry after this many seconds
		time.Sleep(sleep * time.Second)
		retry := f.storageClients[p].Go("Storage.Read", arg, &reply, nil)
		<-retry.Done
		if retry.Error != nil || reply.StorageFail {
			failedStorageId(f, p)
		}
	}
	repChan <- reply
}

func goPut(f *FrontEnd, key string, value string, p string, repChan chan struct {
	Key         string
	Value       *string
	StorageFail bool
	Token       tracing.TracingToken
}, arg struct {
	Key   string
	Value *string
	Token tracing.TracingToken
}, token tracing.TracingToken) {
	reply := struct {
		Key         string
		Value       *string
		StorageFail bool
		Token       tracing.TracingToken
	}{
		Key:   key,
		Value: &value,
	}
	//if it's unjoined, skip
	w := checkUnjoin(f, p)
	if w {
		reply.StorageFail = true
		reply.Token = token
		repChan <- reply
		return
	}

	task := f.storageClients[p].Go("Storage.Write", arg, &reply, nil)
	<-task.Done
	if task.Error != nil || reply.StorageFail {
		sleep := time.Duration(f.timeout) //retry after this many seconds
		time.Sleep(sleep * time.Second)
		retry := f.storageClients[p].Go("Storage.Write", arg, &reply, nil)
		<-retry.Done
		if retry.Error != nil || reply.StorageFail {
			failedStorageId(f, p)
		}
	}
	repChan <- reply
}
func put(f *FrontEnd, key string, value string, trace *tracing.Trace) (string, string, bool, tracing.TracingToken) {
	for {
		if len(f.joining) != cap(f.joining) { //cap is 1, len 0 means empty
			break
		}
	}
	var token tracing.TracingToken
	if trace != nil {
		token = trace.GenerateToken()
	}
	args := struct {
		Key   string
		Value *string
		Token tracing.TracingToken
	}{
		Key:   key,
		Value: &value,
		Token: token,
	}
	freply := struct {
		Key         string
		Value       *string
		StorageFail bool
		Token       tracing.TracingToken
	}{
		Key:         key,
		Value:       &value,
		StorageFail: true,
	}
	f.storageJoinMutex.Lock()
	if len(f.joinedNodes) == 0 {
		f.storageJoinMutex.Unlock()
		return key, value, true, nil
	}
	channelSize := len(f.joinedNodes)
	temp := make([]string, channelSize)
	chanBool := make(chan struct {
		Key         string
		Value       *string
		StorageFail bool
		Token       tracing.TracingToken
	}, channelSize)
	copy(temp, f.joinedNodes)
	f.storageJoinMutex.Unlock()

	for _, p := range temp {
		go goPut(f, key, value, p, chanBool, args, token)
	}
	for {
		if len(chanBool) == cap(chanBool) {
			break
		}
	}
	for i := 0; i < len(chanBool); i++ {
		stream := <-chanBool
		if trace != nil {
			if token != nil {
				if freply.Token == nil {
					freply.Token = token
				} else {
					f.ftrace.Tracer.ReceiveToken(token)
				}
			}
		}
		if stream.StorageFail == false {
			freply.Value = stream.Value
			freply.StorageFail = stream.StorageFail
		}
	}
	return key, value, freply.StorageFail, freply.Token
}

func (f *FrontEnd) Put(Args KvslibPut, Reply *KvsReply) error {
	//receive kvslib trace
	var trace *tracing.Trace
	if f.ftrace != nil {
		if Args.Trace != nil {
			trace = f.ftrace.Tracer.ReceiveToken(Args.Trace)
			trace.RecordAction(FrontEndPut{
				Key:   Args.Key,
				Value: Args.Value,
			})
		}
	}
	var token tracing.TracingToken
	key, val, fail, token := put(f, Args.Key, Args.Value, trace)

	if f.ftrace != nil {
		if token != nil {
			trace = f.ftrace.Tracer.ReceiveToken(token)
		}
	}
	if trace != nil {
		trace.RecordAction(FrontEndPutResult{Err: fail})
	}

	Reply.Key = key
	Reply.Value = &val
	Reply.Err = fail

	if trace != nil {
		Reply.Trace = trace.GenerateToken()
	}
	return nil

	//check if the channel for client:key:opId is filled
	//if filled, wait
	//if the fill was older question: fake add or real add
	//fill the queue for
	//if the fill was earlier: add as normal
	//try the add, if the add is accomplished

}

func (f *FrontEnd) Get(Args KvslibGet, Reply *KvsReply) error {
	//receive kvslib trace
	var trace *tracing.Trace
	if f.ftrace != nil {
		if Args.Trace != nil {
			trace = f.ftrace.Tracer.ReceiveToken(Args.Trace)
			trace.RecordAction(FrontEndGet{
				Key: Args.Key,
			})
		}
	}
	var token tracing.TracingToken
	var value *string
	key, value, fail, token := get(f, Args.Key, Args.Value, trace)
	if f.ftrace != nil {
		if token != nil {
			trace = f.ftrace.Tracer.ReceiveToken(token)
		}
	}
	if trace != nil {
		trace.RecordAction(FrontEndGetResult{
			Key:   key,
			Value: value,
			Err:   fail,
		})
	}

	Reply.Key = key
	Reply.Value = value
	Reply.Err = fail

	if trace != nil {
		Reply.Trace = trace.GenerateToken()
	}
	return nil

	//receive kvslib trace
	//check if the channel for client:key:opId is filled
	//if filled, wait
	//if the fill was older question: fake add or real add
	//fill the queue for
	//if the fill was earlier: add as normal
	//try the add, if the add is accomplished return the value
	//if not try again and if that doesn't work report an error
}
