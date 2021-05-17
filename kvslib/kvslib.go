// Package kvslib provides an API which is a wrapper around RPC calls to the
// frontend.
package kvslib

import (
	"errors"
	"fmt"
	"github.com/DistributedClocks/tracing"
	"net/rpc"
	"sync"
)

type KvslibBegin struct {
	ClientId string
}

type KvslibPut struct {
	ClientId string
	OpId     uint32
	Key      string
	Value    string
}

type KvslibGet struct {
	ClientId string
	OpId     uint32
	Key      string
}

type KvslibPutResult struct {
	OpId uint32
	Err  bool
}

type KvslibGetResult struct {
	OpId  uint32
	Key   string
	Value *string
	Err   bool
}

type KvslibComplete struct {
	ClientId string
}

// NotifyChannel is used for notifying the client about a mining result.
type NotifyChannel chan ResultStruct

type ResultStruct struct {
	OpId        uint32
	StorageFail bool
	Result      *string
}

type KVS struct {
	notifyCh    NotifyChannel
	mutex       sync.Mutex
	mapKeys     map[string]*sync.Mutex
	mapProgress map[string]chan uint32
	wg          *sync.WaitGroup
	//mapTasks map[string]chan bool
	opId     uint32
	frontEnd *rpc.Client
	clientId string
	ltrace   *tracing.Trace
	// Add more KVS instance state here.
}

func NewKVS() *KVS {
	return &KVS{
		notifyCh: nil,
		opId:     uint32(0),
		mapKeys:  nil,
		frontEnd: nil,
		clientId: "",
		wg:       nil,
		ltrace:   nil,
	}
}

// Initialize Initializes the instance of KVS to use for connecting to the frontend,
// and the frontends IP:port. The returned notify-channel channel must
// have capacity ChCapacity and must be used by kvslib to deliver all solution
// notifications. If there is an issue with connecting, this should return
// an appropriate err value, otherwise err should be set to nil.
func (d *KVS) Initialize(localTracer *tracing.Tracer, clientId string, frontEndAddr string, chCapacity uint) (NotifyChannel, error) {
	var trce *tracing.Trace
	if localTracer != nil {
		trce = localTracer.CreateTrace()
	}

	d.ltrace = trce
	d.ltrace.RecordAction(KvslibBegin{ClientId: clientId})
	//connect to front-end

	var wg sync.WaitGroup
	d.wg = &wg

	d.notifyCh = make(NotifyChannel, chCapacity)
	d.clientId = clientId
	d.mapKeys = make(map[string]*sync.Mutex)
	d.mapProgress = make(map[string]chan uint32)
	//d.mapTasks = make(map[string]chan bool)
	d.opId = 0
	//make the new channel
	frontend, err := rpc.Dial("tcp", frontEndAddr)
	if err != nil {
		return nil, fmt.Errorf("error dialing front-end: %s", err)
	}
	d.frontEnd = frontend
	return d.notifyCh, nil
}

// Get is a non-blocking request from the client to the system. This call is used by
// the client when it wants to get value for a key.
func (d *KVS) Get(tracer *tracing.Tracer, clientId string, key string) (uint32, error) {
	// Should return OpId or error
	var trace *tracing.Trace
	if tracer != nil {
		trace = tracer.CreateTrace()
	}
	task := uint32(0) //initialized with zero to provide the lock

	// ensures that the OpId is securely the right one - no race conditions
	d.mutex.Lock()
	task = d.opId //get the isolated state opId and then advance it
	d.opId++
	// check that the key can be found in this map and use it, if not make it and THEN use it
	_, ok := d.mapKeys[clientId+key]
	if !ok {
		d.mapKeys[clientId+key] = &sync.Mutex{}
		d.mapProgress[clientId+key] = make(chan uint32, 1)
	}
	d.wg.Add(1)
	d.mapKeys[clientId+key].Lock()
	go getter(d.wg, d.mapProgress, d.notifyCh, d.frontEnd, key, task, clientId, trace)
	d.mapKeys[clientId+key].Unlock()
	d.mutex.Unlock()

	/*_, ck := d.mapTasks[clientId+":"+strconv.Itoa(int(task))]
	if !ck {
		d.mapTasks[clientId+":"+strconv.Itoa(int(task))] = make(chan bool)
	}
	*/
	//this will lock at the right client:key combination
	//THEORY ONE: If we are set with a particular order
	//The lock will hold if OpID1 < OpID2 when OpID2 accesses the map area, the block will sustain until
	//OpID1 completes. If the key is different, it will run as normal

	return task, nil
}

func getter(wg *sync.WaitGroup, keylocks map[string]chan uint32, channel NotifyChannel, frontend *rpc.Client, key string, opId uint32, clientId string, trace *tracing.Trace) {

	var token tracing.TracingToken
	if trace != nil {
		token = trace.GenerateToken()
	}
	select {
	case keylocks[clientId+key] <- opId:
		arg := struct {
			ClientId string
			OpId     uint32
			Key      string
			Trace    tracing.TracingToken
		}{
			ClientId: clientId,
			OpId:     opId,
			Key:      key,
			Trace:    token}
		rep := struct {
			Key   string
			Value *string
			Err   bool
			Trace tracing.TracingToken
		}{}
		if trace != nil {
			trace.RecordAction(KvslibGet{
				ClientId: clientId,
				OpId:     opId,
				Key:      key,
			})
		}

		frontend.Call("FrontEnd.Get", arg, &rep)

		if trace != nil {
			if rep.Trace != nil {
				trace.Tracer.ReceiveToken(rep.Trace)
				trace.RecordAction(KvslibGetResult{
					OpId:  opId,
					Key:   rep.Key,
					Value: rep.Value,
					Err:   rep.Err,
				})
			}
		}

		channel <- ResultStruct{
			OpId:        opId,
			StorageFail: rep.Err,
			Result:      rep.Value,
		}
		wg.Done()
		<-keylocks[clientId+key]
		return
	}

	//keylocks[clientId+key].Unlock()
}

func putter(wg *sync.WaitGroup, keylocks map[string]chan uint32, channel NotifyChannel, frontend *rpc.Client, key string, value string, opId uint32, clientId string, trace *tracing.Trace) {
	//keylocks[clientId+key].Lock()
	var token tracing.TracingToken
	if trace != nil {
		token = trace.GenerateToken()
	}
	select {
	case keylocks[clientId+key] <- opId:
		arg := struct {
			ClientId string
			OpId     uint32
			Key      string
			Value    string
			Trace    tracing.TracingToken
		}{
			ClientId: clientId,
			OpId:     opId,
			Key:      key,
			Value:    value,
			Trace:    token}
		rep := struct {
			Key   string
			Value *string
			Err   bool
			Trace tracing.TracingToken
		}{}
		if trace != nil {
			trace.RecordAction(KvslibPut{
				ClientId: clientId,
				OpId:     opId,
				Key:      key,
				Value:    value,
			})
		}
		frontend.Call("FrontEnd.Put", arg, &rep)
		if trace != nil {
			if rep.Trace != nil {
				trace.Tracer.ReceiveToken(rep.Trace)
				trace.RecordAction(KvslibPutResult{
					OpId: opId,
					Err:  rep.Err,
				})
			}
		}
		channel <- ResultStruct{
			OpId:        opId,
			StorageFail: rep.Err,
			Result:      rep.Value,
		}
		wg.Done()
		<-keylocks[clientId+key]
		return
	}

	//keylocks[clientId+key].Lock()
}

// Put is a non-blocking request from the client to the system. This call is used by
// the client when it wants to update the value of an existing key or add add a new
// key and value pair.
func (d *KVS) Put(tracer *tracing.Tracer, clientId string, key string, value string) (uint32, error) {
	var trace *tracing.Trace
	if tracer != nil {
		trace = tracer.CreateTrace()
	}

	// Should return OpId or error
	task := uint32(0) //initialized with zero to provide the lock

	// ensures that the OpId is securely the right one - no race conditions
	d.mutex.Lock()
	task = d.opId //get the isolated state opId and then advance it
	d.opId++
	// check that the key can be found in this map and use it, if not make it and THEN use it
	_, ok := d.mapKeys[clientId+key]
	if !ok {
		d.mapKeys[clientId+key] = &sync.Mutex{}
		d.mapProgress[clientId+key] = make(chan uint32, 1)
	}
	d.wg.Add(1)
	d.mapKeys[clientId+key].Lock()
	go putter(d.wg, d.mapProgress, d.notifyCh, d.frontEnd, key, value, task, clientId, trace)
	d.mapKeys[clientId+key].Unlock()
	d.mutex.Unlock()

	//this will lock at the right client:key combination
	//THEORY ONE: If we are set with a particular order
	//The lock will hold if OpID1 < OpID2 when OpID2 accesses this area, the block will sustain until
	//OpID1 completes. If the key is different, it will run as normal

	return task, nil
}

// Close Stops the KVS instance from communicating with the frontend and
// from delivering any solutions via the notify-channel. If there is an issue
// with stopping, this should return an appropriate err value, otherwise err
// should be set to nil.
func (d *KVS) Close() error {
	//d.wg.Done()
	d.wg.Wait()
	close(d.notifyCh)
	//fmt.Print("We waited")
	err := d.frontEnd.Close()
	if err != nil {
		return errors.New("front end not closed as expected")
	}
	d.ltrace.RecordAction(KvslibComplete{ClientId: d.clientId})
	return nil
}
