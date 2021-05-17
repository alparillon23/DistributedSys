package distkvs

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/rpc"
	"os"
	"sync"

	"github.com/DistributedClocks/tracing"
)

type StorageConfig struct {
	StorageID        string
	StorageAdd       StorageAddr
	ListenAddr       string
	FrontEndAddr     string
	DiskPath         string
	TracerServerAddr string
	TracerSecret     []byte
}

type StorageLoadSuccess struct {
	StorageID string
	State     map[string]string
}

//RPC
//contains the detail of a successful insertion
//false for not failed, true for failed
type Success struct {
	Succeeded bool
}
type Put struct {
	Key   string
	Value *string
	Token tracing.TracingToken
}
type Get struct {
	Key   string
	Value *string
	Token tracing.TracingToken
}
type Reply struct {
	Key         string
	Value       *string
	StorageFail bool
	Token       tracing.TracingToken
}

//end of RPC returns
//RPC arg
type StoragePut struct {
	StorageID string
	Key       string
	Value     string
}

type StorageSaveData struct {
	StorageID string
	Key       string
	Value     string
}

//RPC arg
type StorageGet struct {
	StorageID string
	Key       string
}

type StateFile struct {
	State map[string]string
}

type Dummy struct {
	Check string //useless in implementation, just following rpc requirements
}

//Used in order to receive context details
//Those details are if there exists an active storage node we can
//take the state from it in Joining
//if there isn't we don't go through that process and proceed as normal
type Joining struct {
	J bool
}

type StorageGetResult struct {
	StorageID string
	Key       string
	Value     *string
}

type StorageJoining struct {
	StorageID string
}

type StorageJoined struct {
	StorageID string
	State     map[string]string
}

//We will make two additional methods call tempW and tempR
//these methods will only be used when the storage is joining
//
type Storage struct {
	//state may go here
	StorageID string
	//state
	State map[string]string
	//log temp state while joining
	JState map[string]string
	//channel which indicates joining
	Joining chan bool
	//channel which indicates joined
	Joined chan bool
	//initialize block channel
	FrontEnd *rpc.Client
	//configuration
	Trace *tracing.Trace
	//frontend connection
	Path string
	//a map channel
	StorageAddr string
	//trace
	Mutex     sync.Mutex             //Critical for MutexMap
	MutexMap  map[string]*sync.Mutex //Critical when operations are on keys (normal op)
	JMutexMap map[string]*sync.Mutex //Critical when operations are on keys (joining op)
	FMutex    sync.Mutex             //File Mutex - for handling updates
	JMutex    sync.Mutex             //JMutex - use only for handling JState
}

func (s *Storage) Start(storageId string, frontEndAddr string, storageAddr string, diskPath string, strace *tracing.Tracer) error {
	var trace *tracing.Trace
	if strace != nil {
		trace = strace.CreateTrace()
	}
	handler := &Storage{
		StorageID:   storageId,
		State:       nil,
		JState:      make(map[string]string),
		Joining:     make(chan bool, 1),
		Joined:      make(chan bool, 1),
		FrontEnd:    nil,
		Trace:       trace,
		Path:        diskPath,
		StorageAddr: storageAddr,
		Mutex:       sync.Mutex{},
		FMutex:      sync.Mutex{},
		JMutex:      sync.Mutex{},
		MutexMap:    make(map[string]*sync.Mutex),
		JMutexMap:   make(map[string]*sync.Mutex),
	}
	var state map[string]string
	//Issue 1: File Read Problem
	file, err := ioutil.ReadFile(diskPath + "tempfile.json")
	if err != nil {
		state = make(map[string]string)
		err := os.MkdirAll(diskPath, os.ModePerm)
		if err != nil {
			return err
		}
		filer, err := os.Create(diskPath + "tempfile.json")
		//fmt.Print("It didn't work here \n")
		if err != nil {
			return err
		}
		encode, err := json.Marshal(state)
		if err != nil {
			return err
		}
		_, err = filer.Write(encode)
		if err != nil {
			return err
		}
		err = filer.Close()
		if err != nil {
			return err
		}
	} else {
		if err := json.Unmarshal(file, &state); err != nil {
			return fmt.Errorf("failure to parse: %s", err)
		}
		//Getting here means we successfully read the state
	}
	//Issue 2: File Parse Problem
	if trace != nil {
		trace.RecordAction(StorageLoadSuccess{StorageID: storageId, State: state})
	}

	handler.State = state
	//try connecting with FrontEnd
	client, err := rpc.Dial("tcp", frontEndAddr)
	if err != nil {
		return fmt.Errorf("failed to dial frontend: %s", err)
	}
	handler.FrontEnd = client

	server := rpc.NewServer()
	err = server.Register(handler)

	if err != nil {
		return fmt.Errorf("format of Storage RPCs aren't correct: %s", err)
	}
	//send over the storage address
	//type StorageDial struct{
	//	Addr string
	//}
	//func (f *FrontEnd) Initialize(Args StorageDial, Reply *struct{})
	storageAPIListener, e := net.Listen("tcp", storageAddr)
	if e != nil {
		return fmt.Errorf("failed to listen on %s: %s", storageAddr, e)
	}
	go server.Accept(storageAPIListener)
	ce := Joining{}
	connect := handler.FrontEnd.Go("FrontEnd.Initialize",
		struct {
			StorageId string
			Addr      string
		}{StorageId: storageId, Addr: storageAddr},
		&ce, nil)
	<-connect.Done
	if connect.Error != nil {
		return fmt.Errorf("cant connect to front end: %s", connect.Error)
	}
	//if frontend says do it, we trigger it
	if ce.J {
		s.Joining <- true
		if s.Trace != nil {
			s.Trace.RecordAction(StorageJoining{StorageID: storageId})
		}
	}
	for {

	}
}

//Function will be used to send a state from a working
//storage node to one receiving initializing
func (s *Storage) SendState(args Dummy, reply *StateFile) error {
	s.Mutex.Lock()
	reply.State = s.State
	s.Mutex.Unlock()
	return nil
}

//Function will be used to receive a state from the frontend
//sent by the working storage node
func (s *Storage) ReceiveState(args StateFile, reply *Dummy) error {
	err := newStateWrite(s, args.State)
	if err != nil {
		return fmt.Errorf("bad try at receiving the new state")
	}
	//remove the joining notification if there, otherwise still report while indicating joined
	select {
	case <-s.Joining:
		s.Joined <- true
		s.Mutex.Lock()
		if s.Trace != nil {
			s.Trace.RecordAction(StorageJoined{
				StorageID: s.StorageID,
				State:     s.State,
			})
		}
		s.Mutex.Unlock()
		break
	default:
		s.Joined <- true
		s.Mutex.Lock()
		if s.Trace != nil {
			s.Trace.RecordAction(StorageJoined{
				StorageID: s.StorageID,
				State:     s.State,
			})
		}
		s.Mutex.Unlock()
		break
	}
	return nil
}

//tempRead and tempWrite will both dominate during the joining mode
//otherwise (a joined state/no join or joined) we run normal operations
func tempWrite(s *Storage, trace *tracing.Trace, key string, value string) (string, error) {
	//Add to JState
	//pull from state (aka read)
	//amend the value
	//write back to state
	s.JMutex.Lock()
	_, gk := s.JMutexMap[key]
	if !gk {
		s.JMutexMap[key] = &sync.Mutex{}
	}
	s.JMutex.Unlock()
	//once the lock is released, we should see that the new state was already uploaded
	//or it wasn't yet
	select {
	//if the state is fixed do a normal write
	case <-s.Joined:
		s.Joined <- true
		return write(s, trace, key, value)
	//if the state is not yet fixed, still handle it - but don't manipulate the real state
	// that's handled elsewhere
	default:
		last := ""
		s.JMutexMap[key].Lock()
		temp, ok := s.JState[key]
		if ok {
			last = temp
			if temp == value {
				if trace != nil {
					trace.RecordAction(StorageSaveData{
						StorageID: s.StorageID,
						Key:       key,
						Value:     temp,
					})
				}
				s.JMutexMap[key].Unlock()
				return value, nil
			}
		} else {
			last = value
		}
		s.JState[key] = value
		s.JMutexMap[key].Unlock()

		s.FMutex.Lock()
		//we're dealing with a CRITICAL shared resource,
		//block all other file saves until this one completes
		//STRATEGY:
		//pull from memory, amend with new value then rewrite memory with the tempState
		//so we're not actually appending, but the result is still the same

		//PULLED FROM MEMORY DISC
		var state map[string]string
		//Issue 1: File Read Problem
		file, err := ioutil.ReadFile(s.Path + "tempfile.json")
		if err != nil {
			s.FMutex.Unlock()
			return value, fmt.Errorf("failed to read file from joining %s", err)
		}
		err = json.Unmarshal(file, &state)
		if err != nil {
			s.FMutex.Unlock()
			return value, fmt.Errorf("failed to parse disk from incoming join reqs %s", err)
		}
		state[key] = last
		//rewrite to disk
		encode, err := json.Marshal(state)
		if err != nil {
			s.FMutex.Unlock()
			return value, fmt.Errorf("failed to convert to byte array %s", err)
		}
		err = ioutil.WriteFile(s.Path+"tempfile.json", encode, os.ModePerm)
		if err != nil {
			s.FMutex.Unlock()
			return value, fmt.Errorf("failed to write to file %s", err)
		} else {
			if trace != nil {
				trace.RecordAction(StorageSaveData{
					StorageID: s.StorageID,
					Key:       key,
					Value:     value,
				})
			}
		}
		s.FMutex.Unlock()
		return value, nil
	}
}

func tempRead(s *Storage, trace *tracing.Trace, key string) (string, *string) {
	//check the contents of the
	//pull from state (aka read)
	//amend the value
	//write back to state
	s.Mutex.Lock()
	_, ok := s.MutexMap[key]
	if !ok {
		s.MutexMap[key] = &sync.Mutex{}
	}
	_, gk := s.JMutexMap[key]
	if !gk {
		s.JMutexMap[key] = &sync.Mutex{}
	}
	s.Mutex.Unlock()
	//Keys will be captured as they are recorded
	select {
	//if the state is fixed do a normal write
	case <-s.Joined:
		s.Joined <- true
		return read(s, trace, key)
		//if the state is not yet fixed, still handle it - but don't manipulate the real state
		// that's handled elsewhere
	default:
		//if it's not in JState (which would be the most recent iteration)
		//proceed to search in the old state
		var val *string
		s.JMutexMap[key].Lock()
		value, ok := s.JState[key]
		if ok {
			val = &value
		} else {
			s.MutexMap[key].Lock()
			valuee, ogk := s.State[key]
			if ogk {
				val = &valuee
			} else {
				val = nil
			}
			s.MutexMap[key].Unlock()
		}
		s.JMutexMap[key].Unlock()
		if trace != nil {
			trace.RecordAction(StorageGetResult{
				StorageID: s.StorageID,
				Key:       key,
				Value:     val,
			})
		}
		return key, val
	}
}

//newStateWrite is another method used in joining
//it is used to add the received new state safely
//Do the following
//New State->JState->Old State in terms of transmission
//JState->New State->Old State in terms of completeness
//New State -> Replace Old State value by value
//JState -> Replace the New State value by value
//if JState doesn't have the item, Write New State item on Disc
//if JState does have the item, Replace Old State value with JState
func newStateWrite(s *Storage, newState map[string]string) error {
	//LOCK WILL BLOCK SERVICE REQS FROM BEING SERVICED UNTIL WE'RE DONE
	s.JMutex.Lock()
	for key, value := range newState {
		//if found in the JState, make that value add to state (it's newer)
		temp, ok := s.JState[key]
		if ok {
			s.JMutexMap[key].Lock()
			s.State[key] = temp
			s.JMutexMap[key].Unlock()
		} else {
			_, err := swrite(s, key, value) //silent overwrite items
			if err != nil {
				return fmt.Errorf("bad try writing to state on join %s", err)
			}
		}
	}
	s.JMutex.Unlock()
	return nil
}

func (s *Storage) Write(args Put, reply *Reply) error {
	//receive the trace token
	var trace *tracing.Trace
	if s.Trace != nil {
		if args.Token != nil {
			trace = s.Trace.Tracer.ReceiveToken(args.Token)
		}
	}
	if trace != nil {
		trace.RecordAction(StoragePut{
			StorageID: s.StorageID,
			Key:       args.Key,
			Value:     *args.Value,
		})
	}

	select {
	case <-s.Joining:
		s.Joining <- true
		val, err := tempWrite(s, trace, args.Key, *args.Value)
		reply.Key = args.Key
		reply.Value = &val
		if err != nil {
			reply.StorageFail = true
		} else {
			reply.StorageFail = false
		}
		if trace != nil {
			reply.Token = trace.GenerateToken()
		}
		return nil
	default:
		val, err := write(s, trace, args.Key, *args.Value)
		reply.Key = args.Key
		reply.Value = &val
		if err != nil {
			reply.StorageFail = true
		} else {
			reply.StorageFail = false
		}
		if trace != nil {
			reply.Token = trace.GenerateToken()
		}
		return nil
	}
	/*
		//Say it's failed until we get confirmation it passed (at the end)
		reply.StorageFail = true
		//keep the last value
		last := ""
		s.Mutex.Lock()
		_, ok := s.MutexMap[args.Key]
		if !ok {
			s.MutexMap[args.Key] = &sync.Mutex{}
		}
		s.Mutex.Unlock()

		s.MutexMap[args.Key].Lock()
		temp, ok := s.State[args.Key]
		if ok {
			last = temp
			if temp == *args.Value {
				reply.Key = args.Key
				reply.Value = &temp
				reply.StorageFail = false
				trace.RecordAction(StorageSaveData{
					Key:   args.Key,
					Value: temp,
				})
				s.MutexMap[args.Key].Unlock()
				reply.Token = trace.GenerateToken()
				return nil
			}
		} else {
			last = *args.Value
		}
		s.State[args.Key] = *args.Value
		s.MutexMap[args.Key].Unlock()

		s.FMutex.Lock()
		//we're dealing with a CRITICAL shared resource,
		//block all other file saves until this one completes
		encode, err := json.Marshal(s.State)
		if err != nil {
			s.FMutex.Unlock()
			reply.Token = trace.GenerateToken()
			return fmt.Errorf("failed to convert to byte array %s", err)
		}
		err = ioutil.WriteFile(s.Path+"tempfile.json", encode, os.ModePerm)
		if err != nil {
			s.State[args.Key] = last
			s.FMutex.Unlock()
			reply.Token = trace.GenerateToken()
			return fmt.Errorf("failed to write to file %s", err)
		} else {
			trace.RecordAction(StorageSaveData{
				Key:   args.Key,
				Value: *args.Value,
			})
			reply.StorageFail = false //the storage has succeeded if we get here
		}
		s.FMutex.Unlock()

		reply.Key = args.Key
		reply.Value = args.Value
		reply.Token = trace.GenerateToken()

		//keep the last value
		//ammend the new value in the map
		//write to disk, if that fails, keep false and remmend the last value
		//if it succeeds place the value in the map and keep true
		//fmt.Print("We have arrived at our destination \n")
		return nil

	*/
}

func (s *Storage) Read(args Get, reply *Reply) error {
	//trace
	var trace *tracing.Trace
	if s.Trace != nil {
		if args.Token != nil {
			trace = s.Trace.Tracer.ReceiveToken(args.Token)
		}
	}
	if trace != nil {
		trace.RecordAction(StorageGet{StorageID: s.StorageID, Key: args.Key})
	}

	select {
	case <-s.Joining:
		s.Joining <- true
		key, val := tempRead(s, trace, args.Key)
		reply.Key = key
		reply.Value = val
		reply.StorageFail = false
		if trace != nil {
			reply.Token = trace.GenerateToken()
		}
		return nil
	default:
		key, val := read(s, trace, args.Key)
		reply.Key = key
		reply.Value = val
		reply.StorageFail = false
		if trace != nil {
			reply.Token = trace.GenerateToken()
		}
		return nil
	}

	/*
		reply.Key = args.Key
		s.Mutex.Lock()
		_, ok := s.MutexMap[args.Key]
		if !ok {
			s.MutexMap[args.Key] = &sync.Mutex{}
		}
		s.Mutex.Unlock()

		s.MutexMap[args.Key].Lock()
		value, ok := s.State[args.Key]
		if ok {
			reply.Value = &value
		} else {
			reply.Value = nil
		}
		trace.RecordAction(StorageGetResult{
			Key:   args.Key,
			Value: reply.Value,
		})
		s.MutexMap[args.Key].Unlock()

		reply.StorageFail = false
		reply.Token = trace.GenerateToken()
		//wait on initialization
		//check if the value is in the map
		//if it exists return the value and true in reply
		//else try reading from the disk
		//if the disk fails, accept that it is false
		//there is no value, accept that is true and return no value
		return nil

	*/
}

//Helper functions that can be referenced in the right places
//read takes the data from the solid state
func read(s *Storage, trace *tracing.Trace, key string) (string, *string) {
	s.Mutex.Lock()
	_, ok := s.MutexMap[key]
	if !ok {
		s.MutexMap[key] = &sync.Mutex{}
	}
	s.Mutex.Unlock()
	var val *string
	s.MutexMap[key].Lock()
	value, ok := s.State[key]
	if ok {
		val = &value
	} else {
		val = nil
	}
	if trace != nil {
		trace.RecordAction(StorageGetResult{
			StorageID: s.StorageID,
			Key:       key,
			Value:     val,
		})
	}
	s.MutexMap[key].Unlock()

	return key, val
}

//write inserts data into the solid state
func write(s *Storage, trace *tracing.Trace, key string, value string) (string, error) {
	//throw an error
	//keep the last value
	last := ""
	s.Mutex.Lock()
	_, ok := s.MutexMap[key]
	if !ok {
		s.MutexMap[key] = &sync.Mutex{}
	}
	s.Mutex.Unlock()

	s.MutexMap[key].Lock()
	temp, ok := s.State[key]
	if ok {
		last = temp
		if temp == value {
			if trace != nil {
				trace.RecordAction(StorageSaveData{
					StorageID: s.StorageID,
					Key:       key,
					Value:     temp,
				})
			}
			s.MutexMap[key].Unlock()
			return value, nil
		}
	} else {
		last = value
	}
	s.State[key] = value
	s.MutexMap[key].Unlock()

	s.FMutex.Lock()
	//we're dealing with a CRITICAL shared resource,
	//block all other file saves until this one completes
	encode, err := json.Marshal(s.State)
	if err != nil {
		s.FMutex.Unlock()
		return value, fmt.Errorf("failed to convert to byte array %s", err)
	}
	err = ioutil.WriteFile(s.Path+"tempfile.json", encode, os.ModePerm)
	if err != nil {
		s.State[key] = last
		s.FMutex.Unlock()
		return value, fmt.Errorf("failed to write to file %s", err)
	} else {
		if trace != nil {
			trace.RecordAction(StorageSaveData{
				StorageID: s.StorageID,
				Key:       key,
				Value:     value,
			})
		}
	}
	s.FMutex.Unlock()
	return value, nil
}

//write inserts data into the solid state
func swrite(s *Storage, key string, value string) (string, error) {
	//throw an error
	//keep the last value
	last := ""
	s.Mutex.Lock()
	_, ok := s.MutexMap[key]
	if !ok {
		s.MutexMap[key] = &sync.Mutex{}
	}
	s.Mutex.Unlock()

	s.MutexMap[key].Lock()
	temp, ok := s.State[key]
	if ok {
		last = temp
		if temp == value {
			s.MutexMap[key].Unlock()
			return value, nil
		}
	} else {
		last = value
	}
	s.State[key] = value
	s.MutexMap[key].Unlock()

	s.FMutex.Lock()
	//we're dealing with a CRITICAL shared resource,
	//block all other file saves until this one completes
	encode, err := json.Marshal(s.State)
	if err != nil {
		s.FMutex.Unlock()
		return value, fmt.Errorf("failed to convert to byte array %s", err)
	}
	err = ioutil.WriteFile(s.Path+"tempfile.json", encode, os.ModePerm)
	if err != nil {
		s.State[key] = last
		s.FMutex.Unlock()
		return value, fmt.Errorf("failed to write to file %s", err)
	}
	s.FMutex.Unlock()
	return value, nil
}
