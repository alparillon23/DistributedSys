package distkvs

import (
	"errors"

	"example.org/cpsc416/a6/kvslib"
	"github.com/DistributedClocks/tracing"
)

const ChCapacity = 10

type ClientConfig struct {
	ClientID         string
	FrontEndAddr     string
	TracerServerAddr string
	TracerSecret     []byte
}

type Client struct {
	NotifyChannel kvslib.NotifyChannel
	id            string
	frontEndAddr  string
	kvs           *kvslib.KVS
	tracer        *tracing.Tracer
	initialized   bool
	tracerConfig  tracing.TracerConfig
}

func NewClient(config ClientConfig, kvs *kvslib.KVS) *Client {
	return &Client{
		NotifyChannel: nil,
		id:            config.ClientID,
		frontEndAddr:  config.FrontEndAddr,
		kvs:           kvs,
		tracer: tracing.NewTracer(tracing.TracerConfig{
			ServerAddress:  config.TracerServerAddr,
			TracerIdentity: config.ClientID,
			Secret:         config.TracerSecret,
		}),
		initialized: false,
		tracerConfig: tracing.TracerConfig{
			ServerAddress:  config.TracerServerAddr,
			TracerIdentity: config.ClientID,
			Secret:         config.TracerSecret,
		},
	}
}

func (c *Client) Initialize() error {
	var err error
	c.NotifyChannel, err = c.kvs.Initialize(c.tracer, c.id, c.frontEndAddr, ChCapacity)
	if err != nil {
		return errors.New("failed to make channel")
	}
	c.initialized = true
	return nil
}

func (c *Client) Get(clientId string, key string) (uint32, error) {
	return c.kvs.Get(c.tracer, clientId, key)
}

func (c *Client) Put(clientId string, key string, value string) (uint32, error) {
	return c.kvs.Put(c.tracer, clientId, key, value)
}

func (c *Client) Close() error {
	return c.kvs.Close()
}
