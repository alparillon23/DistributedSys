package main

import (
	"flag"
	"log"

	distkvs "example.org/cpsc416/a6"
	"example.org/cpsc416/a6/kvslib"
)

func main() {
	var config distkvs.ClientConfig
	err := distkvs.ReadJSONConfig("config/client_config.json", &config)
	if err != nil {
		log.Fatal(err)
	}
	flag.StringVar(&config.ClientID, "id", config.ClientID, "client ID, e.g. client1")
	flag.Parse()

	client := distkvs.NewClient(config, kvslib.NewKVS())
	if err := client.Initialize(); err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	if err, _ := client.Put(config.ClientID, "key3", "value90"); err != 0 {
		log.Println(err)
	}
	if err, _ := client.Get(config.ClientID, "key3"); err != 0 {
		log.Println(err)
	}
	if err, _ := client.Put(config.ClientID, "key3", "value250"); err != 0 {
		log.Println(err)
	}
	if err, _ := client.Put(config.ClientID, "key3", "value360"); err != 0 {
		log.Println(err)
	}
	if err, _ := client.Get(config.ClientID, "key3"); err != 0 {
		log.Println(err)
	}
	if err, _ := client.Put(config.ClientID, "key2", "value66"); err != 0 {
		log.Println(err)
	}
	if err, _ := client.Put(config.ClientID, "key3", "valueBABY99"); err != 0 {
		log.Println(err)
	}
	if err, _ := client.Get(config.ClientID, "key8"); err != 0 {
		log.Println(err)
	}
	if err, _ := client.Put(config.ClientID, "key4", "value25"); err != 0 {
		log.Println(err)
	}
	if err, _ := client.Put(config.ClientID, "key7", "value454"); err != 0 {
		log.Println(err)
	}
	if err, _ := client.Get(config.ClientID, "key8"); err != 0 {
		log.Println(err)
	}

	for i := 0; i < 11; i++ {
		result := <-client.NotifyChannel
		log.Println(result)
	}
}
