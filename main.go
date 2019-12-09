package main

import (
	"etcd-service-discovery/discovery"
	"flag"
	"fmt"
)

func main() {
	var role = flag.String("role", "", "master | worker")
	flag.Parse()

	endpoints := []string{"http://127.0.0.1:2379"}
	if *role == "master" {
		master := discovery.NewMaster(endpoints)
		master.WatchWorkers()
	} else if *role == "worker" {
		worker := discovery.NewWorker("localhost", "127.0.0.1", endpoints)
		worker.HeartBeat()
	} else {
		fmt.Println("example -h for usage")
	}
}