package discovery

import (
	"context"
	"encoding/json"
	client "github.com/coreos/etcd/clientv3"
	"log"
	"time"
)

// Member is a client machine
type Member struct {
	InGroup bool
	IP      string
	Name    string
	CPU     int
}

// Master Node
type Master struct {
	members map[string]*Member
	API     *client.Client
}

// NewMaster method.
func NewMaster(endpoints []string) *Master {

	// etcd 配置
	cfg := client.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	}

	// 创建 etcd 客户端
	etcdClient, err := client.New(cfg)
	if err != nil {
		log.Fatal("Error: cannot connect to etcd: ", err)
	}

	// 创建 master
	master := &Master{
		members: make(map[string]*Member),
		API:     etcdClient,
	}

	return master
}

func (master *Master) WatchWorkers() {

	// 创建 watcher channel
	watcherCh := master.API.Watch(context.TODO(), "workers", client.WithPrefix())

	// 从 chanel 取数据
	for wresp := range watcherCh {
		for _, ev := range wresp.Events {
			key := string(ev.Kv.Key)
			if ev.Type.String() == "PUT" { // put 方法
				info := NodeToWorkerInfo(ev.Kv.Value)
				if _, ok := master.members[key]; ok {
					log.Println("Update worker ", info.Name)
					master.UpdateWorker(key,info)
				} else {
					log.Println("Add worker ", info.Name)
					master.AddWorker(key, info)
				}

			} else if ev.Type.String() == "DELETE" {  // del 方法
				log.Println("Delete worker ", key)
				delete(master.members, key)
			}
		}
	}

}


func (master *Master) AddWorker(key string,info *WorkerInfo) {
	member := &Member{
		InGroup: true,
		IP:      info.IP,
		Name:    info.Name,
		CPU:     info.CPU,
	}
	master.members[key] = member
}

func (master *Master) UpdateWorker(key string, info *WorkerInfo) {
	if member, ok := master.members[key]; ok {
		member.InGroup = true
	}
}

func NodeToWorkerInfo(value []byte) *WorkerInfo {
	info := &WorkerInfo{}
	err := json.Unmarshal(value, info)
	if err != nil {
		log.Print(err)
	}
	return info
}










