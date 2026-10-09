// Command inspect is a standalone public-API consumer; it performs no writes.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	vhost "github.com/yckao/virtio-net-recovery/vhost"
)

func main() {
	pid := flag.Int("pid", 0, "Expected QEMU PID")
	start := flag.Uint64("start-time", 0, "Expected /proc stat start time")
	object := flag.String("bpf-object", "build/vhost-observe.bpf.o", "Matching observation BPF object")
	flag.Parse()
	if err := run(*pid, *start, *object); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(pid int, start uint64, object string) error {
	host, err := vhost.Open(vhost.Options{BPFObject: object, StateDir: "/run/vhost-recovery", MaxQueues: 512})
	if err != nil {
		return err
	}
	defer host.Close()
	session, err := host.OpenProcess(context.Background(), vhost.ProcessIdentity{PID: pid, StartTime: start})
	if err != nil {
		return err
	}
	defer session.Close()
	inventory, err := session.Inventory(context.Background())
	if err != nil {
		return err
	}
	for _, problem := range inventory.Problems {
		fmt.Printf("slot %d unavailable: %v\n", problem.Slot, problem.Err)
	}
	for _, queue := range inventory.Queues {
		sample, err := session.Inspect(context.Background(), queue)
		if err != nil {
			return err
		}
		fmt.Printf("%s avail=%d used=%d consumed=%d\n", queue, sample.Avail, sample.Used, sample.Consumed)
	}
	return nil
}
