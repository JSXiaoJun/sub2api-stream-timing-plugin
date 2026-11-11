package main

import (
	"fmt"
	pluginv1 "github.com/Wei-Shaw/sub2api/pkg/pluginapi/v1"
	"inventory.local/sub2api-stream-timing/internal/timing"
	"os"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--version" {
		fmt.Println(timing.PluginID + " " + timing.Version)
		return
	}
	pluginv1.Serve(timing.New())
}
