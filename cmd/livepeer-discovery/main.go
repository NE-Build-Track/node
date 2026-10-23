package main

import (
	"fmt"
	"os"

	"github.com/livepeer/node/discovery"
)

func main() {
	if err := discovery.Root(os.Stdout, os.Stderr).Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}
