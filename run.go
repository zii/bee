package main

import (
	"fmt"
	"os"

	"github.com/ethersphere/bee/pkg/localstore"
	"github.com/ethersphere/bee/pkg/logging"
	"github.com/sirupsen/logrus"
)

func main() {
	fmt.Println("starting debug")
	path := os.Args[1]
	logger := logging.New(os.Stdout, logrus.TraceLevel)

	db, err := localstore.New(path, nil, nil, logger)
	if err != nil {
		fmt.Println(err)
	}
	defer db.Close()
	ind, err := db.DebugIndices()
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(ind)
}
