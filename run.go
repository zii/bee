package main

import (
	"fmt"
	"os"

	"github.com/ethersphere/bee/pkg/localstore"
	"github.com/ethersphere/bee/pkg/logging"
	"github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"
)

func main() {
	fmt.Println("starting debug")
	path := os.Args[1]
	logger := logging.New(os.Stdout, logrus.TraceLevel)
	dba, err = leveldb.RecoverFile(path, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	dba.Close()
	db, err := localstore.New(path, nil, nil, logger)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()
	ind, err := db.DebugIndices()
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(ind)
}
