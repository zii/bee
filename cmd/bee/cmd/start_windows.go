// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build windows

package cmd

import (
	"io"

	"github.com/Freman/eventloghook"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/eventlog"

	"github.com/ethersphere/bee/pkg/logging"
)

func isWindowsService() (bool, error) {
	return svc.IsWindowsService()
}

func createWindowsEventLogger(svcName string, w io.Writer, level logrus.Level) (logging.Logger, error) {
	elog, err := eventlog.Open(svcName)
	if err != nil {
		return nil, err
	}

	return logging.New(w, level, eventloghook.NewHook(elog)), nil
}
