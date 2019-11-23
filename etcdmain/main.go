// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcdmain

import (
	"fmt"
	"os"
	"strings"

	"github.com/coreos/go-systemd/daemon"
	"go.uber.org/zap"
)

func Main() {
	// 检查当前的机器环境,这个一般不用管
	checkSupportArch()

	if len(os.Args) > 1 {
		// 是不是启动gateWay 或者grpc-proxy
		cmd := os.Args[1]
		if covArgs := os.Getenv("ETCDCOV_ARGS"); len(covArgs) > 0 {
			args := strings.Split(os.Getenv("ETCDCOV_ARGS"), "\xe7\xcd")[1:]
			rootCmd.SetArgs(args)
			cmd = "grpc-proxy"
		}
		switch cmd {
		case "gateway", "grpc-proxy":
			if err := rootCmd.Execute(); err != nil {
				fmt.Fprint(os.Stderr, err)
				os.Exit(1)
			}
			return
		}
	}
	// 这里是etcd/etcd代理的入口
	startEtcdOrProxyV2()
}

func notifySystemd(lg *zap.Logger) {
	_, err := daemon.SdNotify(false, daemon.SdNotifyReady)
	if err != nil {
		if lg != nil {
			lg.Error("failed to notify systemd for readiness", zap.Error(err))
		} else {
			plog.Errorf("failed to notify systemd for readiness: %v", err)
		}
	}
}
