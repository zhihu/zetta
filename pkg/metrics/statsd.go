// Copyright 2020 Zhizhesihai (Beijing) Technology Limited.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	"fmt"
	"log"

	"gopkg.in/alexcesaro/statsd.v2"
)

//For quick debug, change to prometheus later.
var (
	unit   = "zetta"
	cli    *statsd.Client
	prefix = fmt.Sprintf("zetta.server.%s", unit)
)

func init() {
	var err error
	cli, err = statsd.New(statsd.Address("localhost:8126"))
	if err != nil {
		log.Println("Error: statsd init:", err)
	}
}

func MetricCount(op string) {
	m := fmt.Sprintf("%s.%s.count", prefix, op)
	cli.Increment(m)
}

func MetricCountError(op string) {
	m := fmt.Sprintf("%s.%s.count.err", prefix, op)
	cli.Increment(m)
}

func MetricStartTiming() statsd.Timing {
	return cli.NewTiming()
}

func MetricRecordTiming(t statsd.Timing, op string) {
	m := fmt.Sprintf("%s.%s", prefix, op)
	cli.Timing(m, t.Duration().Seconds()*1000)
}
