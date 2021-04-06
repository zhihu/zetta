module github.com/zhihu/zetta

go 1.13

require (
	cloud.google.com/go v0.50.0
	github.com/apache/thrift v0.13.0
	github.com/blacktear23/go-proxyprotocol v0.0.0-20180807104634-af7a81e8dd0d
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548
	github.com/gogo/protobuf v1.3.1
	github.com/google/uuid v1.1.1
	github.com/gorilla/mux v1.7.4
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.1-0.20190118093823-f849b5445de4
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/klauspost/cpuid v1.2.1
	github.com/ngaut/pools v0.0.0-20180318154953-b7bc8c42aac7
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20201126102027-b0a155152ca3
	github.com/pingcap/failpoint v0.0.0-20200702092429-9f69995143ce
	github.com/pingcap/kvproto v0.0.0-20200907074027-32a3a0accf7d
	github.com/pingcap/log v0.0.0-20200511115504-543df19646ad
	github.com/pingcap/parser v0.0.0-20200803072748-fdf66528323d
	github.com/pingcap/tidb v1.1.0-beta.0.20200826081922-9c1c21270001
	github.com/pingcap/tidb-tools v4.0.1-0.20200530144555-cdec43635625+incompatible
	github.com/pingcap/tipb v0.0.0-20200522051215-f31a15d98fce
	github.com/pingcap/pd/v4 v4.0.5-0.20200817114353-e465cafe8a91
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.5.1
	github.com/shirou/gopsutil v2.19.11+incompatible // indirect
	github.com/spf13/cast v1.3.0
	github.com/spf13/viper v1.7.0
	github.com/tiancaiamao/appdash v0.0.0-20181126055449-889f96f722a2
	// github.com/tikv/pd v1.1.0-beta.0.20210128094944-96efd6f40236
	github.com/uber/jaeger-client-go v2.22.1+incompatible
	github.com/zhihu/zetta-proto v0.0.0-20210404125403-0511ff71c1a1
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738
	go.uber.org/multierr v1.5.0
	go.uber.org/zap v1.15.0
	golang.org/x/text v0.3.3
	golang.org/x/tools v0.0.0-20200820010801-b793a1359eac
	google.golang.org/grpc v1.26.0
	sourcegraph.com/sourcegraph/appdash-data v0.0.0-20151005221446-73f23eafcf67

)

replace github.com/coreos/go-systemd => github.com/coreos/go-systemd/v22 v22.0.0
