module github.com/zhihu/zetta

go 1.13

require (
	cloud.google.com/go v0.46.3
	github.com/StackExchange/wmi v0.0.0-20190523213315-cbe66965904d // indirect
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548
	github.com/go-ole/go-ole v1.2.4 // indirect
	github.com/gogo/protobuf v1.3.1
	github.com/google/uuid v1.1.1
	github.com/gorilla/mux v1.6.2
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.1-0.20190118093823-f849b5445de4
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/klauspost/cpuid v0.0.0-20170728055534-ae7887de9fa5
	github.com/opentracing/opentracing-go v1.0.2
	github.com/pingcap/check v0.0.0-20190102082844-67f458068fc8
	github.com/pingcap/errors v0.11.4
	github.com/pingcap/failpoint v0.0.0-20190512135322-30cc7431d99c
	github.com/pingcap/log v0.0.0-20190715063458-479153f07ebd
	github.com/pingcap/parser v0.0.0-20191120072812-9dc33a611210
	github.com/pingcap/tidb v1.1.0-beta.0.20191122095600-76f0386c5a17
	github.com/pingcap/tidb-tools v3.0.6-0.20191119150227-ff0a3c6e5763+incompatible
	github.com/pingcap/tipb v0.0.0-20191120045257-1b9900292ab6
	github.com/pkg/errors v0.8.1
	github.com/prometheus/client_golang v0.9.3
	github.com/shirou/gopsutil v2.19.11+incompatible // indirect
	github.com/spf13/viper v1.7.0
	github.com/stretchr/testify v1.4.0 // indirect
	github.com/tiancaiamao/appdash v0.0.0-20181126055449-889f96f722a2
	github.com/uber/jaeger-client-go v2.15.0+incompatible
	github.com/zhihu/zetta-proto v0.0.0-20200801112337-dd6ec18688aa
	go.etcd.io/etcd v0.0.0-20190320044326-77d4b742cdbf
	go.uber.org/zap v1.10.0
	golang.org/x/sys v0.0.0-20191220220014-0732a990476f // indirect
	google.golang.org/grpc v1.25.1
	gopkg.in/alexcesaro/statsd.v2 v2.0.0
	sourcegraph.com/sourcegraph/appdash-data v0.0.0-20151005221446-73f23eafcf67

)

replace github.com/coreos/go-systemd => github.com/coreos/go-systemd/v22 v22.0.0
