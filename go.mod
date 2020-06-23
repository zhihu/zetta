module github.com/zhihu/zetta

go 1.13

require (
	cloud.google.com/go v0.50.0
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548
	github.com/gogo/protobuf v1.3.1
	github.com/google/uuid v1.1.1
	github.com/gorilla/mux v1.7.3
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.1-0.20190118093823-f849b5445de4
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/klauspost/cpuid v1.2.0
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20190809092503-95897b64e011
	github.com/pingcap/failpoint v0.0.0-20200506114213-c17f16071c53
	github.com/pingcap/log v0.0.0-20200511115504-543df19646ad
	github.com/pingcap/parser v0.0.0-20200525110646-f45c2cee1dca
	github.com/pingcap/tidb v1.1.0-beta.0.20200526100040-689a6b6439ae
	github.com/pingcap/tidb-tools v4.0.0-rc.1.0.20200514040632-f76b3e428e19+incompatible
	github.com/pingcap/tipb v0.0.0-20200417094153-7316d94df1ee
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.5.1
	github.com/shirou/gopsutil v2.19.11+incompatible // indirect
	github.com/spf13/viper v1.7.0
	github.com/tiancaiamao/appdash v0.0.0-20181126055449-889f96f722a2
	github.com/uber/jaeger-client-go v2.22.1+incompatible
	github.com/zhihu/zetta-proto v0.0.0-20200602094047-43d62f6b0dc7
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738
	go.uber.org/zap v1.15.0
	google.golang.org/grpc v1.26.0
	sourcegraph.com/sourcegraph/appdash-data v0.0.0-20151005221446-73f23eafcf67

)

replace github.com/coreos/go-systemd => github.com/coreos/go-systemd/v22 v22.0.0
