package pdhttp

import (
	"bytes"
	"encoding/json"
	"github.com/zhihu/zetta/tablestore/config"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/pingcap/errors"
	"go.etcd.io/etcd/pkg/transport"
)

var (
	dialClient = &http.Client{}
	pingPrefix = "pd/api/v1/ping"
)

// InitHTTPSClient creates https client with ca file
func InitHTTPSClient(CAPath, CertPath, KeyPath string) error {
	tlsInfo := transport.TLSInfo{
		CertFile:      CertPath,
		KeyFile:       KeyPath,
		TrustedCAFile: CAPath,
	}
	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		return errors.WithStack(err)
	}

	dialClient = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}

	return nil
}

type bodyOption struct {
	contentType string
	body        io.Reader
}

// BodyOption sets the type and content of the body
type BodyOption func(*bodyOption)

// WithBody returns a BodyOption
func WithBody(contentType string, body io.Reader) BodyOption {
	return func(bo *bodyOption) {
		bo.contentType = contentType
		bo.body = body
	}
}

func doRequest(prefix string, method string,
	opts ...BodyOption) (string, error) {
	b := &bodyOption{}
	for _, o := range opts {
		o(b)
	}
	var resp string

	endpoints := getEndpoints()
	err := tryURLs(endpoints, func(endpoint string) error {
		var err error
		url := endpoint + "/" + prefix
		if method == "" {
			method = http.MethodGet
		}
		var req *http.Request

		req, err = http.NewRequest(method, url, b.body)
		if err != nil {
			return err
		}
		if b.contentType != "" {
			req.Header.Set("Content-Type", b.contentType)
		}
		// the resp would be returned by the outer function
		resp, err = dial(req)
		if err != nil {
			return err
		}
		return nil
	})
	return resp, err
}

func dial(req *http.Request) (string, error) {
	resp, err := dialClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var msg []byte
		msg, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return "", err
		}
		return "", errors.Errorf("[%d] %s", resp.StatusCode, msg)
	}

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

// DoFunc receives an endpoint which you can issue request to
type DoFunc func(endpoint string) error

// tryURLs issues requests to each URL and tries next one if there
// is an error
func tryURLs(endpoints []string, f DoFunc) error {
	var err error
	for _, endpoint := range endpoints {
		var u *url.URL
		u, err = url.Parse(endpoint)
		if err != nil {
			return err
		}
		// tolerate some schemes that will be used by users, the TiKV SDK
		// use 'tikv' as the scheme, it is really confused if we do not
		// support it by pd-ctl
		if u.Scheme == "" || u.Scheme == "pd" || u.Scheme == "tikv" {
			u.Scheme = "http"
		}

		endpoint = u.String()
		err = f(endpoint)
		if err != nil {
			continue
		}
		break
	}
	if len(endpoints) > 1 && err != nil {
		err = errors.Errorf("after trying all endpoints, no endpoint is available, the last error we met: %s", err)
	}
	return err
}

func getEndpoints() []string {
	addrs := config.GetGlobalConfig().Path
	eps := strings.Split(addrs, ",")
	for i, ep := range eps {
		if j := strings.Index(ep, "//"); j == -1 {
			eps[i] = "//" + ep
		}
	}
	return eps
}

func postJSON(prefix string, input map[string]interface{}) error {
	data, err := json.Marshal(input)
	if err != nil {
		return err
	}

	endpoints := getEndpoints()
	err = tryURLs(endpoints, func(endpoint string) error {
		var msg []byte
		var r *http.Response
		url := endpoint + "/" + prefix
		r, err = dialClient.Post(url, "application/json", bytes.NewBuffer(data))
		if err != nil {
			return err
		}
		defer r.Body.Close()
		if r.StatusCode != http.StatusOK {
			msg, err = ioutil.ReadAll(r.Body)
			if err != nil {
				return err
			}
			return errors.Errorf("[%d] %s", r.StatusCode, msg)
		}
		return nil
	})
	return err
}
