package cmd

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"

	"golang.org/x/net/http2"
)

type HTTPClientSettings struct {
	Connect               time.Duration
	ConnKeepAlive         time.Duration
	ExpectContinue        time.Duration
	IdleConn              time.Duration
	MaxAllIdleConns       int
	MaxHostIdleConns      int
	ResponseHeader        time.Duration
	TLSHandshake          time.Duration
	TLSInsecureSkipVerify bool
}

func NewHTTPClientWithSettings(httpSettings HTTPClientSettings) (*http.Client, error) {
	var client http.Client
	tr := &http.Transport{
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: httpSettings.TLSInsecureSkipVerify},
		ResponseHeaderTimeout: httpSettings.ResponseHeader,
		Proxy:                 http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			KeepAlive: httpSettings.ConnKeepAlive,
			DualStack: true,
			Timeout:   httpSettings.Connect,
		}).DialContext,
		MaxIdleConns:          httpSettings.MaxAllIdleConns,
		IdleConnTimeout:       httpSettings.IdleConn,
		TLSHandshakeTimeout:   httpSettings.TLSHandshake,
		MaxIdleConnsPerHost:   httpSettings.MaxHostIdleConns,
		ExpectContinueTimeout: httpSettings.ExpectContinue,
	}

	// So client makes HTTP/2 requests
	err := http2.ConfigureTransport(tr)
	if err != nil {
		return &client, err
	}

	return &http.Client{
		Transport: tr,
	}, nil
}
