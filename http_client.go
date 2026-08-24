package flow

import (
	"bytes"
	"crypto/tls"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"time"
)

// HTTPClientElement maps all attributes from the HttpClientType XML schema[cite: 1].
type HTTPClientElement struct {
	XMLName xml.Name `xml:"HTTP_CLIENT"`

	// Core Request Attributes
	ID          string `xml:"id,attr"`
	URI         string `xml:"uri,attr"`
	URL         string `xml:"url,attr"`
	Method      string `xml:"method,attr"`
	Data        string `xml:"data,attr"`
	BodyContent string `xml:",chardata"`
	Headers     string `xml:"headers,attr"`
	ContentType string `xml:"content_type,attr"`

	// Variable Output Assignments
	Var            string `xml:"var,attr"`
	Variable       string `xml:"variable,attr"`
	OutputVar      string `xml:"output_var,attr"`
	OutputVariable string `xml:"output_variable,attr"`
	OutVar         string `xml:"out_var,attr"`

	StatusCodeVar      string `xml:"status_code_var,attr"`
	StatusCodeVariable string `xml:"status_code_variable,attr"`
	StatusVar          string `xml:"status_var,attr"`
	StatusVariable     string `xml:"status_variable,attr"`

	// http.Client Attributes
	Timeout         string `xml:"timeout,attr"`
	MaxRedirects    *int   `xml:"max_redirects,attr"`
	FollowRedirects *bool  `xml:"follow_redirects,attr"`
	CookieJar       *bool  `xml:"cookie_jar,attr"`

	// http.Transport Attributes
	Proxy                  string `xml:"proxy,attr"`
	TLSInsecureSkipVerify  *bool  `xml:"tls_insecure_skip_verify,attr"`
	TLSHandshakeTimeout    string `xml:"tls_handshake_timeout,attr"`
	TLSServerName          string `xml:"tls_server_name,attr"`
	TLSMinVersion          string `xml:"tls_min_version,attr"`
	TLSMaxVersion          string `xml:"tls_max_version,attr"`
	DisableKeepAlives      *bool  `xml:"disable_keep_alives,attr"`
	DisableCompression     *bool  `xml:"disable_compression,attr"`
	MaxIdleConns           *int   `xml:"max_idle_conns,attr"`
	MaxIdleConnsPerHost    *int   `xml:"max_idle_conns_per_host,attr"`
	MaxConnsPerHost        *int   `xml:"max_conns_per_host,attr"`
	IdleConnTimeout        string `xml:"idle_conn_timeout,attr"`
	ResponseHeaderTimeout  string `xml:"response_header_timeout,attr"`
	ExpectContinueTimeout  string `xml:"expect_continue_timeout,attr"`
	MaxResponseHeaderBytes *int64 `xml:"max_response_header_bytes,attr"`
	WriteBufferSize        *int   `xml:"write_buffer_size,attr"`
	ReadBufferSize         *int   `xml:"read_buffer_size,attr"`
	ForceAttemptHTTP2      *bool  `xml:"force_attempt_http2,attr"`
}

// BuildClientAndRequest constructs fully configured http.Client and http.Request instances.
func BuildClientAndRequest(elem HTTPClientElement) (*http.Client, *http.Request, error) {
	// 1. Configure Transport
	transport := &http.Transport{}

	if elem.Proxy != "" {
		proxyURL, err := url.Parse(elem.Proxy)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid proxy URL: %w", err)
		}
		transport.Proxy = http.ProxyURL(proxyURL)
	}

	tlsConfig := &tls.Config{}
	hasTLS := false

	if elem.TLSInsecureSkipVerify != nil {
		tlsConfig.InsecureSkipVerify = *elem.TLSInsecureSkipVerify
		hasTLS = true
	}
	if elem.TLSServerName != "" {
		tlsConfig.ServerName = elem.TLSServerName
		hasTLS = true
	}
	if elem.TLSMinVersion != "" {
		tlsConfig.MinVersion = parseTLSVersion(elem.TLSMinVersion)
		hasTLS = true
	}
	if elem.TLSMaxVersion != "" {
		tlsConfig.MaxVersion = parseTLSVersion(elem.TLSMaxVersion)
		hasTLS = true
	}
	if hasTLS {
		transport.TLSClientConfig = tlsConfig
	}

	if elem.TLSHandshakeTimeout != "" {
		d, err := time.ParseDuration(elem.TLSHandshakeTimeout)
		if err == nil {
			transport.TLSHandshakeTimeout = d
		}
	}
	if elem.DisableKeepAlives != nil {
		transport.DisableKeepAlives = *elem.DisableKeepAlives
	}
	if elem.DisableCompression != nil {
		transport.DisableCompression = *elem.DisableCompression
	}
	if elem.MaxIdleConns != nil {
		transport.MaxIdleConns = *elem.MaxIdleConns
	}
	if elem.MaxIdleConnsPerHost != nil {
		transport.MaxIdleConnsPerHost = *elem.MaxIdleConnsPerHost
	}
	if elem.MaxConnsPerHost != nil {
		transport.MaxConnsPerHost = *elem.MaxConnsPerHost
	}
	if elem.IdleConnTimeout != "" {
		d, err := time.ParseDuration(elem.IdleConnTimeout)
		if err == nil {
			transport.IdleConnTimeout = d
		}
	}
	if elem.ResponseHeaderTimeout != "" {
		d, err := time.ParseDuration(elem.ResponseHeaderTimeout)
		if err == nil {
			transport.ResponseHeaderTimeout = d
		}
	}
	if elem.ExpectContinueTimeout != "" {
		d, err := time.ParseDuration(elem.ExpectContinueTimeout)
		if err == nil {
			transport.ExpectContinueTimeout = d
		}
	}
	if elem.MaxResponseHeaderBytes != nil {
		transport.MaxResponseHeaderBytes = *elem.MaxResponseHeaderBytes
	}
	if elem.WriteBufferSize != nil {
		transport.WriteBufferSize = *elem.WriteBufferSize
	}
	if elem.ReadBufferSize != nil {
		transport.ReadBufferSize = *elem.ReadBufferSize
	}
	if elem.ForceAttemptHTTP2 != nil {
		transport.ForceAttemptHTTP2 = *elem.ForceAttemptHTTP2
	}

	// 2. Configure Client
	client := &http.Client{
		Transport: transport,
	}

	if elem.Timeout != "" {
		d, err := time.ParseDuration(elem.Timeout)
		if err == nil {
			client.Timeout = d
		}
	}

	if elem.CookieJar != nil && *elem.CookieJar {
		jar, err := cookiejar.New(nil)
		if err == nil {
			client.Jar = jar
		}
	}

	// Handle redirects
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if elem.FollowRedirects != nil && !*elem.FollowRedirects {
			return http.ErrUseLastResponse
		}
		if elem.MaxRedirects != nil && len(via) >= *elem.MaxRedirects {
			return errors.New("stopped after max redirects limit")
		}
		return nil
	}

	// 3. Configure Request
	targetURL := elem.URI
	if targetURL == "" {
		targetURL = elem.URL
	}

	method := strings.ToUpper(elem.Method)
	if method == "" {
		method = http.MethodGet
	}

	var bodyReader io.Reader
	bodyData := elem.Data
	if bodyData == "" {
		bodyData = strings.TrimSpace(elem.BodyContent)
	}
	if bodyData != "" {
		bodyReader = bytes.NewBufferString(bodyData)
	}

	req, err := http.NewRequest(method, targetURL, bodyReader)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	if elem.ContentType != "" {
		req.Header.Set("Content-Type", elem.ContentType)
	}

	// Parse custom header string (formatted as "K1:V1,K2:V2" or line-separated "K1:V1\nK2:V2")
	if elem.Headers != "" {
		pairs := strings.FieldsFunc(elem.Headers, func(r rune) bool {
			return r == ',' || r == '\n' || r == ';'
		})
		for _, pair := range pairs {
			kv := strings.SplitN(pair, ":", 2)
			if len(kv) == 2 {
				req.Header.Add(strings.TrimSpace(kv[0]), strings.TrimSpace(kv[1]))
			}
		}
	}

	return client, req, nil
}

// Helpers
func parseTLSVersion(v string) uint16 {
	switch strings.ToLower(v) {
	case "1.0", "tls1.0", "tls10":
		return tls.VersionTLS10
	case "1.1", "tls1.1", "tls11":
		return tls.VersionTLS11
	case "1.2", "tls1.2", "tls12":
		return tls.VersionTLS12
	case "1.3", "tls1.3", "tls13":
		return tls.VersionTLS13
	default:
		return tls.VersionTLS12
	}
}

// Helper methods to identify target output variables
func (e *HTTPClientElement) GetOutputVariable() string {
	for _, v := range []string{e.Var, e.Variable, e.OutputVar, e.OutputVariable, e.OutVar} {
		if v != "" {
			return v
		}
	}
	return ""
}

func (e *HTTPClientElement) GetStatusCodeVariable() string {
	for _, v := range []string{e.StatusCodeVar, e.StatusCodeVariable, e.StatusVar, e.StatusVariable} {
		if v != "" {
			return v
		}
	}
	return ""
}
