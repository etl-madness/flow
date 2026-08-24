package flow

import (
	"crypto/tls"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestGetOutputVariable(t *testing.T) {
	tests := []struct {
		name     string
		elem     HTTPClientElement
		expected string
	}{
		{
			name: "Var only",
			elem: HTTPClientElement{
				Var: "v1",
			},
			expected: "v1",
		},
		{
			name: "Var priority over Variable",
			elem: HTTPClientElement{
				Var:      "v1",
				Variable: "v2",
			},
			expected: "v1",
		},
		{
			name: "Variable priority over OutputVar",
			elem: HTTPClientElement{
				Variable:  "v2",
				OutputVar: "v3",
			},
			expected: "v2",
		},
		{
			name: "OutputVar priority over OutputVariable",
			elem: HTTPClientElement{
				OutputVar:      "v3",
				OutputVariable: "v4",
			},
			expected: "v3",
		},
		{
			name: "OutputVariable priority over OutVar",
			elem: HTTPClientElement{
				OutputVariable: "v4",
				OutVar:         "v5",
			},
			expected: "v4",
		},
		{
			name: "OutVar only",
			elem: HTTPClientElement{
				OutVar: "v5",
			},
			expected: "v5",
		},
		{
			name:     "Empty",
			elem:     HTTPClientElement{},
			expected: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.elem.GetOutputVariable()
			if got != tc.expected {
				t.Errorf("GetOutputVariable() = %q, want %q", got, tc.expected)
			}
		})
	}
}

func TestGetStatusCodeVariable(t *testing.T) {
	tests := []struct {
		name     string
		elem     HTTPClientElement
		expected string
	}{
		{
			name: "StatusCodeVar only",
			elem: HTTPClientElement{
				StatusCodeVar: "sc1",
			},
			expected: "sc1",
		},
		{
			name: "StatusCodeVar priority over StatusCodeVariable",
			elem: HTTPClientElement{
				StatusCodeVar:      "sc1",
				StatusCodeVariable: "sc2",
			},
			expected: "sc1",
		},
		{
			name: "StatusCodeVariable priority over StatusVar",
			elem: HTTPClientElement{
				StatusCodeVariable: "sc2",
				StatusVar:          "sc3",
			},
			expected: "sc2",
		},
		{
			name: "StatusVar priority over StatusVariable",
			elem: HTTPClientElement{
				StatusVar:      "sc3",
				StatusVariable: "sc4",
			},
			expected: "sc3",
		},
		{
			name: "StatusVariable only",
			elem: HTTPClientElement{
				StatusVariable: "sc4",
			},
			expected: "sc4",
		},
		{
			name:     "Empty",
			elem:     HTTPClientElement{},
			expected: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.elem.GetStatusCodeVariable()
			if got != tc.expected {
				t.Errorf("GetStatusCodeVariable() = %q, want %q", got, tc.expected)
			}
		})
	}
}

func TestBuildClientAndRequest_Basic(t *testing.T) {
	// 1. URI vs URL priority (URI takes preference)
	elem := HTTPClientElement{
		URI: "https://example.com/uri",
		URL: "https://example.com/url",
	}
	_, req, err := BuildClientAndRequest(elem)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.URL.String() != "https://example.com/uri" {
		t.Errorf("expected URL to be https://example.com/uri, got %s", req.URL.String())
	}

	// 2. URL fallback when URI is empty
	elemOnlyURL := HTTPClientElement{
		URL: "https://example.com/url",
	}
	_, reqOnlyURL, err := BuildClientAndRequest(elemOnlyURL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reqOnlyURL.URL.String() != "https://example.com/url" {
		t.Errorf("expected URL to be https://example.com/url, got %s", reqOnlyURL.URL.String())
	}

	// 3. Default HTTP Method is GET
	if req.Method != "GET" {
		t.Errorf("expected default method to be GET, got %s", req.Method)
	}
}

func TestBuildClientAndRequest_MethodAndBody(t *testing.T) {
	// Case insensitivity of method
	elem := HTTPClientElement{
		URI:    "https://example.com",
		Method: "post",
	}
	_, req, err := BuildClientAndRequest(elem)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Method != "POST" {
		t.Errorf("expected method to be capitalized to POST, got %s", req.Method)
	}

	// Body content: Data attribute is preferred over BodyContent (chardata)
	elemBody := HTTPClientElement{
		URI:         "https://example.com",
		Method:      "POST",
		Data:        "data-body",
		BodyContent: "chardata-body",
	}
	_, reqBody, err := BuildClientAndRequest(elemBody)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	bodyBytes, _ := io.ReadAll(reqBody.Body)
	if string(bodyBytes) != "data-body" {
		t.Errorf("expected body to be data-body, got %s", string(bodyBytes))
	}

	// Fallback to BodyContent if Data is empty
	elemCharBody := HTTPClientElement{
		URI:         "https://example.com",
		Method:      "POST",
		BodyContent: "  chardata-body  ",
	}
	_, reqCharBody, err := BuildClientAndRequest(elemCharBody)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	bodyBytesChar, _ := io.ReadAll(reqCharBody.Body)
	if string(bodyBytesChar) != "chardata-body" {
		t.Errorf("expected body to be trimmed chardata-body, got %q", string(bodyBytesChar))
	}
}

func TestBuildClientAndRequest_Headers(t *testing.T) {
	// ContentType attribute sets Content-Type header
	elem := HTTPClientElement{
		URI:         "https://example.com",
		ContentType: "application/json",
		Headers:     "X-Custom:Val1,X-Another:Val2\nX-Newline:Val3;X-Semicolon:Val4",
	}
	_, req, err := BuildClientAndRequest(elem)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if req.Header.Get("Content-Type") != "application/json" {
		t.Errorf("expected Content-Type application/json, got %s", req.Header.Get("Content-Type"))
	}
	if req.Header.Get("X-Custom") != "Val1" {
		t.Errorf("expected X-Custom to be Val1, got %s", req.Header.Get("X-Custom"))
	}
	if req.Header.Get("X-Another") != "Val2" {
		t.Errorf("expected X-Another to be Val2, got %s", req.Header.Get("X-Another"))
	}
	if req.Header.Get("X-Newline") != "Val3" {
		t.Errorf("expected X-Newline to be Val3, got %s", req.Header.Get("X-Newline"))
	}
	if req.Header.Get("X-Semicolon") != "Val4" {
		t.Errorf("expected X-Semicolon to be Val4, got %s", req.Header.Get("X-Semicolon"))
	}
}

func TestBuildClientAndRequest_ClientOptions(t *testing.T) {
	// Timeout, CookieJar, Redirects
	followRedir := false
	maxRedir := 5
	cookieJar := true

	elem := HTTPClientElement{
		URI:             "https://example.com",
		Timeout:         "10s",
		FollowRedirects: &followRedir,
		MaxRedirects:    &maxRedir,
		CookieJar:       &cookieJar,
	}

	client, req, err := BuildClientAndRequest(elem)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if client.Timeout != 10*time.Second {
		t.Errorf("expected client timeout to be 10s, got %v", client.Timeout)
	}

	if client.Jar == nil {
		t.Error("expected client to have a cookie jar")
	}

	// Test redirect handler
	errRedir := client.CheckRedirect(req, nil)
	if errRedir != http.ErrUseLastResponse {
		t.Errorf("expected redirect handler to return ErrUseLastResponse when FollowRedirects is false, got %v", errRedir)
	}

	// Test max redirects limit when FollowRedirects is true
	followRedirTrue := true
	elemRedirLimit := HTTPClientElement{
		URI:             "https://example.com",
		FollowRedirects: &followRedirTrue,
		MaxRedirects:    &maxRedir,
	}
	clientLimit, _, err := BuildClientAndRequest(elemRedirLimit)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	via := make([]*http.Request, 5)
	errLimit := clientLimit.CheckRedirect(req, via)
	if errLimit == nil || errLimit.Error() != "stopped after max redirects limit" {
		t.Errorf("expected redirect handler to stop after max redirects limit, got %v", errLimit)
	}

	viaUnderLimit := make([]*http.Request, 4)
	errUnderLimit := clientLimit.CheckRedirect(req, viaUnderLimit)
	if errUnderLimit != nil {
		t.Errorf("expected redirect handler to succeed under limit, got %v", errUnderLimit)
	}
}

func TestBuildClientAndRequest_TransportOptions(t *testing.T) {
	// Proxy configuration error
	elemErrProxy := HTTPClientElement{
		URI:   "https://example.com",
		Proxy: "invalid_scheme://[:foo",
	}
	_, _, err := BuildClientAndRequest(elemErrProxy)
	if err == nil {
		t.Error("expected error for invalid proxy URL, got nil")
	}

	// Transport options and TLS options
	skipVerify := true
	disableKeepAlives := true
	disableComp := true
	maxIdle := 10
	maxIdlePerHost := 5
	maxConnsPerHost := 20
	maxHeaderBytes := int64(4096)
	writeBufSize := 1024
	readBufSize := 2048
	forceHTTP2 := true

	elem := HTTPClientElement{
		URI:                    "https://example.com",
		Proxy:                  "http://localhost:8080",
		TLSInsecureSkipVerify:  &skipVerify,
		TLSServerName:          "test-server",
		TLSMinVersion:          "1.3",
		TLSMaxVersion:          "1.3",
		TLSHandshakeTimeout:    "5s",
		DisableKeepAlives:      &disableKeepAlives,
		DisableCompression:     &disableComp,
		MaxIdleConns:           &maxIdle,
		MaxIdleConnsPerHost:    &maxIdlePerHost,
		MaxConnsPerHost:        &maxConnsPerHost,
		IdleConnTimeout:        "30s",
		ResponseHeaderTimeout:  "15s",
		ExpectContinueTimeout:  "1s",
		MaxResponseHeaderBytes: &maxHeaderBytes,
		WriteBufferSize:        &writeBufSize,
		ReadBufferSize:         &readBufSize,
		ForceAttemptHTTP2:      &forceHTTP2,
	}

	client, _, err := BuildClientAndRequest(elem)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("expected transport to be of type *http.Transport")
	}

	// Verify Proxy URL
	pReq, _ := http.NewRequest("GET", "https://example.com", nil)
	proxyURL, err := transport.Proxy(pReq)
	if err != nil {
		t.Fatalf("failed to call Proxy: %v", err)
	}
	if proxyURL == nil || proxyURL.String() != "http://localhost:8080" {
		t.Errorf("expected proxy URL http://localhost:8080, got %v", proxyURL)
	}

	// Verify TLS client config
	if transport.TLSClientConfig == nil {
		t.Fatal("expected TLSClientConfig to be configured")
	}
	if transport.TLSClientConfig.InsecureSkipVerify != true {
		t.Errorf("expected InsecureSkipVerify true, got %t", transport.TLSClientConfig.InsecureSkipVerify)
	}
	if transport.TLSClientConfig.ServerName != "test-server" {
		t.Errorf("expected ServerName test-server, got %s", transport.TLSClientConfig.ServerName)
	}
	if transport.TLSClientConfig.MinVersion != tls.VersionTLS13 {
		t.Errorf("expected TLS min version TLS1.3, got %x", transport.TLSClientConfig.MinVersion)
	}
	if transport.TLSClientConfig.MaxVersion != tls.VersionTLS13 {
		t.Errorf("expected TLS max version TLS1.3, got %x", transport.TLSClientConfig.MaxVersion)
	}

	// Verify timeouts & attributes
	if transport.TLSHandshakeTimeout != 5*time.Second {
		t.Errorf("expected TLSHandshakeTimeout 5s, got %v", transport.TLSHandshakeTimeout)
	}
	if transport.DisableKeepAlives != true {
		t.Errorf("expected DisableKeepAlives true, got %t", transport.DisableKeepAlives)
	}
	if transport.DisableCompression != true {
		t.Errorf("expected DisableCompression true, got %t", transport.DisableCompression)
	}
	if transport.MaxIdleConns != 10 {
		t.Errorf("expected MaxIdleConns 10, got %d", transport.MaxIdleConns)
	}
	if transport.MaxIdleConnsPerHost != 5 {
		t.Errorf("expected MaxIdleConnsPerHost 5, got %d", transport.MaxIdleConnsPerHost)
	}
	if transport.MaxConnsPerHost != 20 {
		t.Errorf("expected MaxConnsPerHost 20, got %d", transport.MaxConnsPerHost)
	}
	if transport.IdleConnTimeout != 30*time.Second {
		t.Errorf("expected IdleConnTimeout 30s, got %v", transport.IdleConnTimeout)
	}
	if transport.ResponseHeaderTimeout != 15*time.Second {
		t.Errorf("expected ResponseHeaderTimeout 15s, got %v", transport.ResponseHeaderTimeout)
	}
	if transport.ExpectContinueTimeout != 1*time.Second {
		t.Errorf("expected ExpectContinueTimeout 1s, got %v", transport.ExpectContinueTimeout)
	}
	if transport.MaxResponseHeaderBytes != 4096 {
		t.Errorf("expected MaxResponseHeaderBytes 4096, got %d", transport.MaxResponseHeaderBytes)
	}
	if transport.WriteBufferSize != 1024 {
		t.Errorf("expected WriteBufferSize 1024, got %d", transport.WriteBufferSize)
	}
	if transport.ReadBufferSize != 2048 {
		t.Errorf("expected ReadBufferSize 2048, got %d", transport.ReadBufferSize)
	}
	if transport.ForceAttemptHTTP2 != true {
		t.Errorf("expected ForceAttemptHTTP2 true, got %t", transport.ForceAttemptHTTP2)
	}
}

func TestParseTLSVersion(t *testing.T) {
	tests := []struct {
		input    string
		expected uint16
	}{
		{"1.0", tls.VersionTLS10},
		{"tls1.0", tls.VersionTLS10},
		{"tls10", tls.VersionTLS10},
		{"1.1", tls.VersionTLS11},
		{"tls1.1", tls.VersionTLS11},
		{"tls11", tls.VersionTLS11},
		{"1.2", tls.VersionTLS12},
		{"tls1.2", tls.VersionTLS12},
		{"tls12", tls.VersionTLS12},
		{"1.3", tls.VersionTLS13},
		{"tls1.3", tls.VersionTLS13},
		{"tls13", tls.VersionTLS13},
		{"invalid", tls.VersionTLS12}, // default fallback
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got := parseTLSVersion(tc.input)
			if got != tc.expected {
				t.Errorf("parseTLSVersion(%q) = %x, want %x", tc.input, got, tc.expected)
			}
		})
	}
}
