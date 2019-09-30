package collector

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

type ClickhouseClient struct {
	client   *http.Client
	baseUrl  *string
	user     string
	password string
}

func NewClickhouseClient(baseUrl *string, insecure bool, user, password string) *ClickhouseClient {
	return &ClickhouseClient{
		client: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: insecure},
			},
			Timeout: 30 * time.Second,
		},
		baseUrl:  baseUrl,
		user:     user,
		password: password,
	}
}

func (e *ClickhouseClient) Request(query *string) ([]byte, error) {
	req, err := http.NewRequest("POST", *e.baseUrl, strings.NewReader(*query))
	if err != nil {
		return nil, err
	}
	if e.user != "" && e.password != "" {
		req.Header.Set("X-ClickHouse-User", e.user)
		req.Header.Set("X-ClickHouse-Key", e.password)
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error scraping clickhouse from client: %v", err)
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		if err != nil {
			data = []byte(err.Error())
		}
		return nil, fmt.Errorf("status %s (%d): %s", resp.Status, resp.StatusCode, data)
	}

	return data, nil
}
