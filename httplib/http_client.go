package httplib

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/spiderorg/bd-video-sdk/auth"
	"github.com/spiderorg/bd-video-sdk/utils"
)

type Client struct {
	Credential *auth.BceCredentials
	Location   string
	APIVersion string
	Host       string
	Service    string
	Debug      bool
}

//func NewClient(credential *auth.BceCredentials, location string, apiVersion string, service string,
//	host string, debug bool) (Client, error) {
//	return Client{
//		Credential: credential,
//		Location:   location,
//		APIVersion: apiVersion,
//		Debug:      debug,
//		Host:       host,
//	}, nil
//}

func (c *Client) GetBaseURL() string {
	return fmt.Sprintf("%s/%s", c.GetEndpoint(), c.APIVersion)
}

func (c *Client) GetEndpoint() string {
	return fmt.Sprintf("http://%s", c.GetHost())
}

func (c *Client) GetHost() string {
	if c.Host != "" {
		return c.Host
	}
	return fmt.Sprintf("%s.%s.baidubce.com", c.Service, c.Location)
}

type ErrorResponse struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestId string `json:"requestId"`
}

func (e *ErrorResponse) Error() string {
	return fmt.Sprintf("Service returned error: Code=%s, RequestId=%s, Message=%s", e.Code, e.RequestId, e.Message)
}

func (c *Client) DoRequest(req *Request) (*http.Response, error) {
	if req.BaseUrl == "" {
		req.BaseUrl = c.GetBaseURL()
	}
	req.Headers[HOST] = c.GetHost()

	timestamp := utils.GetHttpHeadTimeStamp()
	auth.Debug = c.Debug
	authorization := auth.Sign(c.Credential, timestamp, req.Method, req.Path, req.Query, req.Headers)

	req.Headers[auth.BCE_DATE] = timestamp
	req.Headers[AUTHORIZATION] = authorization

	Debug = c.Debug
	res, err := Run(req, nil)
	if err != nil {
		return res, err
	}

	if res.StatusCode != 200 {
		errR := &ErrorResponse{}
		if req.Method == HEAD || req.Method == DELETE {
			errR.Code = fmt.Sprintf("%d", res.StatusCode)
			errR.Message = res.Status
			errR.RequestId = "EMPTY"
		} else {
			body, err := ioutil.ReadAll(res.Body)
			if err != nil {
				return res, err
			}
			j := json.NewDecoder(strings.NewReader(string(body)))
			j.Decode(&errR)
		}
		return res, errR
	}
	return res, err
}
