package vcr

import (
	"io/ioutil"
	"strings"

	"bytes"
	"encoding/json"

	"github.com/spiderorg/bd-video-sdk/auth"
	"github.com/spiderorg/bd-video-sdk/httplib"
)

const (
	DefaultLocation   = "bj"
	DefaultAPIVersion = "v1"
	DefaultHost       = "vcr.bj.baidubce.com"
	Service           = "vcr"
)

type VcrClient struct {
	httplib.Client
}
type BosQuery struct {
	Source       string `bson:"source" json:"source"`
	Notification string `bson:"notification" json:"notification"`
	Description  string `bson:"description" json:"description"`
	Preset       string `bson:"preset" json:"preset"`
}

// For text audit request from baidu
type TextAudit struct {
	Text string `json:"text"`
}

func NewVcrClient(credential *auth.BceCredentials) (*VcrClient, error) {
	return &VcrClient{
		httplib.Client{
			Credential: credential,
			Location:   DefaultLocation,
			APIVersion: DefaultAPIVersion,
			Debug:      false,
			Host:       DefaultHost,
			Service:    Service,
		}}, nil
}

func (c *VcrClient) AuditVodMedia(mediaId string, preset string, notification string) (err error) {
	query := []string{}
	if preset != "" {
		query = append(query, "preset="+preset)
	}
	if notification != "" {
		query = append(query, "notification="+notification) // 456.34.57.90:4567/api/vedio/audit/callback
	}
	req := &httplib.Request{
		Method:  httplib.PUT,
		Path:    c.APIVersion + "/media/" + mediaId,
		Query:   strings.Join(query, "&"),
		Headers: map[string]string{},
	}
	_, err = c.DoRequest(req)
	return
}

// bucket : bos BucketName
// source : video URL
// videoId : video id
// notification : callback url
// preset : Template name : qupost or quduopai
func (c *VcrClient) AuditBosMedia(bucket string, source string, videoId string, notification string, preset string) (err error) {

	var s BosQuery = BosQuery{Source: source, Notification: notification, Description: videoId, Preset: preset}
	b, err := json.Marshal(s)
	if err != nil {
		return
	}

	body := bytes.NewReader([]byte(b))
	req := &httplib.Request{
		Method:  httplib.PUT,
		Path:    c.APIVersion + "/media",
		Body:    body,
		Headers: map[string]string{"content-type": "application/json"},
	}
	_, err = c.DoRequest(req)
	return
}

type AuditItemEvidenceLocation struct {
	LeftOffsetInPixel int
	TopOffsetInPixel  int
	WidthInPixel      int
	HeightInPixel     int
}

type AuditItemEvidence struct {
	thumbnail string
	Location  AuditItemEvidenceLocation
	Text      string
}

type AuditResultOneItem struct {
	Target        string
	TimeInSeconds int
	Confidence    float32
	Label         string
	Extra         string
}

type AuditOneResult struct {
	Type string
}

type ResponseError struct {
	Code    string
	Message string
}

type AuditResponse struct {
	MediaId    string
	Status     string
	Percent    int
	CreateTime string
	FinishTime string
	Label      string
	Results    []AuditOneResult
	Error      ResponseError
}

func (c *VcrClient) QueryAuditVodMediaResult(mediaId string) (response string, err error) {
	req := &httplib.Request{
		Method:  httplib.GET,
		Headers: map[string]string{},
		Path:    c.APIVersion + "/media/" + mediaId,
	}

	res, err := c.DoRequest(req)
	if err != nil {
		return "", err
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func (c *VcrClient) AuditText(text *TextAudit) (response string, err error) {
	req := &httplib.Request{
		Method:  httplib.PUT,
		Headers: map[string]string{},
		Path:    c.APIVersion + "/text",
	}

	reqSlice, err := json.Marshal(text)
	req.Body = bytes.NewReader(reqSlice)
	req.Type = httplib.JSON

	res, err := c.DoRequest(req)
	if err != nil {
		return "", err
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}
