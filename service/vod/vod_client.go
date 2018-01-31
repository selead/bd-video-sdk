package vod

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"strings"

	"github.com/spiderorg/bd-video-sdk/auth"
	"github.com/spiderorg/bd-video-sdk/httplib"
)

const (
	DefaultLocation   = "bj"
	DefaultAPIVersion = "v1"
	DefaultHost       = "vod.bj.baidubce.com"
	Service           = "vod"
)

type VodClient struct {
	httplib.Client
}

func NewVodClient(credentials *auth.BceCredentials) (*VodClient, error) {
	return &VodClient{
		httplib.Client{
			Credential: credentials,
			Location:   DefaultLocation,
			APIVersion: DefaultAPIVersion,
			Debug:      false,
			Host:       DefaultHost,
			Service:    Service,
		},
	}, nil
}

type ApplyMediaResponse struct {
	MediaId      string `bson:"mediaId" json:"mediaId"`
	SourceBucket string `bson:"sourceBucket" json:"sourceBucket"`
	SourceKey    string `bson:"sourceKey" json:"sourceKey"`
	Host         string `bson:"host" json:"host"`
}

func (c *VodClient) ApplyMedia() (r *ApplyMediaResponse, err error) {
	req := &httplib.Request{
		Method:  httplib.POST,
		Headers: map[string]string{},
		Path:    c.APIVersion + "/media",
		Query:   "apply&mode=no_transcoding",
	}

	res, err := c.DoRequest(req)
	if err != nil {
		return r, err
	}

	var response ApplyMediaResponse
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return r, err
	}

	j := json.NewDecoder(strings.NewReader(string(body)))
	j.Decode(&response)
	return &response, nil
}

type ProcessMediaRequest struct {
	Title                      string `json:"title"`
	Description                string `json:"description"`
	SourceExtension            string `json:"sourceExtension"`
	TranscodingPresetGroupName string `json:"transcodingPresetGroupName"`
}

type MediaId struct {
	MediaId string
}

func (c *VodClient) ProcessMedia(mediaId string, request ProcessMediaRequest) (response string, err error) {
	req := &httplib.Request{
		Method:  httplib.PUT,
		Headers: map[string]string{},
		Path:    c.APIVersion + "/media/" + mediaId,
		Query:   "process",
	}

	jstring, err := json.Marshal(request)
	req.Body = bytes.NewReader(jstring)
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

func (c *VodClient) Get(mediaId string) (response string, err error) {
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
