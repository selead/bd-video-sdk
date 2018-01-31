package vodpro

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
	DefaultHost       = "vodpro.bj.baidubce.com"
	Service           = "vodpro"
)

type VodproClient struct {
	httplib.Client
}

func NewVodproClient(credentials *auth.BceCredentials) (VodproClient, error) {
	return VodproClient{
		httplib.Client{
			Credential: credentials,
			Location:   DefaultLocation,
			APIVersion: DefaultAPIVersion,
			Debug:      false,
			Host:       DefaultHost,
			Service:    Service,
		}}, nil
}

type CreateMediaRequest struct {
	Path             string `json:"path"`
	NotificationName string `json:"notificationName"`
	TriggerName      string `json:"triggerName"`
}

type MediaTag struct {
	Album       string
	AlbumArtist string
	Artist      string
	Composer    string
	Genre       string
	Rotate      string
	Title       string
	Track       string
}

type AudioMeta struct {
	BitRateInKbps  int32
	Channels       int32
	CodecId        int32
	CodecName      string
	Index          int32
	SampleRateInHz int32
}

type VideoMeta struct {
	BitRateInKbps int32
	CodecId       int32
	CodecName     string
	FrameRate     float32
	HeightInPixel int32
	Index         int32
	WidthInPixel  int32
}

type MediaMeta struct {
	BitRateInBps      float32
	DurationInSecond  float32
	FileSizeInByte    uint64
	Format            string
	FormatLongName    string
	StartTimeInSecond uint32
	Type              string
	Video             VideoMeta
	Audio             AudioMeta
	Tag               MediaTag
}

type CreateMediaResponse struct {
	Path string
	Meta MediaMeta
}

func (c *VodproClient) CreateMedia(project string, space string, request CreateMediaRequest) (response CreateMediaResponse, err error) {
	req := &httplib.Request{
		Method:  httplib.POST,
		Headers: map[string]string{},
		Path:    c.APIVersion + "/project/" + project + "/space/" + space + "/media",
	}

	jstring, err := json.Marshal(request)
	req.Body = bytes.NewReader(jstring)
	req.Type = httplib.JSON

	res, err := c.DoRequest(req)
	if err != nil {
		return
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return
	}

	j := json.NewDecoder(strings.NewReader(string(body)))
	j.Decode(&response)

	return
}
