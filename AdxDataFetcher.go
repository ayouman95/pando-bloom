package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/tencentyun/cos-go-sdk-v5"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Region string

const (
	RegionDE Region = "de"
	RegionSG Region = "sg"
	RegionUS Region = "us"
)

const (
	EpUs = "https://pando-adx-us-1374116111.cos.na-siliconvalley.myqcloud.com"
	EpDe = "https://pando-adx-de-1374116111.cos.eu-frankfurt.myqcloud.com"
	EpSg = "https://pando-adx-sg-1374116111.cos.ap-singapore.myqcloud.com"
)

var Regions = []Region{RegionDE, RegionSG, RegionUS}
var RegionEps = map[Region]string{
	RegionDE: EpDe,
	RegionSG: EpSg,
	RegionUS: EpUs,
}

const (
	RedisAddr          = "localhost:6379"
	RedisPassword      = "123456"
	RedisCountGroupKey = "ddj:num:group"
	CosSecretId        = "IKIDPXLpynHRBbgQqvf49A0VfUy7xScSx7xT"
	CpsSecretKey       = "SZLmtf6k33i33i34zarnOgfLilUu1oHY"
)

const (
	CapcutAppId = "com.lemon.lvoverseas"
)

var RedisClient *redis.Client
var CosClients = make(map[Region]*cos.Client)
var ctx = context.Background()

func InitClients() {
	// Redis
	RedisClient = redis.NewClient(&redis.Options{
		Addr:     RedisAddr,
		Password: RedisPassword,
		DB:       0,
	})

	// 为每个 region 创建 client（实际可能不同 endpoint）
	for _, r := range Regions {
		u, _ := url.Parse(RegionEps[r])
		b := &cos.BaseURL{BucketURL: u}
		client := cos.NewClient(b, &http.Client{
			Transport: &cos.AuthorizationTransport{
				SecretID:  CosSecretId,
				SecretKey: CpsSecretKey,
			},
		})
		CosClients[r] = client // 实际中根据 region 切换 endpoint
	}
}

func getLastMinute() (date, hour, minute string) {
	t := time.Now().Add(-1 * time.Minute) // 上一分钟
	return t.Format("20060102"), t.Format("15"), t.Format("04")
}

func listAndDownloadFiles(region Region, date, hour, minute string) ([]string, error) {
	prefix := fmt.Sprintf("adx_device/request/%s/%s/%s", date, hour, minute)

	client := CosClients[region]
	opt := &cos.BucketGetOptions{
		Prefix: prefix,
	}

	result, _, err := client.Bucket.Get(context.Background(), opt)
	if err != nil {
		return nil, err
	}

	var lines []string
	for _, item := range result.Contents {
		resp, err := client.Object.Get(context.Background(), item.Key, nil)
		if err != nil {
			log.Printf("下载失败 %s: %v", item.Key, err)
			continue
		}
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			lines = append(lines, scanner.Text())
		}
		resp.Body.Close()
	}

	return lines, nil
}

type AdxRequest struct {
	AdType      string  `json:"ad_type"`
	AppId       string  `json:"app_id"`
	Brand       string  `json:"brand"`
	CountryCode string  `json:"country_code"`
	DeviceId    string  `json:"deviceId"`
	DeviceType  int     `json:"deviceType"`
	Exchange    string  `json:"exchange"`
	Extra1      string  `json:"extra1"`
	Extra2      string  `json:"extra2"`
	Extra3      string  `json:"extra3"`
	Ip          string  `json:"ip"`
	Language    string  `json:"language"`
	Level       string  `json:"level"`
	Model       string  `json:"model"`
	Msg         string  `json:"msg"`
	NetworkType int     `json:"network_type"`
	OsVersion   string  `json:"os_version"`
	Platform    string  `json:"platform"`
	PosId       int     `json:"pos_id"`
	Price       float64 `json:"price"`
	PubId       PubId   `json:"pub_id"`
	Size        string  `json:"size"`
	Time        string  `json:"time"`
	Timestamp   int     `json:"timestamp"`
	UserAgent   string  `json:"user_agent"`
}

type PubId struct {
	Id string `json:"id"`
}

// AppDemand: appId → total needed
type AppDemand map[string]int

// CPAppMap: country:platform → appId set
type CPAppMap map[string]map[string]bool

type AppOfferSiteDemandMap map[string]map[string]int

func loadDemandFromRedis() (AppDemand, CPAppMap, AppOfferSiteDemandMap, error) {
	appDemand := make(AppDemand)
	cpAppMap := make(CPAppMap)
	appOfferSiteDemandMap := make(AppOfferSiteDemandMap)

	keys, err := RedisClient.HKeys(ctx, RedisCountGroupKey).Result()
	if err != nil {
		return nil, nil, nil, err
	}

	for _, key := range keys {
		count, _ := RedisClient.HGet(ctx, RedisCountGroupKey, key).Int()

		count = count / 10

		// 解析 key: offerId:siteId:country:platform:appId
		parts := strings.Split(key, ":")
		if len(parts) != 5 {
			continue
		}
		offerId, siteId, country, platform, appId := parts[0], parts[1], parts[2], parts[3], parts[4]

		// 累加 appId 需求
		appDemand[appId] += count

		// 构建 country:platform → appId 映射
		cpKey := country + ":" + platform
		if _, exists := cpAppMap[cpKey]; !exists {
			cpAppMap[cpKey] = make(map[string]bool)
		}
		cpAppMap[cpKey][appId] = true

		osKey := offerId + ":" + siteId
		if _, exists := appOfferSiteDemandMap[appId]; !exists {
			appOfferSiteDemandMap[appId] = make(map[string]int)
		}
		appOfferSiteDemandMap[appId][osKey] += count
	}

	return appDemand, cpAppMap, appOfferSiteDemandMap, nil
}

func processMinute(bloomManager *HourlyBloomManager) {
	date, hour, minute := getLastMinute()
	log.Printf("处理 %s %s:%s", date, hour, minute)

	appDemand, cpAppMap, appOfferIdSiteDemandMap, err := loadDemandFromRedis()
	if err != nil {
		log.Printf("加载需求失败: %v", err)
		return
	}

	results := make(map[string][]AdxRequest)

	for _, region := range Regions {
		lines, err := listAndDownloadFiles(region, date, hour, minute)
		if err != nil {
			log.Printf("%s 区域拉取失败: %v", region, err)
			continue
		}

		stop := true

		log.Printf("处理 %s %s:%s %d 条数据", region, date, hour, len(lines))
		for _, line := range lines {
			var req AdxRequest
			if err := json.Unmarshal([]byte(line), &req); err != nil {
				continue
			}

			cpKey := req.CountryCode + ":" + req.Platform
			appIDs, exists := cpAppMap[cpKey]
			if !exists {
				continue
			}

			// TODO 是否需要并发处理
			for appID := range appIDs {
				if appDemand[appID] <= 0 {
					continue
				}

				// 构造去重 key: MD5(appID) + ":" + deviceId
				dedupKey := fmt.Sprintf("%x:%s", md5.Sum([]byte(appID)), req.DeviceId)

				if !bloomManager.Contains(dedupKey) {
					bloomManager.Add(dedupKey)
					results[appID] = append(results[appID], req)
					appDemand[appID]--
				} else {
					log.Printf("%s重复", dedupKey)
				}

				stop = false
			}

			if stop {
				break
			}
		}

	}

	// TODO: 给capcut的数据ip换一下
	// 把ip地址在美国和不在美国的分开
	var usData []AdxRequest
	var notUsData []AdxRequest
	for _, adxRequest := range results[CapcutAppId] {
		country := searchIp(adxRequest.Ip)
		country = strings.Split(country, "|")[0]
		if country == "美国" {
			usData = append(usData, adxRequest)
		} else {
			notUsData = append(notUsData, adxRequest)
		}
	}

	for idx := range notUsData {
		if idx < len(usData) {
			log.Printf("替换ip %d %s %s", idx, notUsData[idx].Ip, usData[idx].Ip)
			notUsData[idx].Ip = usData[idx].Ip
		}
	}

	// 把usData和notUsData再放回到results[CapcutAppId]
	results[CapcutAppId] = append(notUsData, usData[len(notUsData):]...)

	// 依次分给各个offerSite
	for appId, datas := range results {
		offerSiteMap := appOfferIdSiteDemandMap[appId]
		var cur int

		for offerSite, count := range offerSiteMap {
			parts := strings.Split(offerSite, ":")
			offerId, siteId := parts[0], parts[1]

			nextCur := cur + count
			if nextCur > len(datas) {
				nextCur = len(datas)
			}
			requests := datas[cur:nextCur]
			cur = nextCur

			siteIdInt, _ := strconv.Atoi(siteId)
			// 转换成OfferUserDataBase
			var offerUserDataBases []*OfferUserDataBase
			for _, req := range requests {
				offerUserDataBase := transferAdxRequestToOfferUserDataBase(&req, offerId, siteIdInt)
				offerUserDataBases = append(offerUserDataBases, offerUserDataBase)
			}

			if len(requests) > 0 {
				log.Printf("发送%s, %s, %d条数据到ddj", offerId, siteId, len(requests))
			}
			// 发送给ddj ddj接口为 /offer/userdata
			//postData := map[string]interface{}{
			//	"data":    offerUserDataBases,
			//	"offerId": offerId,
			//}
			//err := sendPostRequest("http://ddj.pando.com/offer/userdata", postData)
			//if err != nil {
			//	log.Printf("发送%s, %s, %d条数据到ddj失败", offerId, siteId, len(requests))
			//}
		}

	}
}

// 发送 JSON 数据的示例
func sendPostRequest(url string, data interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("请求失败，状态码: %d", resp.StatusCode)
		return fmt.Errorf("请求失败，状态码: %d", resp.StatusCode)
	}

	return nil
}

func transferAdxRequestToOfferUserDataBase(data *AdxRequest, offerId string, siteId int) *OfferUserDataBase {
	return &OfferUserDataBase{
		Gaid:        data.DeviceId,
		Ip:          data.Ip,
		Geo:         data.CountryCode,
		Bundle:      data.AppId,
		OsVersion:   data.OsVersion,
		Os:          data.Platform,
		DeviceModel: data.PubId.Id, // publisher
		Model:       data.Model,
		Useragent:   data.UserAgent,
		Brand:       data.Brand,
		Lang:        data.Language,
		OfferId:     offerId,
		SiteId:      siteId,
	}
}

type OfferUserDataBase struct {
	Id            int64
	Gaid          string
	Ip            string
	Geo           string
	Bundle        string
	OsVersion     string
	Os            string
	DeviceModel   string // TODO: 改成publisher
	Model         string
	Useragent     string
	Brand         string
	Vertical      string
	Lang          string
	Status        string
	ChaClickId    string
	OfferId       string
	SiteId        int
	SiteIdChannel int
	ChannelId     string
}
