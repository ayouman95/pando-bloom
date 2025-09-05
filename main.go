// main.go
package main

import (
	"bufio"
	"encoding/binary"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/gin-gonic/gin"
)

const (
	HourlyCount   = 50_000_000 // 每小时最多 5000 万条
	FalsePositive = 0.001      // 误判率 0.1%
	NumHours      = 24         // 保留 24 小时
	StateFilePath = "./bloom_state.bin"
	HTTPPort      = ":8080"
)

// BloomFilterWithTime 包含时间戳的布隆过滤器
type BloomFilterWithTime struct {
	BF        *bloom.BloomFilter
	Timestamp int64 // 小时级 Unix 时间（UTC）
}

// HourlyBloomManager 管理 24 个每小时的布隆过滤器
type HourlyBloomManager struct {
	filters [NumHours]*BloomFilterWithTime
	current int           // 当前写入的索引
	mtx     chan struct{} // 轻量级互斥锁（用带缓冲 channel 实现）
}

func NewHourlyBloomManager() *HourlyBloomManager {
	m := &HourlyBloomManager{
		filters: [NumHours]*BloomFilterWithTime{},
		current: -1,
		mtx:     make(chan struct{}, 1),
	}
	m.mtx <- struct{}{} // 初始化锁

	// 尝试从磁盘加载
	if err := m.loadFromDisk(); err != nil {
		log.Printf("首次启动或加载失败，创建新的布隆过滤器: %v", err)
		m.initNew()
	} else {
		log.Printf("成功从磁盘加载状态")
		m.alignToCurrentHour()
	}
	return m
}

// initNew 初始化 24 个新的布隆过滤器
func (m *HourlyBloomManager) initNew() {
	now := time.Now()
	hourStart := now.Truncate(time.Hour).Unix()

	for i := 0; i < NumHours; i++ {
		ts := hourStart - int64((NumHours-1-i))*3600
		m.filters[i] = &BloomFilterWithTime{
			BF:        bloom.NewWithEstimates(uint(HourlyCount), FalsePositive),
			Timestamp: ts,
		}
	}
	m.current = NumHours - 1 // 最后一个是当前小时
}

// alignToCurrentHour 对齐到当前小时，清理过期数据
func (m *HourlyBloomManager) alignToCurrentHour() {
	now := time.Now()
	currentHour := now.Truncate(time.Hour).Unix()

	// 找到当前小时对应的索引
	found := false
	for i := 0; i < NumHours; i++ {
		if m.filters[i] != nil && m.filters[i].Timestamp == currentHour {
			m.current = i
			found = true
			break
		}
	}

	if !found {
		// 时间偏差太大，重新初始化
		log.Printf("时间偏差过大，重新初始化")
		m.initNew()
	}
}

// getCurrentHourIndex 获取当前应写入的索引（基于时间）
func (m *HourlyBloomManager) getCurrentHourIndex() int {
	<-m.mtx
	defer func() { m.mtx <- struct{}{} }()

	now := time.Now()
	currentHour := now.Truncate(time.Hour).Unix()

	// 检查是否需要滚动
	if m.current == -1 || m.filters[m.current].Timestamp != currentHour {
		// 滚动到下一小时
		m.current = (m.current + 1) % NumHours
		m.filters[m.current] = &BloomFilterWithTime{
			BF:        bloom.NewWithEstimates(uint(HourlyCount), FalsePositive),
			Timestamp: currentHour,
		}
		log.Printf("滚动到新小时: %s", time.Unix(currentHour, 0).Format("2006-01-02 15:00"))
	}

	return m.current
}

// Add 添加字符串 返回插入的索引
func (m *HourlyBloomManager) Add(s string) int {
	idx := m.getCurrentHourIndex()
	m.filters[idx].BF.AddString(s)
	return idx
}

// Contains 检查是否在过去 24 小时内出现过
func (m *HourlyBloomManager) Contains(s string) bool {
	<-m.mtx
	defer func() { m.mtx <- struct{}{} }()

	now := time.Now()
	cutoff := now.Add(-24 * time.Hour).Truncate(time.Hour).Unix()

	for i := 0; i < NumHours; i++ {
		f := m.filters[i]
		if f != nil && f.Timestamp >= cutoff {
			if f.BF.TestString(s) {
				return true
			}
		}
	}
	return false
}

// Dedup 接收一批字符串，返回其中未出现过的
func (m *HourlyBloomManager) Dedup(strings []string) []string {
	var newOnes []string
	for _, s := range strings {
		if !m.Contains(s) {
			newOnes = append(newOnes, s)
			m.Add(s)
		}
	}
	return newOnes
}

// SaveToDisk 持久化到磁盘
func (m *HourlyBloomManager) SaveToDisk() error {
	<-m.mtx
	defer func() { m.mtx <- struct{}{} }()

	file, err := os.Create(StateFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	for i := 0; i < NumHours; i++ {
		f := m.filters[i]
		if f == nil {
			// 写入 nil 标记
			binary.Write(writer, binary.BigEndian, int64(0))
			continue
		}

		// 写入时间戳
		binary.Write(writer, binary.BigEndian, f.Timestamp)

		// 写入位数组长度
		bytes, _ := f.BF.GobEncode()
		binary.Write(writer, binary.BigEndian, int64(len(bytes)))
		writer.Write(bytes)
	}

	log.Printf("已持久化到磁盘: %s", StateFilePath)
	return nil
}

// loadFromDisk 从磁盘加载
func (m *HourlyBloomManager) loadFromDisk() error {
	file, err := os.Open(StateFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	var filters [NumHours]*BloomFilterWithTime

	for i := 0; i < NumHours; i++ {
		var timestamp int64
		err := binary.Read(reader, binary.BigEndian, &timestamp)
		if err != nil {
			return err
		}

		if timestamp == 0 {
			filters[i] = nil
			continue
		}

		var size int64
		err = binary.Read(reader, binary.BigEndian, &size)
		if err != nil {
			return err
		}

		data := make([]byte, size)
		_, err = io.ReadFull(reader, data)
		if err != nil {
			return err
		}

		bf := bloom.NewWithEstimates(uint(HourlyCount), FalsePositive)
		bf.GobDecode(data)

		filters[i] = &BloomFilterWithTime{
			BF:        bf,
			Timestamp: timestamp,
		}
	}

	m.filters = filters
	return nil
}

// StartAutoSave 每小时自动保存一次
func (m *HourlyBloomManager) StartAutoSave() {
	go func() {
		// 等待到下一个整点
		now := time.Now()
		next := now.Truncate(time.Hour).Add(time.Hour)
		time.Sleep(time.Until(next))

		ticker := time.NewTicker(time.Hour)
		for range ticker.C {
			if err := m.SaveToDisk(); err != nil {
				log.Printf("自动保存失败: %v", err)
			}
		}
	}()
}

// HandleSignal 注册信号处理，退出时保存
func (m *HourlyBloomManager) HandleSignal() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-c
		log.Printf("接收到信号 %v，正在保存状态并退出...", sig)
		_ = m.SaveToDisk()
		os.Exit(0)
	}()
}

func main() {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()

	manager := NewHourlyBloomManager()

	// 初始化客户端
	InitClients()

	// 启动定时保存
	manager.StartAutoSave()

	// 初始化ip库
	initXdb()

	// 定时拉取
	now := time.Now().UTC()
	next := now.Truncate(time.Minute).Add(time.Minute)
	time.Sleep(time.Until(next) + 10)
	ticker := time.NewTicker(time.Minute)
	for range ticker.C {
		go processMinute(manager)
	}

	//processMinute(manager)

	// 注册信号处理
	manager.HandleSignal()

	// 接口：POST /dedup
	r.POST("/dedup", func(c *gin.Context) {
		var req []string
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(400, gin.H{"error": "invalid json"})
			return
		}

		if len(req) == 0 {
			c.JSON(200, []string{})
			return
		}

		result := manager.Dedup(req)
		c.JSON(200, result)
	})

	// 健康检查
	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	log.Printf("服务启动中，监听端口 %s", HTTPPort)
	if err := r.Run(HTTPPort); err != nil {
		log.Fatalf("服务启动失败: %v", err)
	}
}
