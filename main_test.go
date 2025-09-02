package main

import (
	"encoding/json"
	"fmt"
	"github.com/bits-and-blooms/bloom/v3"
	"github.com/gin-gonic/gin"
	"log"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func testHighVolumeDedup(t *testing.T) {
	manager := NewHourlyBloomManager()

	// 插入 10000 条测试数据
	n := 10000
	for i := 0; i < n; i++ {
		s := fmt.Sprintf("test_str_%d", i)
		if i%2 == 0 {
			// 偶数插入两次
			manager.Add(s)
			manager.Add(s)
		} else {
			// 奇数插入一次
			manager.Add(s)
		}
	}

	// 验证偶数字符串仍为“已存在”
	dupCount := 0
	for i := 0; i < n; i++ {
		s := fmt.Sprintf("test_str_%d", i)
		if manager.Contains(s) {
			dupCount++
		}
	}

	if dupCount != n {
		t.Errorf("预期所有 %d 条都存在，实际 %d 条存在", n, dupCount)
	}

	// 验证新字符串未被污染
	if manager.Contains("never_seen_before") {
		t.Error("错误：新字符串被误判为存在")
	}
}

func test24HourWindow(t *testing.T) {
	// 创建 manager（绕过自动时间对齐）
	filters := [24]*BloomFilterWithTime{}
	for i := 0; i < 24; i++ {
		ts := time.Now().UTC().Add(-time.Duration(24-i) * time.Hour).Truncate(time.Hour).Unix()
		bf := bloom.NewWithEstimates(HourlyCount, FalsePositive)
		filters[i] = &BloomFilterWithTime{BF: bf, Timestamp: ts}
	}

	// 手动设置第 0 小时（24 小时前）的 BF
	veryOldStr := "very_old_string"
	filters[0].BF.AddString(veryOldStr)

	manager := &HourlyBloomManager{
		filters: filters,
		current: 23,
		mtx:     make(chan struct{}, 1),
	}
	manager.mtx <- struct{}{}

	// 模拟当前时间是 now
	now := time.Now().UTC()
	currentHour := now.Truncate(time.Hour).Unix()

	// 插入当前小时的新字符串
	newStr := "brand_new_string"

	idx := manager.Add(newStr)
	if currentHour != manager.filters[idx].Timestamp {
		t.Error("错误：新插入数据不在当前小时")
	}

	// 验证：24 小时前的字符串应被“遗忘”
	if manager.Contains(veryOldStr) {
		t.Error("错误：24 小时前的数据仍被识别，时间窗口失效")
	}

	// 验证：当前小时字符串存在
	if !manager.Contains(newStr) {
		t.Error("错误：当前小时数据未识别")
	}
}

func testBatchDedupAPI(t *testing.T) {
	// 创建测试用的 HTTP 服务
	gin.SetMode(gin.TestMode)
	r := gin.New()
	manager := NewHourlyBloomManager()

	r.POST("/dedup", func(c *gin.Context) {
		var req []string
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(400, gin.H{"error": "invalid json"})
			return
		}
		result := manager.Dedup(req)
		c.JSON(200, result)
	})

	// 场景：第一次请求两个新字符串
	w1 := httptest.NewRecorder()
	req1 := `["str_a", "str_b", "str_a"]`
	r.ServeHTTP(w1, httptest.NewRequest("POST", "/dedup", strings.NewReader(req1)))

	var resp1 []string
	json.Unmarshal(w1.Body.Bytes(), &resp1)

	if len(resp1) != 2 || !contains(resp1, "str_a") || !contains(resp1, "str_b") {
		t.Errorf("第一次请求期望返回 [str_a str_b]，实际: %v", resp1)
	}

	// 场景：第二次请求相同字符串
	w2 := httptest.NewRecorder()
	r.ServeHTTP(w2, httptest.NewRequest("POST", "/dedup", strings.NewReader(req1)))

	var resp2 []string
	json.Unmarshal(w2.Body.Bytes(), &resp2)

	if len(resp2) != 0 {
		t.Errorf("第二次请求应返回空，实际: %v", resp2)
	}

	// 场景：请求一个新 + 一个旧
	w3 := httptest.NewRecorder()
	req3 := `["str_a", "str_c"]`
	r.ServeHTTP(w3, httptest.NewRequest("POST", "/dedup", strings.NewReader(req3)))

	var resp3 []string
	json.Unmarshal(w3.Body.Bytes(), &resp3)

	if len(resp3) != 1 || resp3[0] != "str_c" {
		t.Errorf("应只返回 str_c，实际: %v", resp3)
	}
}

// 辅助函数：检查 slice 是否包含某字符串
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func testPersistenceAndRecovery(t *testing.T) {
	_ = os.Remove(StateFilePath)

	// 第一阶段：写入数据并保存
	{
		manager := NewHourlyBloomManager()
		testStr := "persistent_string"
		manager.Add(testStr)
		err := manager.SaveToDisk()
		if err != nil {
			t.Fatalf("保存失败: %v", err)
		}
		log.Println("✅ 第一阶段：数据已保存")
	}

	// 第二阶段：重新加载
	{
		manager := NewHourlyBloomManager() // 会自动加载
		if !manager.Contains("persistent_string") {
			t.Error("错误：持久化数据未正确恢复")
		} else {
			log.Println("✅ 第二阶段：数据成功恢复")
		}
	}

	_ = os.Remove(StateFilePath)
}

func testHourlyRolling(t *testing.T) {
	// 强制时间对齐测试
	now := time.Now().UTC()
	prevHour := now.Add(-1 * time.Hour).Truncate(time.Hour)
	currHour := now.Truncate(time.Hour)

	manager := &HourlyBloomManager{
		filters: [24]*BloomFilterWithTime{},
		current: 0,
		mtx:     make(chan struct{}, 1),
	}
	manager.mtx <- struct{}{}

	// 初始化：第0个为 prevHour
	manager.filters[0] = &BloomFilterWithTime{
		BF:        bloom.NewWithEstimates(HourlyCount, FalsePositive),
		Timestamp: prevHour.Unix(),
	}

	// 手动设置当前时间为 currHour
	// 调用 Add 触发滚动
	manager.getCurrentHourIndex() // 内部会判断时间不对，滚动

	// 验证 current 已更新
	if manager.current != 1 {
		t.Errorf("期望 current=1，实际=%d", manager.current)
	}

	// 验证新小时已创建
	if manager.filters[1] == nil || manager.filters[1].Timestamp != currHour.Unix() {
		t.Error("错误：未正确创建新小时的布隆过滤器")
	}

	// 验证旧数据仍可查（23小时内）
	testStr := "from_prev_hour"
	manager.filters[0].BF.AddString(testStr)
	if !manager.Contains(testStr) {
		t.Error("错误：1小时前的数据不应被丢弃")
	}
}

func TestDedupService(t *testing.T) {
	// 清理旧状态文件
	_ = os.Remove(StateFilePath)

	t.Run("需求1: 大数据量去重", testHighVolumeDedup)
	t.Run("需求2: 24小时窗口精度", test24HourWindow)
	t.Run("需求3: 批量接口返回新字符串", testBatchDedupAPI)
	t.Run("需求4: 持久化与恢复", testPersistenceAndRecovery)
	t.Run("需求5: 每小时滚动更新", testHourlyRolling)
}
