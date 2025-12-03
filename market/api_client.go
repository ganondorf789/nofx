package market

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"nofx/hook"
	"time"
)

const (
	baseURL = "https://api.hyperliquid.xyz"
)

type APIClient struct {
	client *http.Client
}

func NewAPIClient() *APIClient {
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	hookRes := hook.HookExec[hook.SetHttpClientResult](hook.SET_HTTP_CLIENT, client)
	if hookRes != nil && hookRes.Error() == nil {
		log.Printf("使用Hook设置的HTTP客户端")
		client = hookRes.GetResult()
	}

	return &APIClient{
		client: client,
	}
}

// postInfo 发送POST请求到Hyperliquid info端点
func (c *APIClient) postInfo(requestBody interface{}) ([]byte, error) {
	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", baseURL+"/info", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API返回状态码 %d: %s", resp.StatusCode, string(body))
	}

	return body, nil
}

func (c *APIClient) GetExchangeInfo() (*ExchangeInfo, error) {
	// Hyperliquid使用metaAndAssetCtxs获取交易对信息
	reqBody := map[string]string{"type": "metaAndAssetCtxs"}
	body, err := c.postInfo(reqBody)
	if err != nil {
		return nil, err
	}

	// Hyperliquid返回的是一个数组 [meta, assetCtxs]
	var result []json.RawMessage
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	if len(result) < 2 {
		return nil, fmt.Errorf("无效的metaAndAssetCtxs响应")
	}

	// 解析meta部分获取universe
	var meta struct {
		Universe []struct {
			Name        string `json:"name"`
			SzDecimals  int    `json:"szDecimals"`
			MaxLeverage int    `json:"maxLeverage"`
			IsDelisted  bool   `json:"isDelisted,omitempty"`
		} `json:"universe"`
	}
	if err := json.Unmarshal(result[0], &meta); err != nil {
		return nil, err
	}

	// 转换为ExchangeInfo格式
	exchangeInfo := &ExchangeInfo{
		Symbols: make([]SymbolInfo, 0, len(meta.Universe)),
	}

	for _, asset := range meta.Universe {
		if asset.IsDelisted {
			continue
		}
		exchangeInfo.Symbols = append(exchangeInfo.Symbols, SymbolInfo{
			Symbol:            asset.Name,
			Status:            "TRADING",
			BaseAsset:         asset.Name,
			QuoteAsset:        "USD",
			ContractType:      "PERPETUAL",
			PricePrecision:    8,
			QuantityPrecision: asset.SzDecimals,
		})
	}

	return exchangeInfo, nil
}

func (c *APIClient) GetKlines(symbol, interval string, limit int) ([]Kline, error) {
	// Hyperliquid需要计算startTime和endTime
	// 根据interval计算时间范围
	intervalDuration := getIntervalDuration(interval)
	endTime := time.Now().UnixMilli()
	startTime := endTime - int64(limit)*intervalDuration.Milliseconds()

	// 标准化symbol (去掉USDT后缀)
	coin := NormalizeCoin(symbol)

	reqBody := map[string]interface{}{
		"type":      "candleSnapshot",
		"req": map[string]interface{}{
			"coin":      coin,
			"interval":  interval,
			"startTime": startTime,
			"endTime":   endTime,
		},
	}

	body, err := c.postInfo(reqBody)
	if err != nil {
		return nil, err
	}

	// Hyperliquid返回K线数组
	var hlKlines []HyperliquidCandle
	if err := json.Unmarshal(body, &hlKlines); err != nil {
		log.Printf("获取K线数据失败,响应内容: %s", string(body))
		return nil, err
	}

	klines := make([]Kline, 0, len(hlKlines))
	for _, hlk := range hlKlines {
		kline := parseHyperliquidCandle(hlk)
		klines = append(klines, kline)
	}

	return klines, nil
}

// HyperliquidCandle Hyperliquid K线数据结构
type HyperliquidCandle struct {
	T int64   `json:"t"` // 开盘时间(毫秒)
	T2 int64  `json:"T"` // 收盘时间(毫秒)
	S string  `json:"s"` // 交易对
	I string  `json:"i"` // 时间间隔
	O string  `json:"o"` // 开盘价
	C string  `json:"c"` // 收盘价
	H string  `json:"h"` // 最高价
	L string  `json:"l"` // 最低价
	V string  `json:"v"` // 成交量
	N int     `json:"n"` // 交易数量
}

func parseHyperliquidCandle(hlk HyperliquidCandle) Kline {
	open, _ := parseFloat(hlk.O)
	high, _ := parseFloat(hlk.H)
	low, _ := parseFloat(hlk.L)
	closePrice, _ := parseFloat(hlk.C)
	volume, _ := parseFloat(hlk.V)

	return Kline{
		OpenTime:  hlk.T,
		CloseTime: hlk.T2,
		Open:      open,
		High:      high,
		Low:       low,
		Close:     closePrice,
		Volume:    volume,
		Trades:    hlk.N,
	}
}

func getIntervalDuration(interval string) time.Duration {
	switch interval {
	case "1m":
		return time.Minute
	case "3m":
		return 3 * time.Minute
	case "5m":
		return 5 * time.Minute
	case "15m":
		return 15 * time.Minute
	case "30m":
		return 30 * time.Minute
	case "1h":
		return time.Hour
	case "2h":
		return 2 * time.Hour
	case "4h":
		return 4 * time.Hour
	case "8h":
		return 8 * time.Hour
	case "12h":
		return 12 * time.Hour
	case "1d":
		return 24 * time.Hour
	default:
		return time.Hour
	}
}

// NormalizeCoin 标准化币种名称 (去掉USDT后缀，Hyperliquid使用纯币种名称)
func NormalizeCoin(symbol string) string {
	symbol = Normalize(symbol) // 先标准化为大写+USDT
	if len(symbol) > 4 && symbol[len(symbol)-4:] == "USDT" {
		return symbol[:len(symbol)-4]
	}
	return symbol
}

func (c *APIClient) GetCurrentPrice(symbol string) (float64, error) {
	// 使用allMids获取所有中间价
	reqBody := map[string]string{"type": "allMids"}
	body, err := c.postInfo(reqBody)
	if err != nil {
		return 0, err
	}

	// 返回格式: {"BTC": "50000.5", "ETH": "3000.2", ...}
	var mids map[string]string
	if err := json.Unmarshal(body, &mids); err != nil {
		return 0, err
	}

	coin := NormalizeCoin(symbol)
	priceStr, exists := mids[coin]
	if !exists {
		return 0, fmt.Errorf("未找到 %s 的价格", coin)
	}

	price, err := parseFloat(priceStr)
	if err != nil {
		return 0, err
	}

	return price, nil
}

// GetMetaAndAssetCtxs 获取元数据和资产上下文（包含OI、资金费率等）
func (c *APIClient) GetMetaAndAssetCtxs() ([]json.RawMessage, error) {
	reqBody := map[string]string{"type": "metaAndAssetCtxs"}
	body, err := c.postInfo(reqBody)
	if err != nil {
		return nil, err
	}

	var result []json.RawMessage
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	return result, nil
}
