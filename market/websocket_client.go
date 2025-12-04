package market

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type WSClient struct {
	conn        *websocket.Conn
	mu          sync.RWMutex
	subscribers map[string]chan []byte
	reconnect   bool
	done        chan struct{}
}

// WSMessage Hyperliquid WebSocket消息格式
type WSMessage struct {
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
}

// KlineWSData K线WebSocket数据 - 适配Hyperliquid格式
type KlineWSData struct {
	EventType string `json:"e"`
	EventTime int64  `json:"E"`
	Symbol    string `json:"s"`
	Kline     struct {
		StartTime           int64  `json:"t"`
		CloseTime           int64  `json:"T"`
		Symbol              string `json:"s"`
		Interval            string `json:"i"`
		FirstTradeID        int64  `json:"f"`
		LastTradeID         int64  `json:"L"`
		OpenPrice           string `json:"o"`
		ClosePrice          string `json:"c"`
		HighPrice           string `json:"h"`
		LowPrice            string `json:"l"`
		Volume              string `json:"v"`
		NumberOfTrades      int    `json:"n"`
		IsFinal             bool   `json:"x"`
		QuoteVolume         string `json:"q"`
		TakerBuyBaseVolume  string `json:"V"`
		TakerBuyQuoteVolume string `json:"Q"`
	} `json:"k"`
}

// HyperliquidCandleWS Hyperliquid WebSocket K线数据
type HyperliquidCandleWS struct {
	T int64  `json:"t"` // 开盘时间
	T2 int64 `json:"T"` // 收盘时间
	S string `json:"s"` // 交易对
	I string `json:"i"` // 时间间隔
	O string `json:"o"` // 开盘价
	C string `json:"c"` // 收盘价
	H string `json:"h"` // 最高价
	L string `json:"l"` // 最低价
	V string `json:"v"` // 成交量
	N int    `json:"n"` // 交易数量
}

type TickerWSData struct {
	EventType          string `json:"e"`
	EventTime          int64  `json:"E"`
	Symbol             string `json:"s"`
	PriceChange        string `json:"p"`
	PriceChangePercent string `json:"P"`
	WeightedAvgPrice   string `json:"w"`
	LastPrice          string `json:"c"`
	LastQty            string `json:"Q"`
	OpenPrice          string `json:"o"`
	HighPrice          string `json:"h"`
	LowPrice           string `json:"l"`
	Volume             string `json:"v"`
	QuoteVolume        string `json:"q"`
	OpenTime           int64  `json:"O"`
	CloseTime          int64  `json:"C"`
	FirstID            int64  `json:"F"`
	LastID             int64  `json:"L"`
	Count              int    `json:"n"`
}

func NewWSClient() *WSClient {
	return &WSClient{
		subscribers: make(map[string]chan []byte),
		reconnect:   true,
		done:        make(chan struct{}),
	}
}

func (w *WSClient) Connect() error {
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	// Hyperliquid WebSocket端点
	conn, _, err := dialer.Dial("wss://api.hyperliquid.xyz/ws", nil)
	if err != nil {
		return fmt.Errorf("WebSocket连接失败: %v", err)
	}

	w.mu.Lock()
	w.conn = conn
	w.mu.Unlock()

	log.Println("WebSocket连接成功")

	// 启动消息读取循环
	go w.readMessages()

	return nil
}

func (w *WSClient) SubscribeKline(symbol, interval string) error {
	// Hyperliquid使用coin名称(不带USDT)
	coin := NormalizeCoin(symbol)
	return w.subscribe(coin, "candle", interval)
}

func (w *WSClient) SubscribeTicker(symbol string) error {
	// Hyperliquid没有专门的ticker订阅，使用allMids
	_ = symbol // 保留参数兼容性
	return w.subscribeAllMids()
}

func (w *WSClient) SubscribeMiniTicker(symbol string) error {
	// Hyperliquid没有miniTicker，使用allMids代替
	_ = symbol // 保留参数兼容性
	return w.subscribeAllMids()
}

func (w *WSClient) subscribeAllMids() error {
	subscribeMsg := map[string]interface{}{
		"method": "subscribe",
		"subscription": map[string]interface{}{
			"type": "allMids",
		},
	}

	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.conn == nil {
		return fmt.Errorf("WebSocket未连接")
	}

	err := w.conn.WriteJSON(subscribeMsg)
	if err != nil {
		return err
	}

	log.Printf("订阅allMids流")
	return nil
}

func (w *WSClient) subscribe(coin, subType, interval string) error {
	// Hyperliquid订阅格式
	subscribeMsg := map[string]interface{}{
		"method": "subscribe",
		"subscription": map[string]interface{}{
			"type":     subType,
			"coin":     coin,
			"interval": interval,
		},
	}

	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.conn == nil {
		return fmt.Errorf("WebSocket未连接")
	}

	err := w.conn.WriteJSON(subscribeMsg)
	if err != nil {
		return err
	}

	log.Printf("订阅流: %s %s %s", coin, subType, interval)
	return nil
}

func (w *WSClient) readMessages() {
	for {
		select {
		case <-w.done:
			return
		default:
			w.mu.RLock()
			conn := w.conn
			w.mu.RUnlock()

			if conn == nil {
				time.Sleep(1 * time.Second)
				continue
			}

			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Printf("读取WebSocket消息失败: %v", err)
				w.handleReconnect()
				return
			}

			w.handleMessage(message)
		}
	}
}

func (w *WSClient) handleMessage(message []byte) {
	var wsMsg WSMessage
	if err := json.Unmarshal(message, &wsMsg); err != nil {
		// 可能是其他格式的消息
		return
	}

	// Hyperliquid使用channel字段而不是stream
	w.mu.RLock()
	ch, exists := w.subscribers[wsMsg.Channel]
	w.mu.RUnlock()

	if exists {
		select {
		case ch <- wsMsg.Data:
		default:
			log.Printf("订阅者通道已满: %s", wsMsg.Channel)
		}
	}
}

func (w *WSClient) handleReconnect() {
	if !w.reconnect {
		return
	}

	log.Println("尝试重新连接...")
	time.Sleep(3 * time.Second)

	if err := w.Connect(); err != nil {
		log.Printf("重新连接失败: %v", err)
		go w.handleReconnect()
	}
}

func (w *WSClient) AddSubscriber(stream string, bufferSize int) <-chan []byte {
	ch := make(chan []byte, bufferSize)
	w.mu.Lock()
	w.subscribers[stream] = ch
	w.mu.Unlock()
	return ch
}

func (w *WSClient) RemoveSubscriber(stream string) {
	w.mu.Lock()
	delete(w.subscribers, stream)
	w.mu.Unlock()
}

func (w *WSClient) Close() {
	w.reconnect = false
	close(w.done)

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.conn != nil {
		w.conn.Close()
		w.conn = nil
	}

	// 关闭所有订阅者通道
	for stream, ch := range w.subscribers {
		close(ch)
		delete(w.subscribers, stream)
	}
}
