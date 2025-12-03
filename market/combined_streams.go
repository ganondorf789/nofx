package market

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type CombinedStreamsClient struct {
	conn        *websocket.Conn
	mu          sync.RWMutex
	subscribers map[string]chan []byte
	reconnect   bool
	done        chan struct{}
	batchSize   int // 每批订阅的流数量
}

func NewCombinedStreamsClient(batchSize int) *CombinedStreamsClient {
	return &CombinedStreamsClient{
		subscribers: make(map[string]chan []byte),
		reconnect:   true,
		done:        make(chan struct{}),
		batchSize:   batchSize,
	}
}

func (c *CombinedStreamsClient) Connect() error {
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	// Hyperliquid WebSocket端点
	conn, _, err := dialer.Dial("wss://api.hyperliquid.xyz/ws", nil)
	if err != nil {
		return fmt.Errorf("组合流WebSocket连接失败: %v", err)
	}

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	log.Println("组合流WebSocket连接成功")
	go c.readMessages()

	return nil
}

// BatchSubscribeKlines 批量订阅K线
func (c *CombinedStreamsClient) BatchSubscribeKlines(symbols []string, interval string) error {
	// 将symbols分批处理
	batches := c.splitIntoBatches(symbols, c.batchSize)

	for i, batch := range batches {
		log.Printf("订阅第 %d 批, 数量: %d", i+1, len(batch))

		// Hyperliquid需要逐个订阅candle流
		for _, symbol := range batch {
			coin := NormalizeCoin(symbol)
			if err := c.subscribeCandle(coin, interval); err != nil {
				log.Printf("订阅 %s %s K线失败: %v", coin, interval, err)
			}
		}

		// 批次间延迟，避免被限制
		if i < len(batches)-1 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	return nil
}

// subscribeCandle 订阅单个币种的K线
func (c *CombinedStreamsClient) subscribeCandle(coin, interval string) error {
	subscribeMsg := map[string]interface{}{
		"method": "subscribe",
		"subscription": map[string]interface{}{
			"type":     "candle",
			"coin":     coin,
			"interval": interval,
		},
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.conn == nil {
		return fmt.Errorf("WebSocket未连接")
	}

	return c.conn.WriteJSON(subscribeMsg)
}

// splitIntoBatches 将切片分成指定大小的批次
func (c *CombinedStreamsClient) splitIntoBatches(symbols []string, batchSize int) [][]string {
	var batches [][]string

	for i := 0; i < len(symbols); i += batchSize {
		end := i + batchSize
		if end > len(symbols) {
			end = len(symbols)
		}
		batches = append(batches, symbols[i:end])
	}

	return batches
}

// subscribeStreams 订阅多个流 (Hyperliquid需要逐个订阅)
func (c *CombinedStreamsClient) subscribeStreams(streams []string) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.conn == nil {
		return fmt.Errorf("WebSocket未连接")
	}

	// Hyperliquid不支持批量订阅，需要逐个订阅
	for _, stream := range streams {
		// 解析stream格式 (coin@kline_interval)
		parts := strings.Split(stream, "@")
		if len(parts) != 2 {
			continue
		}
		coin := strings.ToUpper(parts[0])
		intervalParts := strings.Split(parts[1], "_")
		if len(intervalParts) != 2 {
			continue
		}
		interval := intervalParts[1]

		subscribeMsg := map[string]interface{}{
			"method": "subscribe",
			"subscription": map[string]interface{}{
				"type":     "candle",
				"coin":     coin,
				"interval": interval,
			},
		}

		if err := c.conn.WriteJSON(subscribeMsg); err != nil {
			return err
		}
	}

	log.Printf("订阅流: %v", streams)
	return nil
}

func (c *CombinedStreamsClient) readMessages() {
	for {
		select {
		case <-c.done:
			return
		default:
			c.mu.RLock()
			conn := c.conn
			c.mu.RUnlock()

			if conn == nil {
				time.Sleep(1 * time.Second)
				continue
			}

			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Printf("读取组合流消息失败: %v", err)
				c.handleReconnect()
				return
			}

			c.handleCombinedMessage(message)
		}
	}
}

func (c *CombinedStreamsClient) handleCombinedMessage(message []byte) {
	// Hyperliquid消息格式: {"channel": "candle", "data": {...}}
	var combinedMsg struct {
		Channel string          `json:"channel"`
		Data    json.RawMessage `json:"data"`
	}

	if err := json.Unmarshal(message, &combinedMsg); err != nil {
		log.Printf("解析组合消息失败: %v", err)
		return
	}

	// 处理candle消息
	if combinedMsg.Channel == "candle" {
		// 解析candle数据获取coin和interval
		var candleData struct {
			S string `json:"s"` // 交易对
			I string `json:"i"` // 时间间隔
		}
		if err := json.Unmarshal(combinedMsg.Data, &candleData); err == nil {
			// 构造与Binance兼容的stream key
			stream := fmt.Sprintf("%s@kline_%s", strings.ToLower(candleData.S), candleData.I)
			c.mu.RLock()
			ch, exists := c.subscribers[stream]
			c.mu.RUnlock()

			if exists {
				select {
				case ch <- combinedMsg.Data:
				default:
					log.Printf("订阅者通道已满: %s", stream)
				}
			}
		}
		return
	}

	// 其他消息类型直接使用channel作为key
	c.mu.RLock()
	ch, exists := c.subscribers[combinedMsg.Channel]
	c.mu.RUnlock()

	if exists {
		select {
		case ch <- combinedMsg.Data:
		default:
			log.Printf("订阅者通道已满: %s", combinedMsg.Channel)
		}
	}
}

func (c *CombinedStreamsClient) AddSubscriber(stream string, bufferSize int) <-chan []byte {
	ch := make(chan []byte, bufferSize)
	c.mu.Lock()
	c.subscribers[stream] = ch
	c.mu.Unlock()
	return ch
}

func (c *CombinedStreamsClient) handleReconnect() {
	if !c.reconnect {
		return
	}

	log.Println("组合流尝试重新连接...")
	time.Sleep(3 * time.Second)

	if err := c.Connect(); err != nil {
		log.Printf("组合流重新连接失败: %v", err)
		go c.handleReconnect()
	}
}

func (c *CombinedStreamsClient) Close() {
	c.reconnect = false
	close(c.done)

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}

	for stream, ch := range c.subscribers {
		close(ch)
		delete(c.subscribers, stream)
	}
}
