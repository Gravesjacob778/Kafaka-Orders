# 🛒 Kafka 電商訂單系統

基於 Python + Apache Kafka 的簡易電商後台系統，使用事件驅動架構實現訂單處理流程。

## 📐 系統架構

```
                           ┌─────────────────────────┐
                           │      Apache Kafka        │
                           │   Topic: orders.created  │
                           └────┬────────┬────────┬───┘
                                │        │        │
            ┌───────────────────┤        │        ├───────────────────┐
            │                   │        │        │                   │
            ▼                   ▼        │        ▼                   │
   ┌─────────────────┐  ┌──────────────┐│ ┌──────────────────┐       │
   │  📦 庫存服務     │  │  🔔 通知服務  ││ │  🚚 物流服務      │       │
   │  Inventory       │  │  Notification││ │  Logistics        │       │
   │  Consumer        │  │  Consumer    ││ │  Consumer         │       │
   │                  │  │              ││ │                   │       │
   │  - 檢查庫存      │  │  - Email通知  ││ │  - 安排出貨       │       │
   │  - 扣減庫存      │  │  - 簡訊通知   ││ │  - 產生追蹤編號   │       │
   └─────────────────┘  └──────────────┘│ └──────────────────┘       │
                                        │                            │
   ┌────────────────────────────────────┘                            │
   │                                                                 │
   │    🛒 訂單服務 (Order Producer)                                 │
   │    - 接收用戶下單                                                │
   │    - 產生訂單事件                                                │
   │    - 發送到 Kafka                                                │
   └─────────────────────────────────────────────────────────────────┘
```

**Fan-out 模式**：所有 Consumer 擁有各自獨立的 Consumer Group，每個服務都會收到每一筆訂單事件進行獨立處理。

## 📁 專案結構

```
Kafaka-Orders/
├── docker-compose.yml          # Kafka + Zookeeper 容器編排
├── requirements.txt            # Python 依賴套件
├── README.md
├── config/
│   └── settings.py             # 共用設定 (Broker、Topic、Group ID)
├── models/
│   └── order.py                # 訂單資料模型
├── data/
│   └── inventory_data.py       # 模擬庫存資料 (In-Memory)
├── producer/
│   └── order_producer.py       # 訂單 Producer (模擬下單)
└── consumers/
    ├── inventory_consumer.py   # 庫存 Consumer (扣減庫存)
    ├── notification_consumer.py# 通知 Consumer (發送通知)
    └── logistics_consumer.py   # 物流 Consumer (安排出貨)
```

## 🚀 快速開始

### 1. 啟動 Kafka

需要先安裝 [Docker Desktop](https://www.docker.com/products/docker-desktop/)。

```bash
# 啟動 Kafka + Zookeeper
docker-compose up -d

# 確認容器狀態
docker-compose ps
```

### 2. 建立虛擬環境並安裝套件

**建立虛擬環境：**
```bash
python -m venv .venv
```

**啟動虛擬環境：**

Windows PowerShell：
```powershell
.\.venv\Scripts\Activate.ps1
```

Windows CMD：
```cmd
.venv\Scripts\activate.bat
```

**安裝依賴套件：**
```bash
pip install -r requirements.txt
```

### 3. 啟動 Consumer 服務

開啟 **3 個終端機**，分別啟動三個 Consumer：

**終端 1 - 庫存服務：**
```bash
python -m consumers.inventory_consumer
```

**終端 2 - 通知服務：**
```bash
python -m consumers.notification_consumer
```

**終端 3 - 物流服務：**
```bash
python -m consumers.logistics_consumer
```

### 4. 啟動 Producer 下單

開啟 **第 4 個終端機**，啟動訂單 Producer：

**⚠️ 重要：確保已啟動虛擬環境！**

```bash
# 如果尚未啟動虛擬環境，先執行：
.\.venv\Scripts\Activate.ps1  # Windows PowerShell

# 然後啟動 Producer：
python -m producer.order_producer
```

按 `Enter` 即可隨機產生並送出一筆訂單，觀察 3 個 Consumer 終端的處理結果。

## 🧪 測試場景

### 場景 1：正常下單
按 Enter 送出訂單，觀察三個 Consumer 分別輸出：
- **庫存服務**：庫存扣減成功，顯示剩餘庫存
- **通知服務**：模擬發送 Email 與簡訊通知
- **物流服務**：產生追蹤編號，安排出貨

### 場景 2：庫存不足
連續送出大量訂單（同一 SKU），當庫存耗盡時：
- **庫存服務**：顯示庫存不足警告
- **通知服務**：仍正常發送通知（通知不依賴庫存）
- **物流服務**：仍正常安排出貨

## 🔧 設定說明

| 設定項 | 位置 | 預設值 |
|--------|------|--------|
| Kafka Broker | `config/settings.py` | `localhost:9092` |
| Topic | `config/settings.py` | `orders.created` |
| 庫存 Group ID | `config/settings.py` | `inventory-group` |
| 通知 Group ID | `config/settings.py` | `notification-group` |
| 物流 Group ID | `config/settings.py` | `logistics-group` |

## 📦 模擬商品列表

| SKU | 商品名稱 | 單價 (NT$) | 初始庫存 |
|-----|---------|-----------|---------|
| SKU001 | 無線藍牙耳機 | 299 | 50 |
| SKU002 | 機械鍵盤 | 1,299 | 30 |
| SKU003 | 手機保護殼 | 59 | 100 |
| SKU004 | 智慧手錶 | 899 | 20 |
| SKU005 | 行動電源 | 199 | 80 |

## 🛑 停止服務

```bash
# 在各終端按 Ctrl+C 停止 Python 服務

# 停止 Kafka 容器
docker-compose down

# 停止並清除資料 (Volume)
docker-compose down -v
```
