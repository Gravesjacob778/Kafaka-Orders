"""
Kafka 電商系統 - 共用設定
"""

# Kafka Broker 連線設定
KAFKA_BROKER = "localhost:9092"

# Topic 名稱
ORDER_TOPIC = "orders.created"

# Consumer Group IDs
INVENTORY_GROUP = "inventory-group"
NOTIFICATION_GROUP = "notification-group"
LOGISTICS_GROUP = "logistics-group"

# 商品價格表 (SKU -> 單價)
PRODUCT_PRICES = {
    "SKU001": 299.0,   # 無線藍牙耳機
    "SKU002": 1299.0,  # 機械鍵盤
    "SKU003": 59.0,    # 手機保護殼
    "SKU004": 899.0,   # 智慧手錶
    "SKU005": 199.0,   # 行動電源
}

# 商品名稱對照
PRODUCT_NAMES = {
    "SKU001": "無線藍牙耳機",
    "SKU002": "機械鍵盤",
    "SKU003": "手機保護殼",
    "SKU004": "智慧手錶",
    "SKU005": "行動電源",
}
