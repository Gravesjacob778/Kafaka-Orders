"""
物流服務 (Logistics Service) - 物流 Consumer
從 Kafka 接收訂單事件，模擬安排出貨
"""

import json
import random
import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from confluent_kafka import Consumer, KafkaError, KafkaException
from config.settings import KAFKA_BROKER, ORDER_TOPIC, LOGISTICS_GROUP


# 模擬物流商列表
CARRIERS = ["黑貓宅急便", "新竹物流", "宅配通", "郵局包裹"]

# 模擬倉庫列表
WAREHOUSES = ["台北倉", "台中倉", "高雄倉"]


def create_consumer() -> Consumer:
    """建立 Kafka Consumer"""
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": LOGISTICS_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
    })
    consumer.subscribe([ORDER_TOPIC])
    return consumer


def generate_tracking_number() -> str:
    """產生模擬物流追蹤編號"""
    prefix = random.choice(["TW", "SF", "YT", "HN"])
    number = "".join([str(random.randint(0, 9)) for _ in range(12)])
    return f"{prefix}{number}"


def process_order(order: dict, processed_orders: set) -> None:
    """處理訂單 - 安排出貨"""
    order_id = order["order_id"]

    # 冪等性檢查
    if order_id in processed_orders:
        print(f"⚠️  訂單 {order_id} 已安排物流，跳過")
        return

    # 模擬物流安排
    carrier = random.choice(CARRIERS)
    warehouse = random.choice(WAREHOUSES)
    tracking_number = generate_tracking_number()
    estimated_days = random.randint(1, 5)
    estimated_delivery = (datetime.now() + timedelta(days=estimated_days)).strftime("%Y-%m-%d")

    # 計算總件數
    total_items = sum(item["quantity"] for item in order["items"])

    print(f"\n{'─'*50}")
    print(f"🚚 收到出貨請求: 訂單 {order_id}")
    print(f"   用戶: {order['user_id']}")
    print(f"   商品明細:")
    for item in order["items"]:
        print(f"     - {item['name']} x{item['quantity']}")
    print(f"   總件數: {total_items} 件")
    print()
    print(f"   📋 物流安排結果:")
    print(f"      出貨倉庫: {warehouse}")
    print(f"      物流商: {carrier}")
    print(f"      追蹤編號: {tracking_number}")
    print(f"      預計送達: {estimated_delivery} ({estimated_days} 個工作天)")
    print(f"      安排時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\n   ✅ 訂單 {order_id} 物流安排完成")

    # 標記為已處理
    processed_orders.add(order_id)
    print(f"{'─'*50}")


def main():
    """主程式"""
    print("=" * 60)
    print("🚚  物流服務 (Logistics Consumer)")
    print("=" * 60)
    print(f"Kafka Broker: {KAFKA_BROKER}")
    print(f"Topic: {ORDER_TOPIC}")
    print(f"Consumer Group: {LOGISTICS_GROUP}")
    print("-" * 60)

    consumer = create_consumer()
    processed_orders: set = set()

    print("✅ 物流 Consumer 已啟動，等待訂單中...\n")

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())

            order = json.loads(message.value().decode("utf-8"))
            process_order(order, processed_orders)

    except KeyboardInterrupt:
        print("\n\n⚠️  收到中斷信號，正在關閉...")
    finally:
        consumer.close()
        print("✅ 物流 Consumer 已安全關閉")
        print(f"📝 共安排 {len(processed_orders)} 筆物流出貨")


if __name__ == "__main__":
    main()
