"""
訂單服務 (Order Service) - 訂單 Producer
模擬用戶下單，將訂單事件發送到 Kafka
"""

import json
import random
import time
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from confluent_kafka import Producer
from config.settings import KAFKA_BROKER, ORDER_TOPIC, PRODUCT_NAMES
from models.order import create_order

# 模擬用戶列表
MOCK_USERS = ["USER001", "USER002", "USER003", "USER004", "USER005"]

# 可用的 SKU 列表
AVAILABLE_SKUS = list(PRODUCT_NAMES.keys())


def create_producer() -> Producer:
    """建立 Kafka Producer"""
    producer = Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "acks": "all",
    })
    return producer


def generate_random_order() -> dict:
    """隨機產生一筆模擬訂單"""
    user_id = random.choice(MOCK_USERS)

    # 隨機選 1~3 種商品
    num_items = random.randint(1, 3)
    selected_skus = random.sample(AVAILABLE_SKUS, min(num_items, len(AVAILABLE_SKUS)))

    items = []
    for sku in selected_skus:
        items.append({
            "sku": sku,
            "quantity": random.randint(1, 5),
        })

    return create_order(user_id, items)


def send_order(producer: Producer, order: dict):
    """發送訂單到 Kafka"""
    try:
        payload = json.dumps(order, ensure_ascii=False).encode("utf-8")
        delivery_result = {}

        def on_delivery(err, msg):
            if err is not None:
                delivery_result["error"] = err
            else:
                delivery_result["topic"] = msg.topic()
                delivery_result["partition"] = msg.partition()
                delivery_result["offset"] = msg.offset()

        producer.produce(
            topic=ORDER_TOPIC,
            key=order["order_id"].encode("utf-8"),
            value=payload,
            on_delivery=on_delivery,
        )
        producer.poll(0)
        producer.flush(10)

        if delivery_result.get("error") is not None:
            raise RuntimeError(delivery_result["error"])

        print(f"\n{'='*60}")
        print(f"✅ 訂單發送成功！")
        print(f"   訂單編號: {order['order_id']}")
        print(f"   用戶: {order['user_id']}")
        print(f"   商品明細:")
        for item in order["items"]:
            print(f"     - {item['name']} x{item['quantity']} (NT${item['subtotal']:.0f})")
        print(f"   總金額: NT${order['total_price']:.0f}")
        print(f"   Topic: {delivery_result.get('topic', ORDER_TOPIC)}")
        print(f"   Partition: {delivery_result.get('partition', '-')}")
        print(f"   Offset: {delivery_result.get('offset', '-')}")
        print(f"{'='*60}")

    except Exception as e:
        print(f"\n❌ 訂單發送失敗: {e}")


def main():
    """主程式 - 互動式下單"""
    print("=" * 60)
    print("🛒  電商訂單服務 (Order Producer)")
    print("=" * 60)
    print(f"Kafka Broker: {KAFKA_BROKER}")
    print(f"Topic: {ORDER_TOPIC}")
    print("-" * 60)

    producer = create_producer()
    print("✅ Kafka Producer 已連線\n")

    print("操作說明:")
    print("  [Enter]  - 隨機產生一筆訂單")
    print("  [q]      - 退出程式")
    print("-" * 60)

    try:
        while True:
            user_input = input("\n按 Enter 送出訂單 (q 退出): ").strip().lower()

            if user_input == "q":
                print("\n👋 正在關閉 Producer...")
                break

            order = generate_random_order()
            send_order(producer, order)

            # 短暫延遲，避免過快
            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\n\n⚠️  收到中斷信號，正在關閉...")
    finally:
        producer.flush()
        producer.close()
        print("✅ Producer 已安全關閉")


if __name__ == "__main__":
    main()
