"""
庫存服務 (Inventory Service) - 庫存 Consumer
從 Kafka 接收訂單事件，執行庫存扣減
"""

import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from confluent_kafka import Consumer, KafkaError, KafkaException
from config.settings import KAFKA_BROKER, ORDER_TOPIC, INVENTORY_GROUP
from data.inventory_data import check_and_deduct, get_stock, show_all_stock


def create_consumer() -> Consumer:
    """建立 Kafka Consumer"""
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": INVENTORY_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
    })
    consumer.subscribe([ORDER_TOPIC])
    return consumer


def process_order(order: dict, processed_orders: set) -> None:
    """處理訂單 - 扣減庫存"""
    order_id = order["order_id"]

    # 冪等性檢查：避免重複處理
    if order_id in processed_orders:
        print(f"⚠️  訂單 {order_id} 已處理過，跳過")
        return

    print(f"\n{'─'*50}")
    print(f"📦 收到訂單: {order_id} (用戶: {order['user_id']})")

    all_success = True

    for item in order["items"]:
        sku = item["sku"]
        name = item["name"]
        quantity = item["quantity"]

        success, remaining = check_and_deduct(sku, quantity)

        if success:
            print(f"   ✅ {name} ({sku}) 扣減 {quantity} 件成功，剩餘庫存: {remaining}")
        else:
            print(f"   ❌ {name} ({sku}) 庫存不足！需要 {quantity} 件，僅剩 {remaining} 件")
            all_success = False

    if all_success:
        print(f"   🎉 訂單 {order_id} 庫存扣減完成")
    else:
        print(f"   ⚠️  訂單 {order_id} 部分商品庫存不足")

    # 標記為已處理
    processed_orders.add(order_id)

    # 顯示當前庫存快照
    print(f"\n   📊 當前庫存狀態: {show_all_stock()}")
    print(f"{'─'*50}")


def main():
    """主程式"""
    print("=" * 60)
    print("📦  庫存服務 (Inventory Consumer)")
    print("=" * 60)
    print(f"Kafka Broker: {KAFKA_BROKER}")
    print(f"Topic: {ORDER_TOPIC}")
    print(f"Consumer Group: {INVENTORY_GROUP}")
    print("-" * 60)

    print(f"📊 初始庫存: {show_all_stock()}")
    print("-" * 60)

    consumer = create_consumer()
    processed_orders: set = set()

    print("✅ 庫存 Consumer 已啟動，等待訂單中...\n")

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
        print("✅ 庫存 Consumer 已安全關閉")
        print(f"📊 最終庫存: {show_all_stock()}")
        print(f"📝 共處理 {len(processed_orders)} 筆訂單")


if __name__ == "__main__":
    main()
