"""
通知服務 (Notification Service) - 通知 Consumer
從 Kafka 接收訂單事件，模擬發送訂單確認通知
"""

import json
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from confluent_kafka import Consumer, KafkaError, KafkaException
from config.settings import KAFKA_BROKER, ORDER_TOPIC, NOTIFICATION_GROUP


# 模擬用戶聯絡資訊
MOCK_USER_CONTACTS = {
    "USER001": {"name": "王小明", "email": "ming@example.com", "phone": "0912-345-678"},
    "USER002": {"name": "李小華", "email": "hua@example.com", "phone": "0923-456-789"},
    "USER003": {"name": "張大偉", "email": "wei@example.com", "phone": "0934-567-890"},
    "USER004": {"name": "陳美玲", "email": "ling@example.com", "phone": "0945-678-901"},
    "USER005": {"name": "林志豪", "email": "hao@example.com", "phone": "0956-789-012"},
}


def create_consumer() -> Consumer:
    """建立 Kafka Consumer"""
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": NOTIFICATION_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
    })
    consumer.subscribe([ORDER_TOPIC])
    return consumer


def send_email_notification(order: dict, contact: dict) -> None:
    """模擬發送 Email 通知"""
    print(f"   📧 Email 通知已發送")
    print(f"      收件人: {contact['name']} <{contact['email']}>")
    print(f"      主旨: 訂單確認 - 訂單編號 #{order['order_id']}")
    print(f"      內容: 親愛的{contact['name']}您好，")
    print(f"             您的訂單 #{order['order_id']} 已成功建立！")
    print(f"             訂單金額: NT${order['total_price']:.0f}")
    print(f"             我們將盡快為您處理。")


def send_sms_notification(order: dict, contact: dict) -> None:
    """模擬發送簡訊通知"""
    print(f"   📱 簡訊通知已發送")
    print(f"      手機號: {contact['phone']}")
    print(f"      內容: 【電商平台】{contact['name']}您好，訂單#{order['order_id']}已建立，"
          f"金額NT${order['total_price']:.0f}，感謝您的購買！")


def process_order(order: dict, processed_orders: set) -> None:
    """處理訂單 - 發送通知"""
    order_id = order["order_id"]

    # 冪等性檢查
    if order_id in processed_orders:
        print(f"⚠️  訂單 {order_id} 通知已發送過，跳過")
        return

    user_id = order["user_id"]
    contact = MOCK_USER_CONTACTS.get(user_id, {
        "name": "用戶",
        "email": f"{user_id}@example.com",
        "phone": "0900-000-000",
    })

    print(f"\n{'─'*50}")
    print(f"🔔 收到訂單通知請求: {order_id}")
    print(f"   用戶: {contact['name']} ({user_id})")
    print(f"   訂單金額: NT${order['total_price']:.0f}")
    print(f"   商品數量: {len(order['items'])} 種")
    print(f"   下單時間: {order['created_at']}")
    print()

    # 發送 Email 通知
    send_email_notification(order, contact)
    print()

    # 發送簡訊通知
    send_sms_notification(order, contact)

    print(f"\n   ✅ 訂單 {order_id} 所有通知發送完成")
    print(f"      通知時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 標記為已處理
    processed_orders.add(order_id)
    print(f"{'─'*50}")


def main():
    """主程式"""
    print("=" * 60)
    print("🔔  通知服務 (Notification Consumer)")
    print("=" * 60)
    print(f"Kafka Broker: {KAFKA_BROKER}")
    print(f"Topic: {ORDER_TOPIC}")
    print(f"Consumer Group: {NOTIFICATION_GROUP}")
    print("-" * 60)

    consumer = create_consumer()
    processed_orders: set = set()

    print("✅ 通知 Consumer 已啟動，等待訂單中...\n")

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
        print("✅ 通知 Consumer 已安全關閉")
        print(f"📝 共發送 {len(processed_orders)} 筆訂單通知")


if __name__ == "__main__":
    main()
