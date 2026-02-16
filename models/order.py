"""
訂單資料模型
"""

import uuid
from datetime import datetime

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.settings import PRODUCT_PRICES, PRODUCT_NAMES


def create_order(user_id: str, items: list[dict]) -> dict:
    """
    建立訂單資料

    Args:
        user_id: 用戶 ID
        items: 商品列表，每個元素為 {"sku": "SKU001", "quantity": 2}

    Returns:
        訂單 dict，包含完整訂單資訊
    """
    order_id = str(uuid.uuid4())[:8].upper()

    # 計算每個商品的小計與總價
    order_items = []
    total_price = 0.0

    for item in items:
        sku = item["sku"]
        qty = item["quantity"]
        unit_price = PRODUCT_PRICES.get(sku, 0.0)
        subtotal = unit_price * qty
        total_price += subtotal

        order_items.append({
            "sku": sku,
            "name": PRODUCT_NAMES.get(sku, "未知商品"),
            "quantity": qty,
            "unit_price": unit_price,
            "subtotal": subtotal,
        })

    order = {
        "order_id": order_id,
        "user_id": user_id,
        "items": order_items,
        "total_price": round(total_price, 2),
        "status": "CREATED",
        "created_at": datetime.now().isoformat(),
    }

    return order
