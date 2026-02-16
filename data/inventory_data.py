"""
模擬庫存資料（In-Memory）
"""

import threading

# 庫存資料 (SKU -> 庫存數量)
INVENTORY = {
    "SKU001": 50,   # 無線藍牙耳機
    "SKU002": 30,   # 機械鍵盤
    "SKU003": 100,  # 手機保護殼
    "SKU004": 20,   # 智慧手錶
    "SKU005": 80,   # 行動電源
}

# 線程鎖，確保庫存操作的原子性
_lock = threading.Lock()


def get_stock(sku: str) -> int:
    """查詢指定 SKU 的庫存數量"""
    return INVENTORY.get(sku, 0)


def check_and_deduct(sku: str, quantity: int) -> tuple[bool, int]:
    """
    檢查並扣減庫存

    Args:
        sku: 商品 SKU
        quantity: 需要扣減的數量

    Returns:
        (成功與否, 剩餘庫存量)
    """
    with _lock:
        current_stock = INVENTORY.get(sku, 0)

        if current_stock < quantity:
            return False, current_stock

        INVENTORY[sku] = current_stock - quantity
        return True, INVENTORY[sku]


def show_all_stock() -> dict:
    """顯示所有庫存狀態"""
    return dict(INVENTORY)
