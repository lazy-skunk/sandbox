def process(order_id: int, gift_wrap: bool) -> None:
    print(f"注文 {order_id} を受付")
    if gift_wrap:
        print(f"注文 {order_id} をギフト包装")
    print(f"注文 {order_id} を発送")
