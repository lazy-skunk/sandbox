def print_label(
    order_id: int,
    customer_name: str,
    shipping_address: str,
) -> None:
    print(f"注文番号: {order_id}")
    print(f"宛名: {customer_name}")
    print(f"配送先: {shipping_address}")
