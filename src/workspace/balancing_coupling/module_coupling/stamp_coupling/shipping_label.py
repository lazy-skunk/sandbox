from workspace.balancing_coupling.module_coupling.stamp_coupling.order import (
    Order,
)


def print_label(order: Order) -> None:
    print(f"注文番号: {order.order_id}")
    print(f"宛名: {order.customer_name}")
    print(f"配送先: {order.shipping_address}")
