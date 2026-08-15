from workspace.balancing_coupling.module_coupling.stamp_coupling import (
    shipping_label,
)
from workspace.balancing_coupling.module_coupling.stamp_coupling.order import (
    Order,
)


def main() -> None:
    order = Order(
        order_id=1,
        customer_name="Alice",
        shipping_address="Tokyo",
        items=("Book", "Pen"),
        total=1800,
    )
    shipping_label.print_label(order)


if __name__ == "__main__":
    main()
