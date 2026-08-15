from workspace.balancing_coupling.module_coupling.data_coupling import (
    shipping_label,
)


def main() -> None:
    shipping_label.print_label(
        order_id=1,
        customer_name="Alice",
        shipping_address="Tokyo",
    )


if __name__ == "__main__":
    main()
