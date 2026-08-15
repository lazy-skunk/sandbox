from workspace.balancing_coupling.module_coupling.external_coupling import (
    order_service,
    receipt_service,
)


def main() -> None:
    order_id = 1
    print(order_service.is_paid(order_id))
    print(receipt_service.create_receipt(order_id))


if __name__ == "__main__":
    main()
