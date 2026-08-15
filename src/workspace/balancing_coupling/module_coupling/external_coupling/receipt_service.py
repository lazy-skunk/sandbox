from workspace.balancing_coupling.module_coupling.external_coupling import (
    external_system,
)


def create_receipt(order_id: int) -> str:
    result = external_system.request_payment(order_id)
    return f"{result['amount']}円"
