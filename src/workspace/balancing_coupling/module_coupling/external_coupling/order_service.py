from workspace.balancing_coupling.module_coupling.external_coupling import (
    external_system,
)


def is_paid(order_id: int) -> bool:
    result = external_system.request_payment(order_id)
    return result["result_code"] == "OK"
