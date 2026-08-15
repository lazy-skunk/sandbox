from workspace.balancing_coupling.module_coupling.common_coupling import (
    shared_tax,
)


def calculate(subtotal: int) -> float:
    shared_tax.tax_rate = 0.08
    return subtotal * (1 + shared_tax.tax_rate)
