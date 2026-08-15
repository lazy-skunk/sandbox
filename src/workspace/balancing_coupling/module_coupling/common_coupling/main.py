from workspace.balancing_coupling.module_coupling.common_coupling import (
    reduced_price,
    standard_price,
)


def main() -> None:
    order_subtotal = 1000
    print(f"標準税率: {standard_price.calculate(order_subtotal)}")
    print(f"軽減税率: {reduced_price.calculate(order_subtotal)}")
    print(f"標準税率: {standard_price.calculate(order_subtotal)}")


if __name__ == "__main__":
    main()
