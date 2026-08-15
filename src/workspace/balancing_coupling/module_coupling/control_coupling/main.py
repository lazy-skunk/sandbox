from workspace.balancing_coupling.module_coupling.control_coupling import (
    order_processor,
)


def main() -> None:
    order_processor.process(order_id=1, gift_wrap=False)
    order_processor.process(order_id=2, gift_wrap=True)


if __name__ == "__main__":
    main()
