from dataclasses import dataclass


@dataclass(frozen=True)
class Order:
    order_id: int
    customer_name: str
    shipping_address: str
    items: tuple[str, ...]
    total: int
