def request_payment(order_id: int) -> dict[str, object]:
    return {"order_id": order_id, "result_code": "OK", "amount": 1000}
