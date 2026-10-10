class OrderRepository:
    def save(self, order_id: int) -> None:
        self._save_to_database(order_id)

    def _save_to_database(self, order_id: int) -> None:
        print(f"注文 {order_id} を保存")
