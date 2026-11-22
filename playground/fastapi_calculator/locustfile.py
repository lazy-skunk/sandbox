from locust import HttpUser, between, task


class ApiUser(HttpUser):
    wait_time = between(0.5, 1.5)

    @task
    def calculate(self) -> None:
        self.client.post(
            "/calculate",
            headers={"Authorization": "Bearer sample-token"},
            json=[
                {
                    "num_a": i * 2 + 1,
                    "num_b": i * 2 + 2,
                    "method": "add" if i % 2 == 0 else "multiply",
                }
                for i in range(100)
            ],
        )
