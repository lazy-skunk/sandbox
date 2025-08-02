import hashlib


def select_eligible_users(
    users: list[dict[str, str]], seed_value: int, percentage_threshold: int
) -> list[dict[str, str]]:
    eligible_users = []

    for user in users:
        unique_key = f"{user['email']}{seed_value}"
        hashed_value = int(hashlib.md5(unique_key.encode()).hexdigest(), 16)
        random_value = hashed_value % 100

        if random_value < percentage_threshold:
            eligible_users.append(user)

    return eligible_users


if __name__ == "__main__":
    users: list[dict[str, str]] = [
        {
            "email": f"user{i}@example.com",
        }
        for i in range(1, 1000001)
    ]

    seed_value = 42
    percentage_threshold = 15

    eligible_users = select_eligible_users(
        users, seed_value, percentage_threshold
    )

    print(
        f"ユーザー数: {len(users)}, シード値: {seed_value}, 割合: {percentage_threshold}"
    )
    print(f"適用ユーザー数: {len(eligible_users)}")
    print(f"適用率: {round(len(eligible_users) / len(users) * 100, 2)}%")
    # print("対象ユーザー:")
    # for user in eligible_users:
    #     print(user)
