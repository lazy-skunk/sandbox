import inspect

from workspace.balancing_coupling.module_coupling.content_coupling import (
    order_repository,
)


def main() -> None:
    repository = order_repository.OrderRepository()

    methods = dict(inspect.getmembers(repository, predicate=inspect.ismethod))
    save_to_database = methods["_save_to_database"]
    save_to_database(1)


if __name__ == "__main__":
    main()
