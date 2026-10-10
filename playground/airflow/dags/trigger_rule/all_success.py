from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context


@dag(
    dag_id="ALL_SUCCESS",
    tags=["homomo", "TriggerRule"],
    params={
        "guest_count": Param(0, type="integer", minimum=0),
        "abort_no_guests": Param(False, type="boolean"),
    },
)
def build_all_success_dag() -> None:  # noqa: C901
    @task_group()
    def prepare_party() -> None:
        do_something = EmptyOperator(task_id="do_something")

        @task_group()
        def check_guest_count() -> None:
            no_guests = EmptyOperator(task_id="no_guests")
            regular_party = EmptyOperator(task_id="regular_party")
            too_many_guests = EmptyOperator(task_id="too_many_guests")

            @task.branch()
            def choose_party_plan() -> str:
                context = get_current_context()
                guest_count = context["params"]["guest_count"]

                if guest_count == 0:
                    if context["params"]["abort_no_guests"]:
                        raise AirflowFailException("abort_no_guests")
                    return no_guests.task_id

                if guest_count >= 100:
                    return too_many_guests.task_id

                return regular_party.task_id

            choose_party_plan() >> [no_guests, regular_party, too_many_guests]

        guest_count_check = check_guest_count()
        order_food = EmptyOperator(task_id="uber_eats")
        do_something >> guest_count_check >> order_food

    @task_group()
    def hold_party() -> None:
        EmptyOperator(task_id="enjoy_party")

    @task_group()
    def clean_up() -> None:
        EmptyOperator(task_id="wash_dishes")

    prepare_party() >> hold_party() >> clean_up()


build_all_success_dag()
