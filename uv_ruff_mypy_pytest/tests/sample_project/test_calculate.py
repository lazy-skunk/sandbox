from sample_project.calculate import sum_even_numbers


def test_sum_even_numbers() -> None:
    assert sum_even_numbers([1, 2, 3, 4, 5]) == 6
