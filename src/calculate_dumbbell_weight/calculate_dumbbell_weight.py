from itertools import combinations

from tabulate import tabulate

_PLATE_WEIGHTS = [1.25, 2.0, 2.5, 3.5]
_TWO_PLATES_FOR_PAIR = 2
_HEADERS = ["dumbbell_weight", "plates"]
all_combinations = []

for i in range(1, len(_PLATE_WEIGHTS) + 1):
    for subset in combinations(_PLATE_WEIGHTS, i):
        total_weight = sum(subset) * _TWO_PLATES_FOR_PAIR
        all_combinations.append([total_weight, ", ".join(map(str, subset))])

all_combinations.sort(key=lambda x: x[0])

print(tabulate(all_combinations, headers=_HEADERS, tablefmt="fancy_grid"))
