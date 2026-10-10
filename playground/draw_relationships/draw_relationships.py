import argparse
import csv
from pathlib import Path

import matplotlib.pyplot as pyplot
import networkx

COLOR_FOR_SUSPICIOUS_NODE = "red"
COLOR_FOR_UG = "lightblue"
COLOR_FOR_NT_CC = "lightgreen"
COLOR_FOR_OTHERS = "lightpink"


def _load_relationship_graph_csv(file_path: Path) -> networkx.DiGraph:
    graph = networkx.DiGraph()

    with file_path.open(newline="", encoding="utf-8") as csv_file:
        rows = csv.reader(csv_file)
        next(rows, None)

        for row_number, row in enumerate(rows, start=2):
            if not row or all(not column.strip() for column in row):
                continue

            parent_group, child_group = _parse_relationship_row(
                row, row_number
            )
            graph.add_edge(parent_group, child_group)

    return graph


def _parse_relationship_row(
    row: list[str], row_number: int
) -> tuple[str, str]:
    if len(row) != 2:
        raise ValueError(
            f"CSV row {row_number} must have exactly 2 columns: {row!r}"
        )

    parent_group, child_group = (column.strip() for column in row)
    if not parent_group or not child_group:
        raise ValueError(
            f"CSV row {row_number} contains an empty group name: {row!r}"
        )

    return parent_group, child_group


def _calculate_hierarchical_positions(
    graph: networkx.DiGraph,
) -> dict[str, tuple[int, float]]:
    if not networkx.is_directed_acyclic_graph(graph):
        cycle_edges = networkx.find_cycle(graph)
        cycle_text = " -> ".join(parent for parent, _ in cycle_edges)
        raise ValueError(f"Relationship graph contains a cycle: {cycle_text}")

    depths = dict.fromkeys(graph.nodes, 0)
    for group in networkx.topological_sort(graph):
        for child_group in graph.successors(group):
            depths[child_group] = max(depths[child_group], depths[group] + 1)

    groups_by_depth: dict[int, list[str]] = {}
    for group in sorted(depths):
        groups_by_depth.setdefault(depths[group], []).append(group)

    positions: dict[str, tuple[int, float]] = {}
    for depth, groups in groups_by_depth.items():
        group_count = len(groups)
        for index, group in enumerate(groups):
            positions[group] = (depth, index - group_count / 2)

    return positions


def _choose_node_color(graph: networkx.DiGraph, node: str) -> str:
    if "UG-" not in node and any(
        "UG-" in parent_group for parent_group in graph.predecessors(node)
    ):
        return COLOR_FOR_SUSPICIOUS_NODE

    if "UG-" in node:
        return COLOR_FOR_UG

    if "NT" in node or "CC" in node:
        return COLOR_FOR_NT_CC

    return COLOR_FOR_OTHERS


def _draw_relationship_graph(
    graph: networkx.DiGraph,
    positions: dict[str, tuple[int, float]],
) -> None:
    networkx.draw(
        graph,
        positions,
        with_labels=True,
        node_color=[_choose_node_color(graph, node) for node in graph.nodes],
        edge_color="lightgray",
        arrows=True,
        node_shape="o",
    )

    pyplot.scatter([], [], c=COLOR_FOR_UG, label="UG")
    pyplot.scatter([], [], c=COLOR_FOR_NT_CC, label="NT/CC")
    pyplot.scatter([], [], c=COLOR_FOR_OTHERS, label="other")
    pyplot.scatter(
        [],
        [],
        c=COLOR_FOR_SUSPICIOUS_NODE,
        label="suspicious",
    )
    pyplot.legend()
    pyplot.show()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_file", type=Path)
    args = parser.parse_args()

    graph = _load_relationship_graph_csv(args.csv_file)
    positions = _calculate_hierarchical_positions(graph)

    _draw_relationship_graph(graph, positions)


if __name__ == "__main__":
    main()
