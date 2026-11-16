"""
Reducer Module for Semester Results MapReduce.

In the Reduce phase of the MapReduce paradigm:
1. Shuffled and sorted key-value groups (grade, [1, 1, 1, ...]) are processed.
2. The reducer iterates through the associated values for each unique key.
3. The reducer computes the aggregate total student count for each grade.
4. Output pairs (grade, total_students) are produced.
"""

from typing import Iterable, List, Tuple


class GradeReducer:
    """Implements the Reduce phase for student examination grade distribution."""

    def reduce_group(self, grade: str, values: Iterable[int]) -> Tuple[str, int]:
        """
        Reduces a single grouped key and its value collection to an aggregate count.

        Args:
            grade: The academic grade category (key).
            values: Iterable of counts (e.g. [1, 1, 1, ...]).

        Returns:
            Tuple of (grade, total_count).
        """
        total = sum(values)
        return (grade, total)

    def reduce(self, grouped_records: List[Tuple[str, List[int]]]) -> List[Tuple[str, int]]:
        """
        Executes the Reduce phase across all grouped grade pairs.

        Args:
            grouped_records: List of tuples (grade, list_of_counts) sorted from shuffle phase.

        Returns:
            List of final reduced tuples [(grade, total_count), ...].
        """
        results: List[Tuple[str, int]] = []
        for grade, values in grouped_records:
            results.append(self.reduce_group(grade, values))
        return results
