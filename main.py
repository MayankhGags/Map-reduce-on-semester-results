"""
Semester Results Analysis using MapReduce Framework.

This application demonstrates the classic MapReduce computational model:
  [Input CSV] -> [Map Phase] -> [Shuffle & Sort] -> [Reduce Phase] -> [Final Output]

Grades evaluated: S, A, B, C, D, E, F
"""

import argparse
import csv
import os
import sys
from collections import defaultdict
from typing import Dict, List, Tuple

from mapper import GradeMapper, VALID_GRADES
from reducer import GradeReducer

GRADE_DESCRIPTIONS = {
    "S": "Outstanding (>=90%)",
    "A": "Excellent   (80-89%)",
    "B": "Very Good   (70-79%)",
    "C": "Good        (60-69%)",
    "D": "Average     (50-59%)",
    "E": "Pass        (40-49%)",
    "F": "Fail        (<40%)",
}

# Canonical academic grade order
ACADEMIC_ORDER = ["S", "A", "B", "C", "D", "E", "F"]


def load_dataset(file_path: str) -> List[Dict[str, str]]:
    """
    Reads student examination results from a CSV file.

    Args:
        file_path: Path to the CSV dataset.

    Returns:
        List of dictionaries containing row records.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Input file not found at: {file_path}")

    records: List[Dict[str, str]] = []
    with open(file_path, mode="r", encoding="utf-8-sig") as csv_file:
        reader = csv.DictReader(csv_file)
        if reader.fieldnames is None:
            raise ValueError(f"Dataset at {file_path} is empty or lacks headers.")
        
        # Clean header keys (strip trailing spaces)
        for row in reader:
            cleaned_row = {k.strip(): (v.strip() if v else "") for k, v in row.items() if k}
            records.append(cleaned_row)

    return records


def shuffle_and_sort(intermediate_pairs: List[Tuple[str, int]]) -> List[Tuple[str, List[int]]]:
    """
    Explicitly executes the Shuffle & Sort phase of MapReduce:
    1. Partition / Group: Aggregates intermediate (key, value) pairs by key.
    2. Sort: Orders the keys according to standard academic grade hierarchy.

    Args:
        intermediate_pairs: List of (grade, 1) emitted by the mapper.

    Returns:
        List of tuples: [(grade, [1, 1, 1, ...])] sorted in academic order.
    """
    grouped_buckets: Dict[str, List[int]] = defaultdict(list)

    # Shuffle: Group values by key
    for key, value in intermediate_pairs:
        grouped_buckets[key].append(value)

    # Sort: Sort according to canonical academic grade hierarchy
    def sort_key(grade: str) -> int:
        if grade in ACADEMIC_ORDER:
            return ACADEMIC_ORDER.index(grade)
        return len(ACADEMIC_ORDER)  # Unknown grades placed at the end

    sorted_keys = sorted(grouped_buckets.keys(), key=sort_key)
    return [(key, grouped_buckets[key]) for key in sorted_keys]


def print_results(reduced_results: List[Tuple[str, int]], skipped_records: List[Dict[str, str]]) -> None:
    """Formats and displays the final grade-wise distribution and summary metrics."""
    total_valid = sum(count for _, count in reduced_results)
    
    print("\n" + "=" * 70)
    print("        END-SEMESTER EXAMINATION RESULTS - MAPREDUCE ANALYSIS")
    print("=" * 70)
    print(f"{'Grade':<8} | {'Description':<24} | {'Count':<8} | {'Percentage':<10} | {'Distribution'}")
    print("-" * 70)

    # Build lookup for quick lookup
    count_lookup = dict(reduced_results)

    for grade in ACADEMIC_ORDER:
        count = count_lookup.get(grade, 0)
        percentage = (count / total_valid * 100) if total_valid > 0 else 0.0
        bar = "#" * int(percentage // 3)
        desc = GRADE_DESCRIPTIONS.get(grade, "Unknown")
        print(f"  {grade:<6} | {desc:<24} | {count:<8} | {percentage:6.2f}%   | {bar}")

    # Check any extra unexpected keys
    for grade, count in reduced_results:
        if grade not in ACADEMIC_ORDER:
            percentage = (count / total_valid * 100) if total_valid > 0 else 0.0
            print(f"  {grade:<6} | {'Other Grade':<24} | {count:<8} | {percentage:6.2f}%   |")

    print("-" * 70)
    print(f"Total Valid Student Records Evaluated : {total_valid}")
    print(f"Invalid / Skipped Records Logged     : {len(skipped_records)}")
    print("=" * 70)

    if skipped_records:
        print("\n[Log of Skipped / Invalid Records]")
        for idx, rec in enumerate(skipped_records, 1):
            print(f"  {idx}. Student ID: {rec.get('StudentID')}, Reason: {rec.get('Reason')}")
    print()


def run_pipeline(csv_file: str, show_steps: bool = False, infer_missing: bool = False) -> None:
    """Executes the complete MapReduce workflow."""
    print(f"\n[MapReduce Pipeline] Ingesting results from: {csv_file}")
    raw_records = load_dataset(csv_file)
    print(f"[Input Phase] Loaded {len(raw_records)} records from CSV.")

    # 1. MAP PHASE
    mapper = GradeMapper(infer_missing_grades=infer_missing)
    intermediate_pairs = mapper.map(raw_records)
    print(f"[Map Phase Complete] Emitted {len(intermediate_pairs)} intermediate key-value pairs.")
    if show_steps:
        print("  Sample intermediate pairs:", intermediate_pairs[:10], "...")

    # 2. SHUFFLE & SORT PHASE
    grouped_pairs = shuffle_and_sort(intermediate_pairs)
    print(f"[Shuffle & Sort Phase Complete] Grouped into {len(grouped_pairs)} unique grade categories.")
    if show_steps:
        for key, vals in grouped_pairs:
            print(f"  Shuffled Key '{key}': count of values = {len(vals)}")

    # 3. REDUCE PHASE
    reducer = GradeReducer()
    reduced_results = reducer.reduce(grouped_pairs)
    print(f"[Reduce Phase Complete] Aggregation calculated for {len(reduced_results)} grade categories.")

    # 4. OUTPUT / REPORTING
    print_results(reduced_results, mapper.skipped_records)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="MapReduce Framework on End-Semester Examination Results"
    )
    default_csv = os.path.join(os.path.dirname(__file__), "sample_results.csv")
    parser.add_argument(
        "--input", "-i",
        default=default_csv,
        help=f"Path to student examination CSV (default: {default_csv})"
    )
    parser.add_argument(
        "--show-steps", "-s",
        action="store_true",
        help="Print verbose output showing Map, Shuffle & Sort, and Reduce steps."
    )
    parser.add_argument(
        "--infer-missing",
        action="store_true",
        help="Infer letter grades from numerical marks for records with missing grades."
    )
    args = parser.parse_args()

    try:
        run_pipeline(args.input, show_steps=args.show_steps, infer_missing=args.infer_missing)
    except Exception as e:
        print(f"Error during MapReduce execution: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
