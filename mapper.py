"""
Mapper Module for Semester Results MapReduce.

In the Map phase of the MapReduce paradigm:
1. Input records are ingested.
2. The mapper extracts the key of interest (the academic grade category).
3. The mapper validates the record.
4. An intermediate key-value pair `(grade, 1)` is emitted for each valid record.
"""

from typing import Dict, List, Optional, Tuple

# Supported academic grade categories
VALID_GRADES = {"S", "A", "B", "C", "D", "E", "F"}

# Standard academic grade mapping fallback based on percentage marks
GRADE_SCALE = [
    (90, "S"),
    (80, "A"),
    (70, "B"),
    (60, "C"),
    (50, "D"),
    (40, "E"),
    (0, "F"),
]


def infer_grade_from_marks(marks_str: str) -> Optional[str]:
    """Infers letter grade from numerical marks if grade column is missing or blank."""
    try:
        marks = float(marks_str.strip())
        if not (0 <= marks <= 100):
            return None
        for threshold, grade in GRADE_SCALE:
            if marks >= threshold:
                return grade
        return "F"
    except (ValueError, TypeError):
        return None


class GradeMapper:
    """Implements the Map phase for student examination grade distribution."""

    def __init__(self, infer_missing_grades: bool = False):
        self.infer_missing_grades = infer_missing_grades
        self.skipped_records: List[Dict[str, str]] = []

    def map_record(self, record: Dict[str, str]) -> Optional[Tuple[str, int]]:
        """
        Maps a single student record dictionary to an intermediate (grade, 1) tuple.

        Args:
            record: Dictionary containing student attributes (e.g., StudentID, Marks, Grade).

        Returns:
            Tuple of (grade, 1) if record is valid, or None if skipped/invalid.
        """
        raw_grade = record.get("Grade", "").strip().upper()
        student_id = record.get("StudentID", "UNKNOWN").strip()

        # Check if direct grade is present and valid
        if raw_grade in VALID_GRADES:
            return (raw_grade, 1)

        # Handle missing or invalid grade
        if self.infer_missing_grades and "Marks" in record:
            inferred = infer_grade_from_marks(record.get("Marks", ""))
            if inferred in VALID_GRADES:
                return (inferred, 1)

        # Record validation failure
        self.skipped_records.append({
            "StudentID": student_id,
            "Reason": f"Invalid or missing grade '{raw_grade}'",
            "Record": str(record)
        })
        return None

    def map(self, records: List[Dict[str, str]]) -> List[Tuple[str, int]]:
        """
        Executes the Map phase across all student records.

        Args:
            records: List of student record dictionaries.

        Returns:
            List of intermediate key-value pairs [(grade_1, 1), (grade_2, 1), ...].
        """
        intermediate_pairs: List[Tuple[str, int]] = []
        for record in records:
            pair = self.map_record(record)
            if pair is not None:
                intermediate_pairs.append(pair)
        return intermediate_pairs
