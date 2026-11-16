# MapReduce on Semester Examination Results

An academic implementation of the **MapReduce programming model** developed in pure Python to analyze university end-semester examination results and calculate the distribution of student grades.

---

## 1. Project Objective

The objective of this project is to demonstrate the fundamentals of the MapReduce paradigm (Map, Shuffle & Sort, Reduce) without relying on external heavy distributed systems. The application:
- Ingests student examination score records from a CSV dataset.
- Filters and maps records to intermediate key-value pairs `(grade, 1)`.
- Implements an explicit **Shuffle and Sort** phase to partition and group records into canonical grade hierarchies.
- Reduces grouped occurrences to compute total student counts across all grade categories (`S`, `A`, `B`, `C`, `D`, `E`, and `F`).
- Handles malformed data, invalid grades, and missing values gracefully.

---

## 2. Technologies Used

- **Language:** Python 3 (standard library only)
- **Key Modules:** `csv`, `collections.defaultdict`, `typing`, `argparse`, `sys`, `os`
- **Dependencies:** None (No external packages required)

---

## 3. Grade Scale & Scheme

The system supports the standard university grading hierarchy:

| Grade | Classification | Percentage Band | Description |
| :---: | :--- | :---: | :--- |
| **S** | Outstanding | $\ge 90\%$ | Highest level of academic distinction |
| **A** | Excellent | $80 - 89\%$ | Superior performance |
| **B** | Very Good | $70 - 79\%$ | Above average competence |
| **C** | Good | $60 - 69\%$ | Sound understanding of subject matter |
| **D** | Average | $50 - 59\%$ | Satisfactory performance |
| **E** | Pass | $40 - 49\%$ | Minimum qualifying threshold |
| **F** | Fail | $< 40\%$ | Unsatisfactory / Did not meet passing threshold |

---

## 4. File Structure

```
Map-reduce-on-semester-results/
├── README.md              # Project documentation and theoretical background
├── sample_results.csv     # Sample student examination results dataset
├── mapper.py              # Mapper class implementing the Map phase
├── reducer.py             # Reducer class implementing the Reduce phase
├── main.py                # Pipeline orchestrator (Input -> Map -> Shuffle/Sort -> Reduce -> Display)
└── .gitignore             # Standard Python gitignore rules
```

---

## 5. How the Program Works

The architecture mirrors Google's classic MapReduce paper:

```
[ Input CSV ]
      │
      ▼
┌─────────────┐
│  MAP PHASE  │  mapper.py: Reads raw records, validates grades,
└──────┬──────┘  emits intermediate pairs: [(Grade, 1), ...]
      │
      ▼
┌──────────────────┐
│  SHUFFLE & SORT  │  main.py: Groups keys by grade and sorts into
└──────┬───────────┘  canonical order: [(Grade, [1, 1, 1, ...]), ...]
      │
      ▼
┌──────────────┐
│ REDUCE PHASE │  reducer.py: Sums occurrences per key:
└──────┬───────┘  [(Grade, total_count), ...]
      │
      ▼
[ Tabular Distribution Report & Metrics ]
```

1. **Map Phase (`mapper.py`):**
   Reads each row dictionary, checks validity against `{S, A, B, C, D, E, F}`, and emits intermediate key-value pairs `(Grade, 1)`. Invalid rows are redirected to a skipped-records log.
2. **Shuffle and Sort Phase (`main.py`):**
   Partitions and aggregates the intermediate values associated with each key into buckets and orders the keys according to the academic grade sequence: `S -> A -> B -> C -> D -> E -> F`.
3. **Reduce Phase (`reducer.py`):**
   Aggregates the counts for each key: $\sum \text{values}$, producing the final student count for each grade category.
4. **Presentation Phase (`main.py`):**
   Renders a formatted ASCII distribution chart and summary metrics.

---

## 6. Execution Instructions

Run the application using Python:

```bash
# Basic run with the included sample dataset
python main.py

# Verbose execution showing intermediate Map, Shuffle, and Reduce steps
python main.py --show-steps

# Run with a custom input file
python main.py --input /path/to/custom_results.csv

# Run with automatic mark-to-grade inference for records missing explicit grade values
python main.py --infer-missing
```

---

## 7. Expected Output

```
[MapReduce Pipeline] Ingesting results from: ...\sample_results.csv
[Input Phase] Loaded 42 records from CSV.
[Map Phase Complete] Emitted 40 intermediate key-value pairs.
[Shuffle & Sort Phase Complete] Grouped into 7 unique grade categories.
[Reduce Phase Complete] Aggregation calculated for 7 grade categories.

======================================================================
        END-SEMESTER EXAMINATION RESULTS - MAPREDUCE ANALYSIS
======================================================================
Grade    | Description              | Count    | Percentage | Distribution
----------------------------------------------------------------------
  S      | Outstanding (>=90%)      | 5        |  12.50%    | ####
  A      | Excellent   (80-89%)     | 7        |  17.50%    | #####
  B      | Very Good   (70-79%)     | 7        |  17.50%    | #####
  C      | Good        (60-69%)     | 6        |  15.00%    | #####
  D      | Average     (50-59%)     | 6        |  15.00%    | #####
  E      | Pass        (40-49%)     | 5        |  12.50%    | ####
  F      | Fail        (<40%)       | 4        |  10.00%    | ###
----------------------------------------------------------------------
Total Valid Student Records Evaluated : 40
Invalid / Skipped Records Logged     : 2
======================================================================

[Log of Skipped / Invalid Records]
  1. Student ID: ST1041, Reason: Invalid or missing grade 'X'
  2. Student ID: ST1042, Reason: Invalid or missing grade ''
```
