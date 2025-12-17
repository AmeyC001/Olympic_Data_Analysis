from __future__ import annotations

from typing import Any
from dataframe_loader import DataFrame
from collections import Counter


def _parse_csv(text: str, delimiter: str = ",", quote_char: str = '"') -> list[list[str]]:
    rows: list[list[str]] = []
    row: list[str] = []
    field_chars: list[str] = []
    in_quotes = False

    i = 0
    n = len(text)
    while i < n:
        ch = text[i]

        if in_quotes:
            if ch == quote_char:
                nxt = text[i + 1] if i + 1 < n else None
                if nxt == quote_char:
                    field_chars.append(quote_char)
                    i += 2
                    continue
                in_quotes = False
                i += 1
                continue
            field_chars.append(ch)
            i += 1
            continue

        # not in quotes
        if ch == quote_char:
            if field_chars:
                raise ValueError("Unexpected quote inside unquoted field")
            in_quotes = True
            i += 1
            continue

        if ch == delimiter:
            row.append("".join(field_chars))
            field_chars = []
            i += 1
            continue

        if ch == "\r" or ch == "\n":
            # end of record
            row.append("".join(field_chars))
            field_chars = []
            # consume CRLF as a single newline
            if ch == "\r" and i + 1 < n and text[i + 1] == "\n":
                i += 2
            else:
                i += 1
            # skip adding completely empty rows for blank lines
            if any(cell != "" for cell in row) or len(row) > 1:
                rows.append(row)
            row = []
            continue

        field_chars.append(ch)
        i += 1

    # finalize last record if any
    if in_quotes:
        raise ValueError("Unterminated quoted field at end of input")

    # add final field/row if content exists
    if field_chars or row:
        row.append("".join(field_chars))
        rows.append(row)

    return rows


def read_csv(source: Any, delimiter: str = ",", quote_char: str = '"', has_header: bool = False) -> list[list[str]] | list[dict[str, str]]:
    """Read CSV from a file path or file-like object.

    - Supports RFC4180-style quoting with escaped quotes by doubling.
    - Handles CRLF and LF newlines.
    - If has_header=True, returns list of dicts keyed by the header row.
    """

    # Load entire content to support records spanning multiple lines
    if hasattr(source, "read"):
        data = source.read()
    elif isinstance(source, str):
        with open(source, "rb") as f:
            data = f.read()
    else:
        raise TypeError("source must be a file path or file-like object")

    if not isinstance(data, str):
        data = data.decode("utf-8")

    rows = _parse_csv(data, delimiter=delimiter, quote_char=quote_char)

    if not has_header:
        return rows

    if not rows:
        return []

    header = rows[0]
    dict_rows: list[dict[str, str]] = []
    for r in rows[1:]:
        # pad or truncate to header length
        if len(r) < len(header):
            padded = r + [""] * (len(header) - len(r))
        else:
            padded = r[: len(header)]
        dict_rows.append({header[i]: padded[i] for i in range(len(header))})

    return dict_rows



def is_numeric(value: str) -> bool:
    """Check if a string value can be converted to a number."""
    if not value or value == "NA":
        return False
    try:
        float(value)
    except ValueError:
        return False
    else:
        return True


def calculate_statistics(data: list[dict[str, str]], dataset_name: str) -> None:
    """Calculate and display basic statistics for a dataset."""

    print("\n" + "="*80)
    print(f"DATASET: {dataset_name}")
    print("="*80 + "\n")

    if not data:
        print("Empty dataset!")
        return

    # Basic info
    num_rows = len(data)
    columns = list(data[0].keys())
    num_columns = len(columns)

    print("BASIC INFORMATION")
    print(f"   Rows: {num_rows:,}")
    print(f"   Columns: {num_columns}")
    print(f"\n   Column names: {', '.join(columns)}")

    # Analyze each column
    print("\nCOLUMN STATISTICS\n")

    for col in columns:
        values = [row[col] for row in data]

        # Count missing values
        missing = sum(1 for v in values if not v or v == "NA")
        non_missing = num_rows - missing

        # Check if column is numeric
        non_missing_values = [v for v in values if v and v != "NA"]
        numeric_count = sum(1 for v in non_missing_values if is_numeric(v))
        is_numeric_col = numeric_count > len(non_missing_values) * 0.8  # 80% threshold

        print(f"   {col}:")
        print(f"      Type: {'Numeric' if is_numeric_col else 'Categorical/Text'}")
        print(f"      Non-null: {non_missing:,} ({non_missing/num_rows*100:.1f}%)")
        print(f"      Missing: {missing:,} ({missing/num_rows*100:.1f}%)")

        if is_numeric_col:
            # Calculate numeric statistics
            numeric_values = [float(v) for v in non_missing_values if is_numeric(v)]
            if numeric_values:
                print(f"      Min: {min(numeric_values):.2f}")
                print(f"      Max: {max(numeric_values):.2f}")
                print(f"      Mean: {sum(numeric_values)/len(numeric_values):.2f}")
                # Calculate median
                sorted_values = sorted(numeric_values)
                mid = len(sorted_values) // 2
                if len(sorted_values) % 2 == 0:
                    median = (sorted_values[mid-1] + sorted_values[mid]) / 2
                else:
                    median = sorted_values[mid]
                print(f"      Median: {median:.2f}")
        else:
            # Calculate categorical statistics
            unique_values = len(set(non_missing_values))
            print(f"      Unique values: {unique_values:,}")

            # Show top 5 most common values
            if unique_values <= 20:
                value_counts = Counter(non_missing_values)
                most_common = value_counts.most_common(5)
                print("      Top values:")
                for val, count in most_common:
                    display_val = val[:30] + "..." if len(val) > 30 else val
                    print(f"         '{display_val}': {count:,} ({count/non_missing*100:.1f}%)")

        print()

    # Sample data
    print("SAMPLE DATA (first 3 rows):\n")
    for i, row in enumerate(data[:3], 1):
        print(f"   Row {i}:")
        for col, val in row.items():
            display_val = val[:50] + "..." if len(val) > 50 else val
            print(f"      {col}: {display_val}")

def load_data_as_dataframe(csv_path: str) -> DataFrame:
    """Load CSV data and convert to DataFrame."""
    rows = read_csv(csv_path, has_header=True)

    if not rows:
        return DataFrame({})

    # Convert list of dicts to dict of lists
    columns = list(rows[0].keys())
    data = {col: [row[col] for row in rows] for col in columns}

    return DataFrame(data)


if __name__ == "__main__":
    data = read_csv('RC_RCF_data.csv')
    print(data[0])

