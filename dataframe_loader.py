"""
Custom DataFrame implementation with filtering, projection, grouping, aggregation, and join operations.

This module provides a DataFrame class that stores data as a dictionary where each column
is a key-value pair with values stored as Python lists or numpy arrays.
"""

from typing import Any, Callable, Dict, List, Union, Optional
import numpy as np
from collections import defaultdict


class DataFrame:
    """
    A DataFrame class for in-memory data manipulation.

    Data is stored as a dictionary where:
    - Keys are column names (strings)
    - Values are lists or numpy arrays containing column data

    Example:
        >>> df = DataFrame({
        ...     'Name': ['Alice', 'Bob', 'Charlie'],
        ...     'Age': [25, 30, 35],
        ...     'Salary': [50000, 60000, 70000]
        ... })
    """

    def __init__(self, data: Dict[str, Union[List, np.ndarray]]):
        """
        Initialize DataFrame with a dictionary of columns.

        Args:
            data: Dictionary mapping column names to lists or numpy arrays

        Raises:
            ValueError: If columns have different lengths
        """
        if not data:
            self._data = {}
            self._length = 0
            return

        # Validate that all columns have the same length
        lengths = [len(col) for col in data.values()]
        if len(set(lengths)) > 1:
            raise ValueError("All columns must have the same length")

        # Store data as dictionary with lists
        self._data = {col: list(values) for col, values in data.items()}
        self._length = lengths[0] if lengths else 0

    def __len__(self) -> int:
        """Return the number of rows in the DataFrame."""
        return self._length

    def __getitem__(self, key: Union[str, List[str]]) -> Union[List, 'DataFrame']:
        """
        Retrieve column(s) using bracket notation.

        Args:
            key: Column name (string) or list of column names

        Returns:
            If key is a string: list of values in that column
            If key is a list: new DataFrame with selected columns (projection)

        Example:
            >>> df['Name']  # Returns list of names
            >>> df[['Name', 'Age']]  # Returns new DataFrame with Name and Age columns
        """
        if isinstance(key, str):
            if key not in self._data:
                raise KeyError(f"Column '{key}' not found")
            return self._data[key]
        elif isinstance(key, list):
            return self.project(key)
        else:
            raise TypeError("Key must be a string or list of strings")

    def __repr__(self) -> str:
        """String representation of the DataFrame."""
        if not self._data:
            return "DataFrame(empty)"

        # Get column names and calculate widths
        columns = list(self._data.keys())
        col_widths = {col: max(len(col), max(len(str(val)) for val in self._data[col]))
                      for col in columns}

        # Build header
        header = " | ".join(col.ljust(col_widths[col]) for col in columns)
        separator = "-+-".join("-" * col_widths[col] for col in columns)

        # Build rows (limit to first 10 rows)
        rows = []
        for i in range(min(10, self._length)):
            row = " | ".join(str(self._data[col][i]).ljust(col_widths[col]) for col in columns)
            rows.append(row)

        result = f"DataFrame ({self._length} rows)\n{header}\n{separator}\n" + "\n".join(rows)

        if self._length > 10:
            result += f"\n... ({self._length - 10} more rows)"

        return result

    @property
    def columns(self) -> List[str]:
        """Return list of column names."""
        return list(self._data.keys())

    @property
    def shape(self) -> tuple:
        """Return (rows, columns) shape of the DataFrame."""
        return (self._length, len(self._data))

    def filter(self, condition: Callable[[Dict[str, Any]], bool]) -> 'DataFrame':
        """
        Filter rows based on a condition function.

        Args:
            condition: Function that takes a row dict and returns True/False

        Returns:
            New DataFrame with filtered rows

        Example:
            >>> df.filter(lambda row: row['Age'] > 25)
        """
        filtered_data = defaultdict(list)

        for i in range(self._length):
            row = {col: self._data[col][i] for col in self._data}
            if condition(row):
                for col in self._data:
                    filtered_data[col].append(self._data[col][i])

        return DataFrame(dict(filtered_data))

    def project(self, columns: List[str]) -> 'DataFrame':
        """
        Select specific columns (projection operation).

        Args:
            columns: List of column names to select

        Returns:
            New DataFrame with only the specified columns

        Example:
            >>> df.project(['Name', 'Age'])
        """
        if not all(col in self._data for col in columns):
            missing = [col for col in columns if col not in self._data]
            raise KeyError(f"Columns not found: {missing}")

        return DataFrame({col: self._data[col] for col in columns})

    def group_by(self, columns: Union[str, List[str]]) -> 'GroupedDataFrame':
        """
        Group DataFrame by one or more columns.

        Args:
            columns: Column name(s) to group by

        Returns:
            GroupedDataFrame object for aggregation operations

        Example:
            >>> df.group_by('Department').agg({'Salary': 'mean'})
            >>> df.group_by(['Department', 'Role']).agg({'Salary': ['mean', 'sum']})
        """
        if isinstance(columns, str):
            columns = [columns]

        return GroupedDataFrame(self, columns)

    def agg(self, aggregations: Dict[str, Union[str, List[str], Callable]]) -> 'DataFrame':
        """
        Perform aggregation operations on the entire DataFrame.

        Args:
            aggregations: Dict mapping column names to aggregation functions
                         Functions can be: 'sum', 'mean', 'min', 'max', 'count', or callable

        Returns:
            New DataFrame with aggregated results

        Example:
            >>> df.agg({'Age': 'mean', 'Salary': ['min', 'max']})
        """
        result = {}

        for col, funcs in aggregations.items():
            if col not in self._data:
                raise KeyError(f"Column '{col}' not found")

            if isinstance(funcs, str):
                funcs = [funcs]
            elif callable(funcs):
                funcs = [funcs]

            for func in funcs:
                agg_value = self._apply_aggregation(self._data[col], func)
                if len(funcs) > 1:
                    result_key = f"{col}_{func if isinstance(func, str) else func.__name__}"
                else:
                    result_key = col
                result[result_key] = [agg_value]

        return DataFrame(result)

    def _apply_aggregation(self, values: List, func: Union[str, Callable]) -> Any:
        """Apply an aggregation function to a list of values."""
        if isinstance(func, str):
            func_map = {
                'sum': sum,
                'mean': lambda x: sum(x) / len(x) if len(x) > 0 else 0,
                'min': min,
                'max': max,
                'count': len,
                'std': lambda x: np.std(x) if len(x) > 0 else 0,
                'var': lambda x: np.var(x) if len(x) > 0 else 0,
            }
            if func not in func_map:
                raise ValueError(f"Unknown aggregation function: {func}")
            func = func_map[func]

        return func(values)

    def join(self, other: 'DataFrame', on: Union[str, List[str]],
             how: str = 'inner', suffixes: tuple = ('_x', '_y')) -> 'DataFrame':
        """
        Join this DataFrame with another DataFrame.

        Args:
            other: DataFrame to join with
            on: Column name(s) to join on
            how: Join type - 'inner', 'left', 'right', or 'outer'
            suffixes: Suffixes to add to overlapping column names

        Returns:
            New DataFrame with joined data

        Example:
            >>> df1.join(df2, on='ID', how='inner')
            >>> df1.join(df2, on=['ID', 'Date'], how='left')
        """
        if isinstance(on, str):
            on = [on]

        # Validate join columns exist
        for col in on:
            if col not in self._data:
                raise KeyError(f"Column '{col}' not found in left DataFrame")
            if col not in other._data:
                raise KeyError(f"Column '{col}' not found in right DataFrame")

        # Build index for the right DataFrame
        right_index = defaultdict(list)
        for i in range(len(other)):
            key = tuple(other._data[col][i] for col in on)
            right_index[key].append(i)

        # Determine overlapping columns (excluding join keys)
        left_cols = set(self._data.keys()) - set(on)
        right_cols = set(other._data.keys()) - set(on)
        overlap = left_cols & right_cols

        # Prepare result columns
        result_data = defaultdict(list)

        # Perform join based on type
        if how in ['inner', 'left']:
            for i in range(self._length):
                key = tuple(self._data[col][i] for col in on)
                right_matches = right_index.get(key, [])

                if right_matches:
                    for right_idx in right_matches:
                        # Add join keys
                        for col in on:
                            result_data[col].append(self._data[col][i])
                        # Add left columns
                        for col in left_cols:
                            col_name = col + suffixes[0] if col in overlap else col
                            result_data[col_name].append(self._data[col][i])
                        # Add right columns
                        for col in right_cols:
                            col_name = col + suffixes[1] if col in overlap else col
                            result_data[col_name].append(other._data[col][right_idx])
                elif how == 'left':
                    # Add join keys
                    for col in on:
                        result_data[col].append(self._data[col][i])
                    # Add left columns
                    for col in left_cols:
                        col_name = col + suffixes[0] if col in overlap else col
                        result_data[col_name].append(self._data[col][i])
                    # Add None for right columns
                    for col in right_cols:
                        col_name = col + suffixes[1] if col in overlap else col
                        result_data[col_name].append(None)

        if how == 'right':
            left_index = defaultdict(list)
            for i in range(self._length):
                key = tuple(self._data[col][i] for col in on)
                left_index[key].append(i)

            for i in range(len(other)):
                key = tuple(other._data[col][i] for col in on)
                left_matches = left_index.get(key, [])

                if left_matches:
                    for left_idx in left_matches:
                        # Add join keys
                        for col in on:
                            result_data[col].append(other._data[col][i])
                        # Add left columns
                        for col in left_cols:
                            col_name = col + suffixes[0] if col in overlap else col
                            result_data[col_name].append(self._data[col][left_idx])
                        # Add right columns
                        for col in right_cols:
                            col_name = col + suffixes[1] if col in overlap else col
                            result_data[col_name].append(other._data[col][i])
                else:
                    # Add join keys
                    for col in on:
                        result_data[col].append(other._data[col][i])
                    # Add None for left columns
                    for col in left_cols:
                        col_name = col + suffixes[0] if col in overlap else col
                        result_data[col_name].append(None)
                    # Add right columns
                    for col in right_cols:
                        col_name = col + suffixes[1] if col in overlap else col
                        result_data[col_name].append(other._data[col][i])

        if how == 'outer':
            # Combine inner join with unmatched rows from both sides
            matched_left = set()
            matched_right = set()

            # Inner part
            for i in range(self._length):
                key = tuple(self._data[col][i] for col in on)
                right_matches = right_index.get(key, [])

                if right_matches:
                    matched_left.add(i)
                    for right_idx in right_matches:
                        matched_right.add(right_idx)
                        # Add join keys
                        for col in on:
                            result_data[col].append(self._data[col][i])
                        # Add left columns
                        for col in left_cols:
                            col_name = col + suffixes[0] if col in overlap else col
                            result_data[col_name].append(self._data[col][i])
                        # Add right columns
                        for col in right_cols:
                            col_name = col + suffixes[1] if col in overlap else col
                            result_data[col_name].append(other._data[col][right_idx])

            # Unmatched left rows
            for i in range(self._length):
                if i not in matched_left:
                    for col in on:
                        result_data[col].append(self._data[col][i])
                    for col in left_cols:
                        col_name = col + suffixes[0] if col in overlap else col
                        result_data[col_name].append(self._data[col][i])
                    for col in right_cols:
                        col_name = col + suffixes[1] if col in overlap else col
                        result_data[col_name].append(None)

            # Unmatched right rows
            for i in range(len(other)):
                if i not in matched_right:
                    for col in on:
                        result_data[col].append(other._data[col][i])
                    for col in left_cols:
                        col_name = col + suffixes[0] if col in overlap else col
                        result_data[col_name].append(None)
                    for col in right_cols:
                        col_name = col + suffixes[1] if col in overlap else col
                        result_data[col_name].append(other._data[col][i])

        return DataFrame(dict(result_data))


class GroupedDataFrame:
    """
    Represents a grouped DataFrame for aggregation operations.
    """

    def __init__(self, df: DataFrame, group_cols: List[str]):
        """
        Initialize grouped DataFrame.

        Args:
            df: Original DataFrame
            group_cols: Columns to group by
        """
        self._df = df
        self._group_cols = group_cols
        self._groups = self._build_groups()

    def _build_groups(self) -> Dict[tuple, List[int]]:
        """Build index of groups."""
        groups = defaultdict(list)
        for i in range(len(self._df)):
            key = tuple(self._df._data[col][i] for col in self._group_cols)
            groups[key].append(i)
        return dict(groups)

    def agg(self, aggregations: Dict[str, Union[str, List[str], Callable]]) -> DataFrame:
        """
        Perform aggregation on grouped data.

        Args:
            aggregations: Dict mapping column names to aggregation functions

        Returns:
            New DataFrame with aggregated results

        Example:
            >>> df.group_by('Department').agg({'Salary': 'mean', 'Age': ['min', 'max']})
        """
        result_data = defaultdict(list)

        for group_key, indices in self._groups.items():
            # Add group key columns
            for i, col in enumerate(self._group_cols):
                result_data[col].append(group_key[i])

            # Perform aggregations
            for col, funcs in aggregations.items():
                if col not in self._df._data:
                    raise KeyError(f"Column '{col}' not found")

                if isinstance(funcs, str):
                    funcs = [funcs]
                elif callable(funcs):
                    funcs = [funcs]

                # Get values for this group
                group_values = [self._df._data[col][idx] for idx in indices]

                for func in funcs:
                    agg_value = self._df._apply_aggregation(group_values, func)
                    if len(funcs) > 1:
                        result_key = f"{col}_{func if isinstance(func, str) else func.__name__}"
                    else:
                        result_key = col
                    result_data[result_key].append(agg_value)

        return DataFrame(dict(result_data))


# Example usage and tests
if __name__ == "__main__":
    # Create sample DataFrame
    df = DataFrame({
        'Name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'Department': ['Engineering', 'Sales', 'Engineering', 'Sales', 'Engineering'],
        'Age': [25, 30, 35, 28, 32],
        'Salary': [75000, 65000, 85000, 70000, 80000]
    })

    print("Original DataFrame:")
    print(df)
    print()

    # Test __getitem__
    print("Names (using df['Name']):")
    print(df['Name'])
    print()

    # Test projection
    print("Projection (Name and Age):")
    print(df[['Name', 'Age']])
    print()

    # Test filtering
    print("Filter (Age > 28):")
    filtered = df.filter(lambda row: row['Age'] > 28)
    print(filtered)
    print()

    # Test group by and aggregation
    print("Group by Department with mean Salary:")
    grouped = df.group_by('Department').agg({'Salary': 'mean', 'Age': ['min', 'max']})
    print(grouped)
    print()

    # Test join
    df2 = DataFrame({
        'Name': ['Alice', 'Bob', 'Frank'],
        'Bonus': [5000, 3000, 4000]
    })

    print("Join on Name (inner):")
    joined = df.join(df2, on='Name', how='inner')
    print(joined)
    print()

    print("Join on Name (left):")
    joined_left = df.join(df2, on='Name', how='left')
    print(joined_left)
