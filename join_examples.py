"""
Join Operations: Connect athlete data with country names for readable reports.
Demonstrates join operation using DataFrame from csv_functions.py
"""

from __future__ import annotations

from dataframe_loader import DataFrame
from utils import load_data_as_dataframe



class JoinExamples():
    def __init__(self, athlete_data_path, noc_regions_path):
        self.athletes_df = load_data_as_dataframe(athlete_data_path)
        self.noc_df = load_data_as_dataframe(noc_regions_path)

    def get_basic_inner_join(self) -> None:
        """
        Example 1: Basic Inner Join
        Join athletes with NOC regions to get full country names
        """

        print("\n" + "="*80)
        print("EXAMPLE 1: BASIC INNER JOIN - Athletes with Country Names")
        print("="*80 + "\n")

        print(f"Athletes dataset: {len(self.athletes_df):,} rows")
        print(f"NOC regions dataset: {len(self.noc_df):,} rows")
        print(f"\nNOC columns: {', '.join(self.noc_df.columns)}\n")

        # Project to reduce columns before join
        print("STEP 1: project() - Selecting relevant athlete columns...")
        athlete_subset = self.athletes_df[['Name', 'NOC', 'Year', 'Sport', 'Medal']]
        print(f"Result: {len(athlete_subset.columns)} columns selected\n")

        # Perform inner join
        print("STEP 2: join() - Inner join on 'NOC' column...")
        print("Operation: athlete_subset.join(noc_df, on='NOC', how='inner')\n")

        joined_df = athlete_subset.join(self.noc_df, on='NOC', how='inner')

        print(f"Result: {len(joined_df):,} rows, {len(joined_df.columns)} columns")
        print(f"Columns: {', '.join(joined_df.columns)}\n")

        # Show sample with readable country names
        print("Sample: Athletes with full country names (first 20):")
        print("-" * 80)

        for i in range(min(20, len(joined_df))):
            name = joined_df['Name'][i]
            noc = joined_df['NOC'][i]
            region = joined_df['region'][i]
            year = joined_df['Year'][i]
            sport = joined_df['Sport'][i]

            print(f"{i+1:3d}. {name:<35} | {noc:3s} - {region:<25} | {year} | {sport}")


def main():
    """Main function demonstrating join operations."""

    print("\n" + "="*80)
    print("JOIN OPERATIONS - Connecting Athlete Data with Country Names")
    print("Using csv_functions.py DataFrame Implementation")
    print("="*80)
    print("\nDemonstrates:")
    print("  - Inner join: Only matching records")
    print("  - Left join: Keep all records from left table")
    print("  - Join with grouping and aggregation")
    print("  - Creating readable reports with joins")
    print("  - Handling column name conflicts with suffixes")
    print("="*80)

    # Run all examples
    join_object = JoinExamples(athlete_data_path="athlete_events.csv", noc_regions_path="noc_regions.csv")
    join_object.get_basic_inner_join()

    print("\n" + "="*80)
    print("Key DataFrame operations demonstrated:")
    print("  1. df.join(other, on='column', how='inner|left|right|outer')")
    print("  2. df.join(other, on='col', suffixes=('_x', '_y'))")
    print("  3. Combining join with filter, project, group_by, agg")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
