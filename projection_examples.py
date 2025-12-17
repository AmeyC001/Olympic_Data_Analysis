"""
Projection Examples: Extract athlete names and medal counts.
Demonstrates projection operation using DataFrame from csv_functions.py
"""

from __future__ import annotations

from dataframe_loader import DataFrame
from utils import load_data_as_dataframe

class ProjectionApplication():
    def __init__(self, athlete_data_path):
        self.athletes_df = load_data_as_dataframe(athlete_data_path)

    def get_basic_projection(self) -> None:
        """
        Example 1: Basic column projection
        Demonstrates: df[column_list] to select specific columns
        """

        print("\n" + "="*80)
        print("EXAMPLE 1: BASIC PROJECTION - Selecting Specific Columns")
        print("="*80 + "\n")

        
        print(f"Original dataset: {len(self.athletes_df):,} rows, {len(self.athletes_df.columns)} columns")
        print(f"All columns: {', '.join(self.athletes_df.columns)}\n")

        # PROJECTION: Select only Name and Medal columns
        print("OPERATION: df[['Name', 'Medal']]")
        print("Projecting to select only 'Name' and 'Medal' columns...\n")

        name_medal_df = self.athletes_df[['Name', 'Medal']]

        print(f"Result: {len(name_medal_df):,} rows, {len(name_medal_df.columns)} columns")
        print(f"Projected columns: {', '.join(name_medal_df.columns)}\n")

        # Show sample data
        print("Sample data (first 10 rows):")
        print("-" * 80)
        for i in range(min(10, len(name_medal_df))):
            name = name_medal_df['Name'][i]
            medal = name_medal_df['Medal'][i]
            medal_display = medal if medal and medal != 'NA' else 'No medal'
            print(f"{i+1:3d}. {name:<40} | {medal_display}")


    def get_multi_column_projection(self) -> None:
        """
        Example 2: Multi-column projection
        Extract athlete profile: Name, Year, Sport, Medal
        """

        print("\n\n" + "="*80)
        print("EXAMPLE 2: MULTI-COLUMN PROJECTION - Athlete Profiles")
        print("="*80 + "\n")

        # PROJECTION: Select multiple columns for athlete profile
        print("OPERATION: df[['Name', 'Year', 'Sport', 'Event', 'Medal']]")
        print("Projecting to create athlete performance profiles...\n")

        profile_df = self.athletes_df[['Name', 'Year', 'Sport', 'Event', 'Medal']]

        print(f"Result: {len(profile_df):,} rows, {len(profile_df.columns)} columns")
        print(f"Columns: {', '.join(profile_df.columns)}\n")

        # Show medal winners only
        print("Sample: Medal-winning performances (first 15):")
        print("-" * 80)

        count = 0
        for i in range(len(profile_df)):
            medal = profile_df['Medal'][i]
            if medal and medal in ['Gold', 'Silver', 'Bronze']:
                name = profile_df['Name'][i]
                year = profile_df['Year'][i]
                sport = profile_df['Sport'][i]
                event = profile_df['Event'][i]

                print(f"{count+1:3d}. {name}")
                print(f"     Year: {year} | Medal: {medal}")
                print(f"     Sport: {sport}")
                print(f"     Event: {event[:60]}...")
                print()

                count += 1
                if count >= 15:
                    break


    def get_projection_with_filter(self) -> None:
        """
        Example 3: Projection after filtering
        Filter for gold medals, then project specific columns
        """

        print("\n" + "="*80)
        print("EXAMPLE 3: PROJECTION AFTER FILTERING - Gold Medalists")
        print("="*80 + "\n")

        # STEP 1: Filter for gold medals
        print("STEP 1: filter() - Filtering for gold medal winners...")
        gold_df = self.athletes_df.filter(lambda row: row['Medal'] == 'Gold')
        print(f"Result: {len(gold_df):,} gold medal records\n")

        # STEP 2: Project to select specific columns
        print("STEP 2: project() - Selecting 'Name', 'NOC', 'Year', 'Sport' columns...")
        gold_profile_df = gold_df[['Name', 'NOC', 'Year', 'Sport']]
        print(f"Result: {len(gold_profile_df):,} rows, {len(gold_profile_df.columns)} columns\n")

        print("Sample: Gold medalists (first 20):")
        print("-" * 80)

        for i in range(min(20, len(gold_profile_df))):
            name = gold_profile_df['Name'][i]
            noc = gold_profile_df['NOC'][i]
            year = gold_profile_df['Year'][i]
            sport = gold_profile_df['Sport'][i]

            print(f"{i+1:3d}. {name:<35} | {noc:3s} | {year:4s} | {sport}")


    def get_athlete_medal_counts(self) -> None:
        """
        Example 4: Projection + Aggregation - Medal counts per athlete
        """

        print("\n\n" + "="*80)
        print("EXAMPLE 4: ATHLETE MEDAL COUNTS - Projection + Aggregation")
        print("="*80 + "\n")

        # STEP 1: Filter for medal winners
        print("STEP 1: filter() - Filtering for medal winners only...")
        medal_winners = self.athletes_df.filter(
            lambda row: row['Medal'] in ['Gold', 'Silver', 'Bronze']
        )
        print(f"Result: {len(medal_winners):,} medal-winning performances\n")

        # STEP 2: Project relevant columns
        print("STEP 2: project() - Selecting 'Name', 'NOC', 'Medal' columns...")
        medals_df = medal_winners[['Name', 'NOC', 'Medal']]
        print(f"Result: {len(medals_df):,} rows, {len(medals_df.columns)} columns\n")

        # STEP 3: Group by athlete name
        print("STEP 3: group_by() - Grouping by athlete name...")
        grouped = medals_df.group_by('Name')
        print("Result: Athletes grouped for aggregation\n")

        # STEP 4: Aggregate to count medals
        print("STEP 4: agg() - Counting medals per athlete...")
        athlete_medal_counts = grouped.agg({'Medal': 'count'})
        print(f"Result: {len(athlete_medal_counts):,} unique athletes\n")

        # Sort by medal count (manual sorting)
        names = athlete_medal_counts['Name']
        counts = [int(c) for c in athlete_medal_counts['Medal']]

        # Create list of (name, count) tuples and sort
        athlete_data = list(zip(names, counts))
        athlete_data.sort(key=lambda x: x[1], reverse=True)

        # Display top medal winners
        print("="*80)
        print("TOP 30 ATHLETES BY TOTAL MEDAL COUNT")
        print("="*80 + "\n")

        for idx, (name, count) in enumerate(athlete_data[:30], 1):
            # Get NOC for this athlete
            noc = 'UNK'
            for i in range(len(medals_df)):
                if medals_df['Name'][i] == name:
                    noc = medals_df['NOC'][i]
                    break

            print(f"{idx:3d}. {name:<45} | {noc:3s} | {count:3d} medals")


    def get_projection_by_country(self) -> None:
        """
        Example 5: Project and count medals by country
        """

        print("\n\n" + "="*80)
        print("EXAMPLE 5: MEDAL COUNTS BY COUNTRY - Projection + Grouping")
        print("="*80 + "\n")

        # Filter for medal winners
        print("STEP 1: filter() - Filtering for medal winners...")
        medal_winners = self.athletes_df.filter(
            lambda row: row['Medal'] in ['Gold', 'Silver', 'Bronze']
        )
        print(f"Result: {len(medal_winners):,} medal-winning performances\n")

        # Project to NOC and Medal columns
        print("STEP 2: project() - Selecting 'NOC' and 'Medal' columns...")
        noc_medals_df = medal_winners[['NOC', 'Medal']]
        print(f"Result: {len(noc_medals_df):,} rows, {len(noc_medals_df.columns)} columns\n")

        # Group by NOC
        print("STEP 3: group_by() - Grouping by country (NOC)...")
        country_groups = noc_medals_df.group_by('NOC')
        print("Result: Countries grouped for aggregation\n")

        # Aggregate medal counts
        print("STEP 4: agg() - Counting medals per country...")
        country_medal_counts = country_groups.agg({'Medal': 'count'})
        print(f"Result: {len(country_medal_counts):,} countries\n")

        # Sort by medal count
        nocs = country_medal_counts['NOC']
        counts = [int(c) for c in country_medal_counts['Medal']]

        country_data = list(zip(nocs, counts))
        country_data.sort(key=lambda x: x[1], reverse=True)

        # Display top countries
        print("="*80)
        print("TOP 25 COUNTRIES BY TOTAL MEDAL COUNT")
        print("="*80 + "\n")

        for idx, (noc, count) in enumerate(country_data[:25], 1):
            print(f"{idx:3d}. {noc:3s} | {count:6,} medals")


    def get_sport_specific_projection(self) -> None:
        """
        Example 6: Sport-specific athlete medal counts
        """

        print("\n\n" + "="*80)
        print("EXAMPLE 6: SPORT-SPECIFIC MEDAL COUNTS - Complete Pipeline")
        print("="*80 + "\n")

        sport = "Swimming"

        # Complete pipeline
        print(f"Analyzing {sport} medal winners...\n")

        print(f"STEP 1: filter() - Sport = '{sport}' AND Medal winner...")
        sport_medals = self.athletes_df.filter(
            lambda row: row['Sport'] == sport and row['Medal'] in ['Gold', 'Silver', 'Bronze']
        )
        print(f"Result: {len(sport_medals):,} medal-winning performances in {sport}\n")

        print("STEP 2: project() - Select 'Name', 'NOC', 'Year', 'Medal'...")
        projected = sport_medals[['Name', 'NOC', 'Year', 'Medal']]
        print(f"Result: {len(projected):,} rows, {len(projected.columns)} columns\n")

        print("STEP 3: group_by() - Group by athlete name...")
        grouped = projected.group_by('Name')
        print("Result: Athletes grouped\n")

        print("STEP 4: agg() - Count medals per athlete...")
        athlete_counts = grouped.agg({'Medal': 'count'})
        print(f"Result: {len(athlete_counts):,} unique swimmers\n")

        # Sort and display
        names = athlete_counts['Name']
        counts = [int(c) for c in athlete_counts['Medal']]
        data = sorted(zip(names, counts), key=lambda x: x[1], reverse=True)

        print("="*80)
        print(f"TOP 20 {sport.upper()} MEDALISTS")
        print("="*80 + "\n")

        for idx, (name, count) in enumerate(data[:20], 1):
            # Get NOC
            noc = 'UNK'
            for i in range(len(projected)):
                if projected['Name'][i] == name:
                    noc = projected['NOC'][i]
                    break

            print(f"{idx:3d}. {name:<40} | {noc:3s} | {count:3d} medals")


def main():
    """Main function demonstrating projection operations."""

    print("\n" + "="*80)
    print("PROJECTION EXAMPLES - DataFrame Operations")
    print("="*80)
    print("\nDemonstrates:")
    print("  - Basic projection: df[['column1', 'column2']]")
    print("  - Multi-column projection")
    print("  - Projection after filtering")
    print("  - Projection with grouping and aggregation")
    print("="*80)

    # Run all examples
    pro_obj = ProjectionApplication(athlete_data_path = "athlete_events.csv")
    pro_obj.get_basic_projection()
    pro_obj.get_multi_column_projection()
    pro_obj.get_projection_with_filter()
    pro_obj.get_athlete_medal_counts()
    pro_obj.get_projection_by_country()
    pro_obj.get_sport_specific_projection()

    print("\n" + "="*80)
    print("All projection examples complete!")
    print("="*80)
    print("\nKey DataFrame operations demonstrated:")
    print("  1. df[columns] - Project/select specific columns")
    print("  2. df.filter() - Filter rows by condition")
    print("  3. df.group_by() - Group data by column(s)")
    print("  4. df.agg() - Aggregate grouped data")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
