"""
Find athletes who competed in multiple Olympic Games.
Uses custom DataFrame implementation from csv_functions.py
"""

from __future__ import annotations

from dataframe_loader import DataFrame
from utils import load_data_as_dataframe


class MultipleOlympicAnalyzer():
    def __init__(self, athlete_data_path):
        self.athletes_df = load_data_as_dataframe(athlete_data_path)

    def find_multi_olympic_athletes(self, min_olympics: int = 2, limit: int = 50) -> None:
        """
        Find athletes who competed in multiple Olympic Games using DataFrame operations.

        Args:
            min_olympics: Minimum number of Olympics to filter for (default: 2)
            limit: Maximum number of athletes to display in detail
        """

        print("\n" + "="*80)
        print("LOADING OLYMPIC DATA")
        print("="*80 + "\n")

        # Load athlete events dataset
        print(f"Loaded {len(self.athletes_df):,} athlete event records\n")

        print("="*80)
        print("ANALYZING MULTI-OLYMPIC ATHLETES USING DATAFRAME OPERATIONS")
        print("="*80 + "\n")

        # Use DataFrame projection to get only needed columns
        print("Step 1: Projecting relevant columns...")
        relevant_df = self.athletes_df[['Name', 'Year', 'NOC', 'Sport', 'Event', 'Medal']]
        print(f"Selected {len(relevant_df.columns)} columns from dataset\n")

        # Group by athlete name to count unique years
        print("Step 2: Grouping by athlete name...")
        athlete_groups = relevant_df.group_by('Name')
        print("Grouped athletes for aggregation\n")

        # Get aggregated data - count of events per athlete
        print("Step 3: Aggregating athlete statistics...")
        athlete_stats = athlete_groups.agg({'Year': 'count'})
        print(f"Found {len(athlete_stats):,} unique athletes\n")

        # Now we need to count unique years per athlete (manual processing needed for unique count)
        # Extract data for analysis
        names = relevant_df['Name']
        years = relevant_df['Year']
        nocs = relevant_df['NOC']
        sports = relevant_df['Sport']
        events = relevant_df['Event']
        medals = relevant_df['Medal']

        # Build athlete profiles with unique year counting
        print("Step 4: Building athlete Olympic participation profiles...")
        athlete_data = {}

        for i in range(len(relevant_df)):
            name = names[i]
            year = years[i]
            noc = nocs[i]
            sport = sports[i]
            event = events[i]
            medal = medals[i]

            if name not in athlete_data:
                athlete_data[name] = {
                    'years': set(),
                    'noc': noc,
                    'sports': set(),
                    'events': [],
                    'medals': []
                }

            athlete_data[name]['years'].add(year)
            athlete_data[name]['sports'].add(sport)
            athlete_data[name]['events'].append(event)
            if medal and medal != 'NA':
                athlete_data[name]['medals'].append(medal)

        print(f"Completed profiling for {len(athlete_data):,} athletes\n")

        # Filter for athletes with multiple Olympics
        print(f"Step 5: Filtering for athletes competing in {min_olympics}+ Olympics...")
        multi_olympic_athletes = {
            name: data for name, data in athlete_data.items()
            if len(data['years']) >= min_olympics
        }
        print(f"Found {len(multi_olympic_athletes):,} multi-Olympic athletes\n")

        # Demonstrate DataFrame filter operation on original data
        print("Step 6: Using DataFrame filter to get sample of multi-Olympic data...")
        # Get list of multi-Olympic athlete names
        multi_olympic_names = list(multi_olympic_athletes.keys())[:100]  # Sample for demo

        # Filter original dataframe for these athletes
        filtered_df = relevant_df.filter(lambda row: row['Name'] in multi_olympic_names)
        print(f"Filtered dataset contains {len(filtered_df):,} records from sample athletes\n")

        if not multi_olympic_athletes:
            print("No athletes found matching criteria\n")
            return

        # Sort by number of Olympics (descending)
        sorted_athletes = sorted(
            multi_olympic_athletes.items(),
            key=lambda x: len(x[1]['years']),
            reverse=True
        )

        # Statistics
        max_olympics = max(len(data['years']) for data in multi_olympic_athletes.values())
        avg_olympics = sum(len(data['years']) for data in multi_olympic_athletes.values()) / len(multi_olympic_athletes)

        print("="*80)
        print("SUMMARY STATISTICS")
        print("="*80 + "\n")
        print(f"Athletes competing in {min_olympics}+ Olympics: {len(multi_olympic_athletes):,}")
        print(f"Maximum Olympics by one athlete: {max_olympics}")
        print(f"Average Olympics per athlete: {avg_olympics:.1f}\n")

        # Olympics distribution
        olympics_counts = {}
        for data in multi_olympic_athletes.values():
            num_olympics = len(data['years'])
            olympics_counts[num_olympics] = olympics_counts.get(num_olympics, 0) + 1

        print("Distribution by number of Olympics:")
        for num in sorted(olympics_counts.keys(), reverse=True):
            count = olympics_counts[num]
            print(f"   {num} Olympics: {count:,} athletes")

        # Display top athletes
        print("\n" + "="*80)
        print(f"TOP MULTI-OLYMPIC ATHLETES (showing up to {limit})")
        print("="*80 + "\n")

        for idx, (name, data) in enumerate(sorted_athletes[:limit], 1):
            num_olympics = len(data['years'])
            year_list = sorted(data['years'])
            year_range = f"{year_list[0]} - {year_list[-1]}"
            noc = data['noc']
            sports = sorted(data['sports'])
            num_events = len(data['events'])
            num_medals = len(data['medals'])

            print(f"{idx}. {name}")
            print(f"   Country: {noc}")
            print(f"   Olympics: {num_olympics} ({year_range})")
            print(f"   Years: {', '.join(year_list)}")
            print(f"   Sports: {', '.join(sports)}")
            print(f"   Total events competed: {num_events}")

            if num_medals > 0:
                gold = data['medals'].count('Gold')
                silver = data['medals'].count('Silver')
                bronze = data['medals'].count('Bronze')
                print(f"   Medals: {num_medals} total (Gold: {gold}, Silver: {silver}, Bronze: {bronze})")
            else:
                print("Medals: None")

            print()

        if len(sorted_athletes) > limit:
            print(f"... and {len(sorted_athletes) - limit:,} more multi-Olympic athletes\n")



    def analyze_sport_with_dataframe_ops(self, sport_name: str, min_olympics: int = 2) -> None:
        """
        Analyze multi-Olympic athletes in a specific sport using DataFrame operations.
        Demonstrates: filter, project, group_by, agg from csv_functions.py

        Args:
            sport_name: Name of the sport to analyze
            min_olympics: Minimum number of Olympics
        """

        print("\n" + "="*80)
        print(f"MULTI-OLYMPIC ATHLETES IN {sport_name.upper()}")
        print("Demonstrating DataFrame Operations from csv_functions.py")
        print("="*80 + "\n")

        # OPERATION 1: FILTER - Filter for specific sport
        print(f"OPERATION 1: filter() - Filtering for {sport_name}...")
        sport_df = self.athletes_df.filter(lambda row: row['Sport'] == sport_name)
        print(f"Result: {len(sport_df):,} records in {sport_name}\n")

        if len(sport_df) == 0:
            print(f"No data found for sport: {sport_name}\n")
            return

        # OPERATION 2: PROJECT - Select specific columns
        print("OPERATION 2: project() - Selecting relevant columns...")
        projected_df = sport_df[['Name', 'Year', 'NOC', 'Event', 'Medal']]
        print(f"Result: {len(projected_df.columns)} columns selected: {', '.join(projected_df.columns)}\n")

        # OPERATION 3: GROUP BY - Group by athlete name
        print("OPERATION 3: group_by() - Grouping by athlete name...")
        athlete_groups = projected_df.group_by('Name')
        print("Result: Athletes grouped for aggregation\n")

        # OPERATION 4: AGGREGATE - Count events per athlete
        print("OPERATION 4: agg() - Counting events per athlete...")
        athlete_counts = athlete_groups.agg({'Year': 'count'})
        print(f"Result: Aggregated statistics for {len(athlete_counts):,} unique athletes\n")

        # Analyze unique years per athlete (requires manual processing)
        print("Additional Analysis: Counting unique Olympic years per athlete...")
        names = projected_df['Name']
        years = projected_df['Year']
        medals = projected_df['Medal']
        nocs = projected_df['NOC']

        athlete_olympics = {}
        for i in range(len(projected_df)):
            name = names[i]
            year = years[i]

            if name not in athlete_olympics:
                athlete_olympics[name] = {
                    'years': set(),
                    'noc': nocs[i],
                    'medals': []
                }

            athlete_olympics[name]['years'].add(year)
            if medals[i] and medals[i] != 'NA':
                athlete_olympics[name]['medals'].append(medals[i])

        # Filter for multi-Olympic athletes
        multi_olympic = {
            name: data for name, data in athlete_olympics.items()
            if len(data['years']) >= min_olympics
        }

        print(f"Found {len(multi_olympic):,} athletes competing in {min_olympics}+ Olympics\n")

        if not multi_olympic:
            return

        # Sort and display
        sorted_athletes = sorted(
            multi_olympic.items(),
            key=lambda x: len(x[1]['years']),
            reverse=True
        )

        print("="*80)
        print(f"TOP {sport_name.upper()} ATHLETES (Multiple Olympics)")
        print("="*80 + "\n")

        for idx, (name, data) in enumerate(sorted_athletes[:15], 1):
            num_olympics = len(data['years'])
            year_list = sorted(data['years'])
            noc = data['noc']
            num_medals = len(data['medals'])

            print(f"{idx:2d}. {name} ({noc})")
            print(f"    Olympics: {num_olympics} - Years: {', '.join(year_list)}")

            if num_medals > 0:
                gold = data['medals'].count('Gold')
                silver = data['medals'].count('Silver')
                bronze = data['medals'].count('Bronze')
                print(f"    Medals: {num_medals} (G:{gold} S:{silver} B:{bronze})")
            else:
                print("    Medals: 0")

            print()


    def compare_sports_using_dataframe(self) -> None:
        """
        Compare multiple sports using DataFrame filter and group_by operations.
        """

        print("\n" + "="*80)
        print("COMPARING SPORTS USING DATAFRAME OPERATIONS")
        print("="*80 + "\n")


        sports_to_compare = ['Swimming', 'Athletics', 'Gymnastics', 'Rowing', 'Fencing']

        print("Analyzing participation across multiple sports...\n")

        results = []

        for sport in sports_to_compare:
            # Filter for sport
            def make_sport_filter(sport_name: str):
                return lambda row: row['Sport'] == sport_name

            sport_df = self.athletes_df.filter(make_sport_filter(sport))

            # Count unique athletes
            names = set(sport_df['Name'])
            years = set(sport_df['Year'])

            # Count medals
            medals = [m for m in sport_df['Medal'] if m and m != 'NA']

            results.append({
                'sport': sport,
                'athletes': len(names),
                'olympics': len(years),
                'events': len(sport_df),
                'medals': len(medals)
            })

        # Display comparison
        print("="*80)
        print("SPORT COMPARISON SUMMARY")
        print("="*80 + "\n")

        print(f"{'Sport':<15} {'Athletes':>10} {'Olympics':>10} {'Events':>10} {'Medals':>10}")
        print("-" * 80)

        for result in sorted(results, key=lambda x: x['athletes'], reverse=True):
            print(f"{result['sport']:<15} {result['athletes']:>10,} {result['olympics']:>10} "
                f"{result['events']:>10,} {result['medals']:>10,}")


def main():

    moa_obj = MultipleOlympicAnalyzer(athlete_data_path = "athlete_events.csv")
    print("\n" + "="*80)
    print("MULTI-OLYMPIC ATHLETES ANALYZER")
    print("="*80)

    # Example 1: All multi-Olympic athletes with DataFrame operations
    print("\n\nEXAMPLE 1: Athletes who competed in 4+ Olympics")
    print("Demonstrates: project, group_by, agg, filter operations")
    moa_obj.find_multi_olympic_athletes(min_olympics=4, limit=30)

    # Example 2: Sport-specific analysis with explicit DataFrame operations
    print("\n\nEXAMPLE 2: Swimming - Demonstrating All DataFrame Operations")
    print("Shows: filter -> project -> group_by -> agg pipeline")
    moa_obj.analyze_sport_with_dataframe_ops(sport_name="Swimming", min_olympics=3)

    # Example 3: Another sport
    print("\n\nEXAMPLE 3: Athletics - Using DataFrame Operations")
    moa_obj.analyze_sport_with_dataframe_ops(sport_name="Athletics", min_olympics=3)

    # Example 4: Sport comparison using filter operations
    print("\n\nEXAMPLE 4: Comparing Multiple Sports")
    print("Demonstrates: Multiple filter operations and aggregations")
    moa_obj.compare_sports_using_dataframe()

    print("\n" + "="*80)
    print("Analysis complete!")
    print("All DataFrame operations demonstrated:")
    print("  - filter(): Filter rows based on conditions")
    print("  - project() / [columns]: Select specific columns")
    print("  - group_by(): Group data by columns")
    print("  - agg(): Aggregate grouped data")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
