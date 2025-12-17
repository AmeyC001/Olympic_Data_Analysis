"""
Grouping & Aggregation Examples using DataFrame from csv_functions.py
- Medal counts by country and sport
- Average athlete age/height/weight by sport
- Historical medal trends by decade
"""

from __future__ import annotations

from dataframe_loader import DataFrame
from utils import load_data_as_dataframe

class ProjectionApplication():
    def __init__(self, athlete_data_path, noc_regions_path):
        self.athletes_df = load_data_as_dataframe(athlete_data_path)
        self.noc_df = load_data_as_dataframe(noc_regions_path)

    def get_medals_by_country(self) -> None:
        """
        Example 1: Medal counts by country
        """

        print("\n" + "="*80)
        print("EXAMPLE 1: MEDAL COUNTS BY COUNTRY")
        print("="*80 + "\n")

        print(f"Loaded {len(self.athletes_df):,} athlete records\n")

        # STEP 1: Filter for medal winners
        print("STEP 1: filter() - Filtering for medal winners...")
        medal_winners = self.athletes_df.filter(
            lambda row: row['Medal'] in ['Gold', 'Silver', 'Bronze']
        )
        print(f"Result: {len(medal_winners):,} medal-winning performances\n")

        # STEP 2: Project relevant columns
        print("STEP 2: project() - Selecting 'NOC' and 'Medal' columns...")
        medals_df = medal_winners[['NOC', 'Medal']]
        print(f"Result: {len(medals_df.columns)} columns\n")

        # STEP 3: Group by NOC
        print("STEP 3: group_by() - Grouping by country (NOC)...")
        noc_groups = medals_df.group_by('NOC')
        print("Result: Countries grouped for aggregation\n")

        # STEP 4: Aggregate - count medals
        print("STEP 4: agg() - Counting medals per country...")
        medal_counts = noc_groups.agg({'Medal': 'count'})
        print(f"Result: {len(medal_counts):,} countries\n")

        # Join with country names
        print("STEP 5: join() - Adding country names...")
        with_names = medal_counts.join(self.noc_df, on='NOC', how='left')
        print(f"Result: {len(with_names):,} countries with names\n")

        # Sort and display
        nocs = with_names['NOC']
        regions = with_names['region']
        counts = [int(c) for c in with_names['Medal']]

        data = sorted(zip(nocs, regions, counts), key=lambda x: x[2], reverse=True)

        print("="*80)
        print("TOP 25 COUNTRIES BY TOTAL MEDAL COUNT")
        print("="*80 + "\n")

        for idx, (noc, region, count) in enumerate(data[:25], 1):
            region_display = region if region else noc
            print(f"{idx:3d}. {noc:3s} - {region_display:<40} | {count:6,} medals")


    def get_medals_by_sport(self) -> None:
        """
        Example 2: Medal counts by sport
        Shows which sports award the most medals
        """

        print("\n\n" + "="*80)
        print("EXAMPLE 2: MEDAL COUNTS BY SPORT")
        print("="*80 + "\n")

        # Filter for medal winners
        print("STEP 1: filter() - Medal winners only...")
        medal_winners = self.athletes_df.filter(
            lambda row: row['Medal'] in ['Gold', 'Silver', 'Bronze']
        )
        print(f"Result: {len(medal_winners):,} medals\n")

        # Project
        print("STEP 2: project() - Selecting 'Sport' and 'Medal'...")
        sport_medals = medal_winners[['Sport', 'Medal']]
        print(f"Result: {len(sport_medals.columns)} columns\n")

        # Group by sport
        print("STEP 3: group_by() - Grouping by sport...")
        sport_groups = sport_medals.group_by('Sport')
        print("Result: Sports grouped\n")

        # Aggregate
        print("STEP 4: agg() - Counting medals per sport...")
        sport_counts = sport_groups.agg({'Medal': 'count'})
        print(f"Result: {len(sport_counts):,} sports\n")

        # Sort and display
        sports = sport_counts['Sport']
        counts = [int(c) for c in sport_counts['Medal']]
        data = sorted(zip(sports, counts), key=lambda x: x[1], reverse=True)

        print("="*80)
        print("TOP 20 SPORTS BY MEDAL COUNT")
        print("="*80 + "\n")

        for idx, (sport, count) in enumerate(data[:20], 1):
            print(f"{idx:3d}. {sport:<30} | {count:6,} medals")


    def get_medals_by_country_and_sport(self) -> None:
        """
        Example 3: Medal counts by country AND sport (two-level grouping)
        Which countries excel in which sports
        """

        print("\n\n" + "="*80)
        print("EXAMPLE 3: MEDAL COUNTS BY COUNTRY AND SPORT")
        print("="*80 + "\n")


        # Filter for medal winners
        print("STEP 1: filter() - Medal winners only...")
        medal_winners = self.athletes_df.filter(
            lambda row: row['Medal'] in ['Gold', 'Silver', 'Bronze']
        )
        print(f"Result: {len(medal_winners):,} medals\n")

        # Project
        print("STEP 2: project() - Selecting 'NOC', 'Sport', 'Medal'...")
        medals_df = medal_winners[['NOC', 'Sport', 'Medal']]
        print(f"Result: {len(medals_df.columns)} columns\n")

        # Group by NOC and Sport (multi-column grouping)
        print("STEP 3: group_by() - Grouping by ['NOC', 'Sport']...")
        country_sport_groups = medals_df.group_by(['NOC', 'Sport'])
        print("Result: Country-sport combinations grouped\n")

        # Aggregate
        print("STEP 4: agg() - Counting medals per country-sport combination...")
        country_sport_counts = country_sport_groups.agg({'Medal': 'count'})
        print(f"Result: {len(country_sport_counts):,} country-sport combinations\n")

        # Sort and find top combinations
        nocs = country_sport_counts['NOC']
        sports = country_sport_counts['Sport']
        counts = [int(c) for c in country_sport_counts['Medal']]

        data = sorted(zip(nocs, sports, counts), key=lambda x: x[2], reverse=True)

        print("="*80)
        print("TOP 30 COUNTRY-SPORT COMBINATIONS BY MEDAL COUNT")
        print("="*80 + "\n")

        for idx, (noc, sport, count) in enumerate(data[:30], 1):
            print(f"{idx:3d}. {noc:3s} - {sport:<30} | {count:5,} medals")


    def get_average_age_by_sport(self) -> None:
        """
        Example 4: Average athlete age by sport
        Demonstrates aggregation with mean function
        """

        print("\n\n" + "="*80)
        print("EXAMPLE 4: AVERAGE ATHLETE AGE BY SPORT")
        print("="*80 + "\n")


        # Filter for records with valid age
        print("STEP 1: filter() - Athletes with valid age data...")
        def has_valid_age(row):
            age = row['Age']
            if not age or age == 'NA':
                return False
            try:
                float(age)
                return True
            except (ValueError, TypeError):
                return False

        valid_age_df = self.athletes_df.filter(has_valid_age)
        print(f"Result: {len(valid_age_df):,} records with valid age\n")

        # Project
        print("STEP 2: project() - Selecting 'Sport' and 'Age'...")
        sport_age_df = valid_age_df[['Sport', 'Age']]
        print(f"Result: {len(sport_age_df.columns)} columns\n")

        age_numeric = [float(age) for age in sport_age_df['Age']]
        sport_age_df = DataFrame({
            'Sport': sport_age_df['Sport'],
            'Age': age_numeric
        })

        # Group by sport
        print("STEP 3: group_by() - Grouping by sport...")
        sport_groups = sport_age_df.group_by('Sport')
        print("Result: Sports grouped\n")

        # Aggregate with mean
        print("STEP 4: agg() - Calculating average age per sport...")
        avg_ages = sport_groups.agg({'Age': 'mean'})
        print(f"Result: {len(avg_ages):,} sports\n")

        # Sort by average age
        sports = avg_ages['Sport']
        ages = [float(a) for a in avg_ages['Age']]
        data = sorted(zip(sports, ages), key=lambda x: x[1])

        print("="*80)
        print("SPORTS BY AVERAGE ATHLETE AGE")
        print("="*80 + "\n")

        print("YOUNGEST SPORTS (by average age):")
        for idx, (sport, age) in enumerate(data[:10], 1):
            print(f"{idx:3d}. {sport:<30} | Avg Age: {age:5.1f} years")

        print("\nOLDEST SPORTS (by average age):")
        for idx, (sport, age) in enumerate(data[-10:], 1):
            print(f"{idx:3d}. {sport:<30} | Avg Age: {age:5.1f} years")


    def get_athlete_stats_by_sport(self) -> None:
        """
        Example 5: Multiple aggregations - age, height, weight by sport
        Demonstrates multiple aggregation functions at once
        """

        print("\n\n" + "="*80)
        print("EXAMPLE 5: ATHLETE STATISTICS BY SPORT (Age, Height, Weight)")
        print("="*80 + "\n")

        # Filter for valid data
        print("STEP 1: filter() - Athletes with valid age, height, and weight...")
        def has_valid_stats(row):
            for col in ['Age', 'Height', 'Weight']:
                val = row[col]
                if not val or val == 'NA':
                    return False
                try:
                    float(val)
                except (ValueError, TypeError):
                    return False
            return True

        valid_stats_df = self.athletes_df.filter(has_valid_stats)
        print(f"Result: {len(valid_stats_df):,} records with complete stats\n")

        # Project
        print("STEP 2: project() - Selecting sport and physical attributes...")
        stats_df = valid_stats_df[['Sport', 'Age', 'Height', 'Weight']]
        print(f"Result: {len(stats_df.columns)} columns\n")

        stats_df = DataFrame({
            'Sport': stats_df['Sport'],
            'Age': [float(age) for age in stats_df['Age']],
            'Height': [float(height) for height in stats_df['Height']],
            'Weight': [float(weight) for weight in stats_df['Weight']]
        })

        # Group by sport
        print("STEP 3: group_by() - Grouping by sport...")
        sport_groups = stats_df.group_by('Sport')
        print("Result: Sports grouped\n")

        # Multiple aggregations
        print("STEP 4: agg() - Computing multiple statistics...")
        sport_stats = sport_groups.agg({
            'Age': 'mean',
            'Height': 'mean',
            'Weight': 'mean'
        })
        print(f"Result: {len(sport_stats):,} sports with aggregate statistics\n")

        # Display
        sports = sport_stats['Sport']
        ages = [float(a) for a in sport_stats['Age']]
        heights = [float(h) for h in sport_stats['Height']]
        weights = [float(w) for w in sport_stats['Weight']]

        data = list(zip(sports, ages, heights, weights))
        data.sort(key=lambda x: x[0])  # Sort alphabetically

        print("="*80)
        print("ATHLETE PHYSICAL STATISTICS BY SPORT (Top 25 alphabetically)")
        print("="*80 + "\n")
        print(f"{'Sport':<25} | {'Avg Age':>8} | {'Avg Height':>11} | {'Avg Weight':>11}")
        print("-" * 80)

        for sport, age, height, weight in data[:25]:
            print(f"{sport:<25} | {age:8.1f} | {height:8.1f} cm | {weight:8.1f} kg")


    def get_medal_trends_by_decade(self) -> None:
        """
        Example 6: Historical medal trends by decade
        Shows how medal distribution changed over time
        """

        print("\n\n" + "="*80)
        print("EXAMPLE 6: MEDAL TRENDS BY DECADE")
        print("="*80 + "\n")

        # Filter for medal winners
        print("STEP 1: filter() - Medal winners only...")
        medal_winners = self.athletes_df.filter(
            lambda row: row['Medal'] in ['Gold', 'Silver', 'Bronze']
        )
        print(f"Result: {len(medal_winners):,} medals\n")

        # Add decade column (manual transformation)
        print("STEP 2: Adding decade column...")
        years = medal_winners['Year']
        medals = medal_winners['Medal']

        # Create decade buckets
        decade_data = {}
        for i in range(len(medal_winners)):
            year_str = years[i]
            medal = medals[i]

            try:
                year = int(year_str)
                decade = (year // 10) * 10  # e.g., 1996 -> 1990
                decade_str = f"{decade}s"

                if decade_str not in decade_data:
                    decade_data[decade_str] = []
                decade_data[decade_str].append(medal)
            except (ValueError, TypeError):
                pass

        print(f"Result: {len(decade_data)} decades identified\n")

        # Count medals by decade
        print("STEP 3: Counting medals by decade...")
        decade_counts = {}
        for decade, medal_list in decade_data.items():
            decade_counts[decade] = {
                'total': len(medal_list),
                'gold': medal_list.count('Gold'),
                'silver': medal_list.count('Silver'),
                'bronze': medal_list.count('Bronze')
            }

        # Sort by decade
        sorted_decades = sorted(decade_counts.keys())

        print("="*80)
        print("MEDAL DISTRIBUTION BY DECADE")
        print("="*80 + "\n")
        print(f"{'Decade':<10} | {'Total':>8} | {'Gold':>8} | {'Silver':>8} | {'Bronze':>8}")
        print("-" * 80)

        for decade in sorted_decades:
            counts = decade_counts[decade]
            print(f"{decade:<10} | {counts['total']:8,} | {counts['gold']:8,} | "
                f"{counts['silver']:8,} | {counts['bronze']:8,}")


    def get_country_sport_excellence(self) -> None:
        """
        Example 7: Which sports each top country excels at
        Group by country, then analyze their top sports
        """

        print("\n\n" + "="*80)
        print("EXAMPLE 7: TOP SPORTS FOR LEADING COUNTRIES")
        print("="*80 + "\n")

        # Filter for medal winners
        medal_winners = self.athletes_df.filter(
            lambda row: row['Medal'] in ['Gold', 'Silver', 'Bronze']
        )

        top_countries = ['USA', 'CHN', 'GBR', 'FRA', 'GER']

        for country in top_countries:
            print(f"\n{'*'*80}")
            print(f"COUNTRY: {country}")
            print(f"{'*'*80}")

            # Filter for this country
            def make_country_filter(noc_code: str):
                return lambda row: row['NOC'] == noc_code

            country_medals = medal_winners.filter(make_country_filter(country))
            print(f"Total medals: {len(country_medals):,}\n")

            # Project and group by sport
            sport_medals = country_medals[['Sport', 'Medal']]
            sport_groups = sport_medals.group_by('Sport')
            sport_counts = sport_groups.agg({'Medal': 'count'})

            # Sort and show top 5 sports
            sports = sport_counts['Sport']
            counts = [int(c) for c in sport_counts['Medal']]
            data = sorted(zip(sports, counts), key=lambda x: x[1], reverse=True)

            print("Top 5 Sports:")
            for idx, (sport, count) in enumerate(data[:5], 1):
                print(f"  {idx}. {sport:<30} | {count:5,} medals")


def main():
    """Main function demonstrating grouping and aggregation."""

    print("\n" + "="*80)
    print("GROUPING & AGGREGATION EXAMPLES")
    print("Using csv_functions.py DataFrame Implementation")
    print("="*80)
    print("\nDemonstrates:")
    print("  - group_by() for single and multiple columns")
    print("  - agg() with count, mean, and other functions")
    print("  - Medal counts by country and sport")
    print("  - Average athlete statistics by sport")
    print("  - Historical trends by decade")
    print("="*80)

    # Run all examples
    proj_obj = ProjectionApplication(athlete_data_path="athlete_events.csv", noc_regions_path="noc_regions.csv")
    proj_obj.get_medals_by_country()
    proj_obj.get_medals_by_sport()
    proj_obj.get_medals_by_country_and_sport()
    proj_obj.get_average_age_by_sport()
    proj_obj.get_athlete_stats_by_sport()
    proj_obj.get_medal_trends_by_decade()
    proj_obj.get_country_sport_excellence()

    print("\n" + "="*80)
    print("All grouping & aggregation examples complete!")
    print("="*80)
    print("\nKey DataFrame operations demonstrated:")
    print("  1. df.group_by('column') - Group by single column")
    print("  2. df.group_by(['col1', 'col2']) - Group by multiple columns")
    print("  3. grouped.agg({'col': 'count'}) - Count aggregation")
    print("  4. grouped.agg({'col': 'mean'}) - Average aggregation")
    print("  5. grouped.agg({'col1': 'mean', 'col2': 'mean'}) - Multiple aggregations")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
