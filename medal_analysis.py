"""
Analyze Olympic medal winners by age ranges and sports.
"""

from __future__ import annotations

from dataframe_loader import DataFrame
from utils import load_data_as_dataframe

class MedalAnalyzer():
    def __init__(self, athlete_data_path):
        self.athletes_df = load_data_as_dataframe(athlete_data_path)

    def analyze_medals_by_age_range(self) -> None:
        """Analyze medal distribution across different age ranges."""

        print("\n" + "="*80)
        print("MEDAL WINNERS BY AGE RANGE")
        print("="*80 + "\n")

        print(f"Loaded {len(self.athletes_df):,} records\n")

        # Filter for medal winners only
        print("Filtering for medal winners...")
        medal_winners = self.athletes_df.filter(
            lambda row: row['Medal'] in ['Gold', 'Silver', 'Bronze']
        )
        print(f"Found {len(medal_winners):,} medal-winning performances\n")

        # Get ages and medals
        ages = medal_winners['Age']
        medals = medal_winners['Medal']
        names = medal_winners['Name']
        sports = medal_winners['Sport']
        years = medal_winners['Year']

        # Define age ranges
        age_ranges = {
            '10-19 (Teens)': (10, 19),
            '20-29 (Twenties)': (20, 29),
            '30-39 (Thirties)': (30, 39),
            '40-49 (Forties)': (40, 49),
            '50+ (Fifties+)': (50, 100)
        }

        # Categorize medals by age range
        age_range_data = {range_name: {'Gold': 0, 'Silver': 0, 'Bronze': 0, 'Total': 0, 'athletes': set()}
                        for range_name in age_ranges}

        unknown_age = 0

        for i in range(len(medal_winners)):
            age_str = ages[i]
            medal = medals[i]
            name = names[i]

            # Check if age is valid
            if not age_str or age_str == 'NA':
                unknown_age += 1
                continue

            try:
                age = int(float(age_str))
            except (ValueError, TypeError):
                unknown_age += 1
                continue

            # Find appropriate age range
            for range_name, (min_age, max_age) in age_ranges.items():
                if min_age <= age <= max_age:
                    age_range_data[range_name][medal] += 1
                    age_range_data[range_name]['Total'] += 1
                    age_range_data[range_name]['athletes'].add(name)
                    break

        # Display results
        print("="*80)
        print("MEDAL DISTRIBUTION BY AGE RANGE")
        print("="*80 + "\n")

        total_medals = sum(data['Total'] for data in age_range_data.values())

        for range_name in age_ranges:
            data = age_range_data[range_name]
            gold = data['Gold']
            silver = data['Silver']
            bronze = data['Bronze']
            total = data['Total']
            unique_athletes = len(data['athletes'])

            if total == 0:
                continue

            percentage = (total / total_medals * 100) if total_medals > 0 else 0

            print(f"{range_name}")
            print("-" * 80)
            print(f"   Total Medals: {total:,} ({percentage:.1f}% of all medals)")
            print(f"   Gold: {gold:,} | Silver: {silver:,} | Bronze: {bronze:,}")
            print(f"   Unique Athletes: {unique_athletes:,}")
            print()

        if unknown_age > 0:
            print(f"Unknown Age: {unknown_age:,} medals\n")

        # Find youngest and oldest medalists
        print("\n" + "="*80)
        print("EXTREME AGE MEDALISTS")
        print("="*80 + "\n")

        valid_ages = []
        for i in range(len(medal_winners)):
            age_str = ages[i]
            if age_str and age_str != 'NA':
                try:
                    age = int(float(age_str))
                    valid_ages.append((age, names[i], medals[i], sports[i], years[i]))
                except (ValueError, TypeError):
                    pass

        if valid_ages:
            # Youngest medalists
            youngest = sorted(valid_ages, key=lambda x: x[0])[:5]
            print("YOUNGEST MEDALISTS:")
            for age, name, medal, sport, year in youngest:
                print(f"   Age {age}: {name} - {medal} medal in {sport} ({year})")

            # Oldest medalists
            oldest = sorted(valid_ages, key=lambda x: x[0], reverse=True)[:5]
            print("OLDEST MEDALISTS:")
            for age, name, medal, sport, year in oldest:
                print(f"   Age {age}: {name} - {medal} medal in {sport} ({year})")


    def analyze_medals_by_sport(self, top_n: int = 20) -> None:
        """Analyze medal distribution across different sports."""

        print("\n\n" + "="*80)
        print("MEDAL WINNERS BY SPORT")
        print("="*80 + "\n")

        
        print(f"Loaded {len(self.athletes_df):,} records\n")

        # Filter for medal winners
        print("Filtering for medal winners...")
        medal_winners = self.athletes_df.filter(
            lambda row: row['Medal'] in ['Gold', 'Silver', 'Bronze']
        )
        print(f"Found {len(medal_winners):,} medal-winning performances\n")

        # Group by sport and count medals
        print("Analyzing medals by sport...")
        sport_stats = medal_winners.group_by('Sport').agg({'Medal': 'count'})

        # Get data
        sport_names = sport_stats['Sport']
        medal_counts = [int(c) for c in sport_stats['Medal']]

        # Sort by medal count
        sport_data = list(zip(sport_names, medal_counts))
        sport_data.sort(key=lambda x: x[1], reverse=True)

        # Display top sports
        print("="*80)
        print(f"TOP {top_n} SPORTS BY TOTAL MEDALS")
        print("="*80 + "\n")

        total_medals = sum(medal_counts)
        print(f"Total Medals Awarded: {total_medals:,}")
        print(f"Total Sports: {len(sport_names)}\n")

        for idx, (sport, count) in enumerate(sport_data[:top_n], 1):
            percentage = (count / total_medals * 100) if total_medals > 0 else 0

            # Get medal breakdown for this sport
            def make_sport_filter(sport_name: str):
                return lambda row: row['Sport'] == sport_name

            sport_medals = medal_winners.filter(make_sport_filter(sport))

            # Count gold, silver, bronze
            sport_medal_list = sport_medals['Medal']
            gold = sport_medal_list.count('Gold')
            silver = sport_medal_list.count('Silver')
            bronze = sport_medal_list.count('Bronze')

            print(f"{idx:2d}. {sport}")
            print(f"    Total Medals: {count:,} ({percentage:.1f}%)")
            print(f"    Gold: {gold:,} | Silver: {silver:,} | Bronze: {bronze:,}")
            print()


    def analyze_sport_by_age(self, sport_name: str) -> None:
        """Analyze age distribution for a specific sport."""

        print("\n\n" + "="*80)
        print(f"AGE ANALYSIS FOR {sport_name.upper()}")
        print("="*80 + "\n")


        # Filter for specific sport and medal winners
        print(f"Filtering for {sport_name} medal winners...")
        sport_medals = self.athletes_df.filter(
            lambda row: row['Sport'] == sport_name and row['Medal'] in ['Gold', 'Silver', 'Bronze']
        )

        if len(sport_medals) == 0:
            print(f"No medal data found for {sport_name}\n")
            return

        print(f"Found {len(sport_medals):,} medals in {sport_name}\n")

        # Analyze ages
        ages = sport_medals['Age']
        medals = sport_medals['Medal']
        names = sport_medals['Name']

        valid_ages = []
        for i in range(len(sport_medals)):
            age_str = ages[i]
            if age_str and age_str != 'NA':
                try:
                    age = int(float(age_str))
                    valid_ages.append((age, names[i], medals[i]))
                except (ValueError, TypeError):
                    pass

        if not valid_ages:
            print("No valid age data available\n")
            return

        # Calculate statistics
        age_values = [a[0] for a in valid_ages]
        min_age = min(age_values)
        max_age = max(age_values)
        avg_age = sum(age_values) / len(age_values)

        # Calculate median
        sorted_ages = sorted(age_values)
        mid = len(sorted_ages) // 2
        if len(sorted_ages) % 2 == 0:
            median_age = (sorted_ages[mid-1] + sorted_ages[mid]) / 2
        else:
            median_age = sorted_ages[mid]

        print("AGE STATISTICS:")
        print(f"   Youngest Medalist: {min_age} years old")
        print(f"   Oldest Medalist: {max_age} years old")
        print(f"   Average Age: {avg_age:.1f} years old")
        print(f"   Median Age: {median_age:.1f} years old")

        # Show youngest and oldest
        print("\n   Youngest Medalists:")
        for age, name, medal in sorted(valid_ages, key=lambda x: x[0])[:3]:
            print(f"      Age {age}: {name} ({medal})")

        print("\n   Oldest Medalists:")
        for age, name, medal in sorted(valid_ages, key=lambda x: x[0], reverse=True)[:3]:
            print(f"      Age {age}: {name} ({medal})")


def main():
    """Main function with examples."""
    ma_obj = MedalAnalyzer(athlete_data_path = "athlete_events.csv")
    print("\n" + "="*80)
    print("OLYMPIC MEDAL ANALYSIS BY AGE AND SPORT")
    print("Using Custom DataFrame Implementation from csv_functions.py")
    print("="*80)

    # Example 1: Medals by age range
    print("\n\nEXAMPLE 1: Medal Distribution Across Age Ranges")
    ma_obj.analyze_medals_by_age_range()

    # Example 2: Medals by sport
    print("\n\nEXAMPLE 2: Top Sports by Medal Count")
    ma_obj.analyze_medals_by_sport(top_n=15)

    # Example 3: Sport-specific age analysis
    print("\n\nEXAMPLE 3: Age Analysis for Gymnastics")
    ma_obj.analyze_sport_by_age("Gymnastics")

    print("\n\nEXAMPLE 4: Age Analysis for Swimming")
    ma_obj.analyze_sport_by_age("Swimming")

    print("\n" + "="*80)
    print("Analysis complete!")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
