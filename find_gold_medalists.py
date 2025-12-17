"""
Find gold medalists from Olympic athlete events using custom DataFrame implementation.
"""

from __future__ import annotations

from dataframe_loader import DataFrame
from utils import load_data_as_dataframe



def find_gold_medalists(
    countries: list[str] | None = None,
    limit_per_country: int | None = 10
) -> None:
    """
    Find and display gold medalists from specific countries.

    Args:
        countries: List of NOC codes (e.g., ['USA', 'CHN', 'GBR']). None = all countries
        limit_per_country: Number of examples to show per country
    """

    print("\n" + "="*80)
    print("LOADING OLYMPIC DATA")
    print("="*80 + "\n")

    # Load datasets
    print("Loading athlete_events.csv...")
    athletes_df = load_data_as_dataframe(
        "athlete_events.csv"
    )
    print(f"Loaded {len(athletes_df):,} athlete records")

    print("\n Loading noc_regions.csv...")
    noc_df = load_data_as_dataframe(
        "noc_regions.csv"
    )
    print(f"Loaded {len(noc_df):,} NOC region records")

    print("\n" + "="*80)
    print("FILTERING FOR GOLD MEDALS")
    print("="*80 + "\n")

    # Filter for gold medals only
    gold_medals_df = athletes_df.filter(lambda row: row['Medal'] == 'Gold')
    print(f"Found {len(gold_medals_df):,} gold medal records")

    # Filter by countries if specified
    if countries:
        print(f"\nFiltering for countries: {', '.join(countries)}")
        gold_medals_df = gold_medals_df.filter(lambda row: row['NOC'] in countries)
        print(f"Found {len(gold_medals_df):,} gold medals from specified countries")

    if len(gold_medals_df) == 0:
        print("\nNo gold medals found matching criteria")
        return

    # Join with NOC data to get region names
    print("\nJoining with NOC region data...")
    joined_df = gold_medals_df.join(
        noc_df,
        on='NOC',
        how='left',
        suffixes=('', '_region')
    )
    print(f"Joined {len(joined_df):,} records")

    # Group by country and count
    print("\nAnalyzing gold medals by country...")
    country_counts = joined_df.group_by('NOC').agg({'Medal': 'count'})
    print(f"Found gold medals from {len(country_counts):,} countries")

    # Display results
    print("\n" + "="*80)
    print("GOLD MEDAL SUMMARY BY COUNTRY")
    print("="*80 + "\n")

    # Get NOC list sorted by medal count
    noc_list = country_counts['NOC']
    medal_counts = [int(c) for c in country_counts['Medal']]

    # Sort by count descending
    noc_counts = list(zip(noc_list, medal_counts))
    noc_counts.sort(key=lambda x: x[1], reverse=True)

    total_medals = sum(medal_counts)
    print(f"Total Gold Medals: {total_medals:,}")
    print(f"Countries: {len(noc_counts)}\n")

    # Display each country
    for noc, count in noc_counts:
        # Get region name
        region_name = None
        for i in range(len(joined_df)):
            if joined_df['NOC'][i] == noc:
                region_name = joined_df['region'][i] if 'region' in joined_df.columns else noc
                break

        if not region_name:
            region_name = noc

        print(f"\n{'*'*80}")
        print(f"{noc} - {region_name}")
        print(f"{'*'*80}")
        print(f"Total Gold Medals: {count:,}\n")

        # Filter for this country's medals
        def make_noc_filter(noc_code: str):
            return lambda row: row['NOC'] == noc_code

        country_medals = joined_df.filter(make_noc_filter(noc))

        # Select relevant columns
        display_df = country_medals[['Name', 'Year', 'City', 'Sport', 'Event']]

        # Show sample of medals
        print("Sample Gold Medalists:")
        print("-" * 80)

        medals_to_show = min(limit_per_country or 10, len(display_df))
        for i in range(medals_to_show):
            name = display_df['Name'][i]
            year = display_df['Year'][i]
            city = display_df['City'][i]
            sport = display_df['Sport'][i]
            event = display_df['Event'][i]

            print(f"{i+1:3d}. {name}")
            print(f"      Year: {year:4s} | City: {city}")
            print(f"      Sport: {sport}")
            print(f"      Event: {event}")

        if len(display_df) > medals_to_show:
            remaining = len(display_df) - medals_to_show
            print(f"      ... and {remaining:,} more gold medals")

        # Sport breakdown
        sport_stats = country_medals.group_by('Sport').agg({'Medal': 'count'})
        sports = sport_stats['Sport']
        sport_counts = [int(c) for c in sport_stats['Medal']]

        # Sort by count
        sport_data = list(zip(sports, sport_counts))
        sport_data.sort(key=lambda x: x[1], reverse=True)

        print("\n   Top Sports by Gold Medals:")
        for sport, sport_count in sport_data[:10]:
            print(f"      * {sport}: {sport_count:,}")

        if len(sport_data) > 10:
            print(f"      ... and {len(sport_data) - 10} more sports")


def main():
    """Main function with examples."""

    print("\n" + "="*80)
    print("OLYMPIC GOLD MEDAL FINDER")
    print("Using Custom DataFrame Implementation from csv_functions.py")
    print("="*80)

    # Example 1: Top countries
    print("\n\nEXAMPLE 1: Top Gold Medal Countries (USA, China, Great Britain)")
    find_gold_medalists(countries=['USA', 'CHN', 'GBR'], limit_per_country=5)

    # Example 2: Specific country
    print("\n\nEXAMPLE 2: All Gold Medals from Jamaica")
    find_gold_medalists(countries=['JAM'], limit_per_country=20)

    print("\n" + "="*80)
    print("Analysis complete!")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
