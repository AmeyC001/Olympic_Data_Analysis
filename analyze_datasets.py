"""
Analyze Olympic athlete events and NOC regions datasets.
Uses custom CSV parser from csv_fun.py
"""
from utils import read_csv, calculate_statistics, is_numeric


def main():
    """Main function to analyze both datasets."""

    print("\n" + "="*80)
    print("OLYMPIC DATASETS ANALYSIS")
    print("="*80)

    # Load athlete events dataset
    print("\nLoading athlete_events.csv...")
    athlete_data = read_csv(
        "athlete_events.csv",
        has_header=True
    )
    print(f"Loaded {len(athlete_data):,} records")

    # Load NOC regions dataset
    print("\nLoading noc_regions.csv...")
    noc_data = read_csv(
        "noc_regions.csv",
        has_header=True
    )
    print(f"Loaded {len(noc_data):,} records")

    # Analyze both datasets
    calculate_statistics(athlete_data, "Olympic Athlete Events")
    calculate_statistics(noc_data, "NOC Regions")

    # Cross-dataset insights
    print("\n" + "="*80)
    print("CROSS-DATASET INSIGHTS")
    print("="*80 + "\n")

    # Get unique NOCs from both datasets
    athlete_nocs = {row.get("NOC", "") for row in athlete_data if row.get("NOC")}
    noc_regions = {row.get("NOC", "") for row in noc_data if row.get("NOC")}

    print(f"   Unique NOCs in athlete events: {len(athlete_nocs)}")
    print(f"   Unique NOCs in regions data: {len(noc_regions)}")
    print(f"   NOCs in both datasets: {len(athlete_nocs & noc_regions)}")
    print(f"   NOCs only in athlete events: {len(athlete_nocs - noc_regions)}")
    print(f"   NOCs only in regions data: {len(noc_regions - athlete_nocs)}")

    # Year range analysis
    if athlete_data:
        years = [row.get("Year", "") for row in athlete_data if row.get("Year") and is_numeric(row.get("Year", ""))]
        if years:
            year_values = [int(y) for y in years]
            print(f"\n   Year range: {min(year_values)} - {max(year_values)}")
            print(f"   Time span: {max(year_values) - min(year_values)} years")

    print("\n" + "="*80)
    print("Analysis complete!")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
