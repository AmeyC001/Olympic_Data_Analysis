"""
Summary Reports: Generate insights and professional reports
- Top 10 Countries by Gold Medals
- Most Successful Athletes
"""

from __future__ import annotations

from dataframe_loader import DataFrame
from utils import load_data_as_dataframe


class SummaryReport():
    def __init__(self, athlete_data_path, noc_regions_path):
        self.athletes_df = load_data_as_dataframe(athlete_data_path)
        self.noc_df = load_data_as_dataframe(noc_regions_path)

    def report_1_top_countries_by_gold(self) -> None:
        """
        Report 1: Top 10 Countries by Gold Medals
        """

        print("\n" + "="*80)
        print("REPORT 1: TOP 10 COUNTRIES BY GOLD MEDALS")
        print("="*80 + "\n")


        # Filter for gold medals
        print("Analyzing gold medal winners...")
        gold_medals = self.athletes_df.filter(lambda row: row['Medal'] == 'Gold')
        print(f"Found {len(gold_medals):,} gold medal performances\n")

        # Project and group
        gold_by_country = gold_medals[['NOC', 'Medal']]
        country_groups = gold_by_country.group_by('NOC')
        gold_counts = country_groups.agg({'Medal': 'count'})

        # Join with country names
        with_names = gold_counts.join(self.noc_df, on='NOC', how='left')

        # Sort
        nocs = with_names['NOC']
        regions = with_names['region']
        counts = [int(c) for c in with_names['Medal']]
        data = sorted(zip(nocs, regions, counts), key=lambda x: x[2], reverse=True)

        # Generate report
        print("*" + "*"*78 + "*")
        print("|" + " "*25 + "GOLD MEDAL RANKINGS" + " "*34 + "|")
        print("|" + "*"*78 + "|")
        print("| Rank | NOC | Country                              | Gold Medals   |")
        print("|" + "*"*78 + "|")

        for idx, (noc, region, count) in enumerate(data[:10], 1):
            region_display = region[:35] if region else noc
            print(f"| {idx:4d} | {noc:3s} | {region_display:<36} | {count:13,} |")

        print("|" + "-"*78 + "|")

        # Add insights
        total_gold = sum(counts)
        top_10_gold = sum(c for _, _, c in data[:10])
        percentage = (top_10_gold / total_gold * 100) if total_gold > 0 else 0

        print("\nINSIGHTS:")
        print(f"   * Total gold medals awarded: {total_gold:,}")
        print(f"   * Top 10 countries hold: {top_10_gold:,} ({percentage:.1f}% of all gold)")
        print(f"   * Top country ({data[0][0]}): {data[0][2]:,} gold medals")
        print(f"   * Countries that won gold: {len(data)}")


    def report_2_top_countries_total_medals(self) -> None:
        """
        Report 2: Top 10 Countries by Total Medals (Gold + Silver + Bronze)
        """

        print("\n\n" + "="*80)
        print("REPORT 2: TOP 10 COUNTRIES BY TOTAL MEDALS")
        print("="*80 + "\n")

        # Filter for all medals
        print("Analyzing all medal winners...")
        medal_winners = self.athletes_df.filter(
            lambda row: row['Medal'] in ['Gold', 'Silver', 'Bronze']
        )
        print(f"Found {len(medal_winners):,} total medals\n")

        # Group and count
        medals_by_country = medal_winners[['NOC', 'Medal']]
        country_groups = medals_by_country.group_by('NOC')
        medal_counts = country_groups.agg({'Medal': 'count'})

        # Join with names
        with_names = medal_counts.join(self.noc_df, on='NOC', how='left')

        # Sort
        nocs = with_names['NOC']
        regions = with_names['region']
        counts = [int(c) for c in with_names['Medal']]
        data = sorted(zip(nocs, regions, counts), key=lambda x: x[2], reverse=True)

        # Generate report
        print("|" + "-"*78 + "|")
        print("|" + " "*23 + "TOTAL MEDAL RANKINGS" + " "*34 + "|")
        print("|" + "-"*78 + "|")
        print("| Rank | NOC | Country                              | Total Medals  |")
        print("|" + "-"*78 + "|")

        for idx, (noc, region, count) in enumerate(data[:10], 1):
            region_display = region[:35] if region else noc
            print(f"| {idx:4d} | {noc:3s} | {region_display:<36} | {count:13,} |")

        print("|" + "-"*78 + "|")

        print("\n INSIGHTS:")
        print(f"   * Total medals awarded: {sum(counts):,}")
        print(f"   * Average medals per country: {sum(counts)/len(data):.1f}")
        print(f"   * Median country has: {sorted(counts)[len(counts)//2]:,} medals")


    def report_3_most_successful_athletes(self) -> None:
        """
        Report 3: Top 10 Most Successful Athletes (by total medals)
        """

        print("\n\n" + "="*80)
        print("REPORT 3: TOP 10 MOST SUCCESSFUL ATHLETES (All Medals)")
        print("="*80 + "\n")

        # Filter for medal winners
        print("Identifying most decorated athletes...")
        medal_winners = self.athletes_df.filter(
            lambda row: row['Medal'] in ['Gold', 'Silver', 'Bronze']
        )
        print(f"Analyzing {len(medal_winners):,} medal performances\n")

        # Project relevant columns
        athlete_medals = medal_winners[['Name', 'NOC', 'Sport', 'Medal']]

        # Group by athlete name
        athlete_groups = athlete_medals.group_by('Name')
        athlete_counts = athlete_groups.agg({'Medal': 'count'})

        # Sort
        names = athlete_counts['Name']
        counts = [int(c) for c in athlete_counts['Medal']]
        athlete_data = sorted(zip(names, counts), key=lambda x: x[1], reverse=True)

        # Get additional details for top athletes
        print("|" + "-"*78 + "|")
        print("|" + " "*20 + "MOST DECORATED ATHLETES" + " "*32 + "|")
        print("|" + "-"*78 + "|")
        print("| Rank | Athlete Name                          | NOC | Total Medals |")
        print("|" + "-"*78 + "|")

        for idx, (name, count) in enumerate(athlete_data[:10], 1):
            # Get NOC for this athlete
            noc = 'UNK'
            for i in range(len(athlete_medals)):
                if athlete_medals['Name'][i] == name:
                    noc = athlete_medals['NOC'][i]
                    break

            name_display = name[:35] if len(name) > 35 else name
            print(f"| {idx:4d} | {name_display:<36} | {noc:3s} | {count:12,} |")

        print("|" + "-"*78 + "|")

        # Add medal breakdown for #1 athlete
        top_athlete = athlete_data[0][0]
        top_medals = [
            athlete_medals['Medal'][i]
            for i in range(len(athlete_medals))
            if athlete_medals['Name'][i] == top_athlete
        ]

        gold = top_medals.count('Gold')
        silver = top_medals.count('Silver')
        bronze = top_medals.count('Bronze')

        print("\n CHAMPION: " + top_athlete)
        print(f"   * Gold: {gold} | Silver: {silver} | Bronze: {bronze}")
        print(f"   * Total: {len(top_medals)} medals")


    def report_4_gold_medal_champions(self) -> None:
        """
        Report 4: Top 10 Athletes by Gold Medals Only
        """

        print("\n\n" + "="*80)
        print("REPORT 4: TOP 10 GOLD MEDAL CHAMPIONS")
        print("="*80 + "\n")

        # Filter for gold medals only
        print("Finding gold medal champions...")
        gold_medals = self.athletes_df.filter(lambda row: row['Medal'] == 'Gold')
        print(f"Analyzing {len(gold_medals):,} gold medal performances\n")

        # Project and group
        athlete_golds = gold_medals[['Name', 'NOC', 'Sport', 'Year', 'Medal']]
        athlete_groups = athlete_golds.group_by('Name')
        gold_counts = athlete_groups.agg({'Medal': 'count'})

        # Sort
        names = gold_counts['Name']
        counts = [int(c) for c in gold_counts['Medal']]
        data = sorted(zip(names, counts), key=lambda x: x[1], reverse=True)

        # Report
        print("|" + "-"*78 + "|")
        print("|" + " "*24 + "GOLD MEDAL LEGENDS" + " "*32 + "|")
        print("|" + "-"*78 + "|")
        print("| Rank | Athlete Name                          | NOC | Gold Medals  |")
        print("|" + "-"*78 + "|")

        for idx, (name, count) in enumerate(data[:10], 1):
            # Get NOC
            noc = 'UNK'
            for i in range(len(athlete_golds)):
                if athlete_golds['Name'][i] == name:
                    noc = athlete_golds['NOC'][i]
                    break

            name_display = name[:35] if len(name) > 35 else name
            print(f"| {idx:4d} | {name_display:<36} | {noc:3s} | {count:12,} |")

        print("|" + "-"*78 + "|")

        # Show sport breakdown for top 3
        print("\n TOP CHAMPIONS BY SPORT:")
        for idx, (name, _count) in enumerate(data[:3], 1):
            sports = [
                athlete_golds['Sport'][i]
                for i in range(len(athlete_golds))
                if athlete_golds['Name'][i] == name
            ]

            sport_set = set(sports)
            sport_display = ", ".join(sport_set) if len(sport_set) <= 3 else f"{len(sport_set)} different sports"
            print(f"   {idx}. {name}: {sport_display}")


    def report_5_sport_excellence(self) -> None:
        """
        Report 5: Top 5 Athletes in Popular Sports
        """

        print("\n\n" + "="*80)
        print("REPORT 5: SPORT EXCELLENCE - Top Athletes by Sport")
        print("="*80 + "\n")

        medal_winners = self.athletes_df.filter(
            lambda row: row['Medal'] in ['Gold', 'Silver', 'Bronze']
        )

        popular_sports = ['Swimming', 'Athletics', 'Gymnastics', 'Rowing', 'Cycling']

        for sport in popular_sports:
            print(f"\n|{'-'*78}|")
            print(f"| {sport.upper():<71} TOP ATHLETES |")
            print(f"|{'-'*78}|")

            # Filter for sport
            def make_sport_filter(sport_name: str):
                return lambda row: row['Sport'] == sport_name

            sport_medals = medal_winners.filter(make_sport_filter(sport))

            if len(sport_medals) == 0:
                print(f"| No data available{'':<60} |")
                print(f"|{'-'*78}|")
                continue

            # Group by athlete
            athlete_data = sport_medals[['Name', 'NOC', 'Medal']]
            athlete_groups = athlete_data.group_by('Name')
            athlete_counts = athlete_groups.agg({'Medal': 'count'})

            # Sort
            names = athlete_counts['Name']
            counts = [int(c) for c in athlete_counts['Medal']]
            data = sorted(zip(names, counts), key=lambda x: x[1], reverse=True)

            # Display top 5
            for idx, (name, count) in enumerate(data[:5], 1):
                # Get NOC
                noc = 'UNK'
                for i in range(len(athlete_data)):
                    if athlete_data['Name'][i] == name:
                        noc = athlete_data['NOC'][i]
                        break

                name_display = name[:45] if len(name) > 45 else name
                print(f"| {idx}. {name_display:<45} | {noc:3s} | {count:3d} medals |")

            print(f"|{'-'*78}|")


    def report_6_medal_distribution(self) -> None:
        """
        Report 6: Overall Medal Distribution Summary
        """

        print("\n\n" + "="*80)
        print("REPORT 6: OLYMPIC MEDAL DISTRIBUTION SUMMARY")
        print("="*80 + "\n")

        # Filter for medals
        medal_winners = self.athletes_df.filter(
            lambda row: row['Medal'] in ['Gold', 'Silver', 'Bronze']
        )

        # Count each medal type
        medals = medal_winners['Medal']
        gold_count = medals.count('Gold')
        silver_count = medals.count('Silver')
        bronze_count = medals.count('Bronze')
        total_medals = len(medals)

        # Unique athletes
        unique_athletes = len(set(medal_winners['Name']))

        # Unique countries
        unique_countries = len(set(medal_winners['NOC']))

        # Unique sports
        unique_sports = len(set(medal_winners['Sport']))

        # Unique years
        unique_years = len(set(medal_winners['Year']))

        # Generate summary
        print("|" + "-"*78 + "|")
        print("|" + " "*24 + "OVERALL STATISTICS" + " "*34 + "|")
        print("|" + "-"*78 + "|")
        print(f"| Total Medals Awarded:                                    {total_medals:13,} |")
        print(f"|   * Gold Medals:                                         {gold_count:13,} |")
        print(f"|   * Silver Medals:                                       {silver_count:13,} |")
        print(f"|   * Bronze Medals:                                       {bronze_count:13,} |")
        print("|" + "-"*78 + "|")
        print(f"| Unique Medal-Winning Athletes:                           {unique_athletes:13,} |")
        print(f"| Countries that Won Medals:                               {unique_countries:13,} |")
        print(f"| Sports with Medals:                                      {unique_sports:13,} |")
        print(f"| Olympic Years Represented:                               {unique_years:13,} |")
        print("|" + "-"*78 + "|")

        # Percentages
        gold_pct = (gold_count / total_medals * 100) if total_medals > 0 else 0
        silver_pct = (silver_count / total_medals * 100) if total_medals > 0 else 0
        bronze_pct = (bronze_count / total_medals * 100) if total_medals > 0 else 0

        print("\nMEDAL DISTRIBUTION:")
        print(f"   * Gold:   {gold_pct:5.1f}%  {'*' * int(gold_pct/2)}")
        print(f"   * Silver: {silver_pct:5.1f}%  {'*' * int(silver_pct/2)}")
        print(f"   * Bronze: {bronze_pct:5.1f}%  {'*' * int(bronze_pct/2)}")

        # Average medals per athlete
        avg_medals_per_athlete = total_medals / unique_athletes if unique_athletes > 0 else 0
        print("\nINSIGHTS:")
        print(f"   * Average medals per athlete: {avg_medals_per_athlete:.2f}")
        print(f"   * Average medals per country: {total_medals/unique_countries:.1f}")
        print(f"   * Average medals per sport: {total_medals/unique_sports:.1f}")


def main():
    """Generate all summary reports."""

    print("\n" + "="*80)
    print(" "*20 + "OLYMPIC DATA - SUMMARY REPORTS")
    print("="*80)

    # Generate all reports
    summary_obj = SummaryReport(athlete_data_path="athlete_events.csv", noc_regions_path="noc_regions.csv")
    summary_obj.report_1_top_countries_by_gold()
    summary_obj.report_2_top_countries_total_medals()
    summary_obj.report_3_most_successful_athletes()
    summary_obj.report_4_gold_medal_champions()
    summary_obj.report_5_sport_excellence()
    summary_obj.report_6_medal_distribution()

    print("\n" + "="*80)
    print("ALL REPORTS GENERATED SUCCESSFULLY")
    print("="*80)
    print("\nReports include:")
    print("  * Top 10 Countries by Gold Medals")
    print("  * Top 10 Countries by Total Medals")
    print("  * Top 10 Most Successful Athletes (All Medals)")
    print("  * Top 10 Gold Medal Champions")
    print("  * Sport Excellence (Top Athletes per Sport)")
    print("  * Overall Medal Distribution Summary")
    print("\nDataFrame operations used: filter, project, group_by, agg, join")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
