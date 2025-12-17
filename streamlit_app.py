"""
Comprehensive Streamlit App for Olympic Athletes Data Analysis
Uses ALL classes from the project_chede folder with interactive filtering and visualization.
"""

import streamlit as st
from dataframe_loader import DataFrame
from utils import load_data_as_dataframe, is_numeric
import os
import sys

# Import all analysis classes from the project
sys.path.insert(0, os.path.dirname(__file__))

# Import all analysis modules
from medal_analysis import MedalAnalyzer
from multiple_olympics_athletes import MultipleOlympicAnalyzer
from grouping_aggregation_examples import ProjectionApplication as GroupingApp
from summary_reports import SummaryReport

# Set page configuration
st.set_page_config(
    page_title="Olympic Athletes Analysis Platform",
    page_icon="🏅",
    layout="wide",
    initial_sidebar_state="expanded"
)


@st.cache_data
def load_data():
    """Load the Olympic athletes dataset."""
    csv_path = os.path.join(os.path.dirname(__file__), 'athlete_events.csv')
    return load_data_as_dataframe(csv_path)


@st.cache_data
def load_noc_data():
    """Load the NOC regions dataset."""
    csv_path = os.path.join(os.path.dirname(__file__), 'noc_regions.csv')
    return load_data_as_dataframe(csv_path)


def get_unique_values(df: DataFrame, column: str) -> list:
    """Get unique values from a DataFrame column, sorted."""
    values = list(set(df[column]))
    values = [v for v in values if v and v != 'NA']
    return sorted(values)


def filter_dataframe(df: DataFrame, filters: dict) -> DataFrame:
    """Apply multiple filters to a DataFrame."""

    def apply_all_filters(row):
        if filters['sports'] and row['Sport'] not in filters['sports']:
            return False
        if filters['medals'] and row['Medal'] not in filters['medals']:
            return False
        if filters['countries'] and row['NOC'] not in filters['countries']:
            return False
        if filters['genders'] and row['Sex'] not in filters['genders']:
            return False
        if filters['year_range']:
            try:
                year = int(row['Year'])
                if year < filters['year_range'][0] or year > filters['year_range'][1]:
                    return False
            except (ValueError, TypeError):
                return False
        if filters['age_range']:
            age = row['Age']
            if age and age != 'NA':
                try:
                    age_val = float(age)
                    if age_val < filters['age_range'][0] or age_val > filters['age_range'][1]:
                        return False
                except (ValueError, TypeError):
                    return False
            else:
                return False
        if filters['seasons'] and row['Season'] not in filters['seasons']:
            return False
        return True

    return df.filter(apply_all_filters)


def display_dataframe(df: DataFrame, max_rows: int = 100):
    """Display DataFrame in a nice table format."""
    if len(df) == 0:
        st.warning("No data matches the selected filters.")
        return

    columns = list(df._data.keys())
    display_data = []
    for i in range(min(max_rows, len(df))):
        row = {col: df[col][i] for col in columns}
        display_data.append(row)

    st.dataframe(display_data, width=True)

    if len(df) > max_rows:
        st.info(f"Showing {max_rows} of {len(df):,} rows. Use filters to narrow down results.")


def show_statistics(df: DataFrame):
    """Display summary statistics for the filtered data."""
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Records", f"{len(df):,}")
    with col2:
        print(df.columns)
        unique_athletes = len(set(df['ID']))
        st.metric("Unique Athletes", f"{unique_athletes:,}")
    with col3:
        unique_sports = len({s for s in df['Sport'] if s and s != 'NA'})
        st.metric("Sports", unique_sports)
    with col4:
        medals = [m for m in df['Medal'] if m in ['Gold', 'Silver', 'Bronze']]
        st.metric("Medal Winners", f"{len(medals):,}")

    if medals:
        st.markdown("#### Medal Distribution")
        medal_col1, medal_col2, medal_col3 = st.columns(3)

        with medal_col1:
            st.metric("🥇 Gold", medals.count('Gold'))
        with medal_col2:
            st.metric("🥈 Silver", medals.count('Silver'))
        with medal_col3:
            st.metric("🥉 Bronze", medals.count('Bronze'))


def tab_data_explorer():
    """Tab for basic data exploration and filtering."""
    st.header("🔍 Data Explorer")
    st.markdown("Interactive filtering and exploration of Olympic athlete records")

    df = load_data()

    with st.expander("🔧 Filters", expanded=True):
        col1, col2, col3 = st.columns(3)

        all_sports = get_unique_values(df, 'Sport')
        all_countries = get_unique_values(df, 'NOC')

        with col1:
            selected_sports = st.multiselect("Sport", options=all_sports[:50])
            selected_medals = st.multiselect("Medal", options=['Gold', 'Silver', 'Bronze', 'NA'])

        with col2:
            selected_countries = st.multiselect("Country (NOC)", options=all_countries[:50])
            selected_genders = st.multiselect("Gender", options=['M', 'F'])

        with col3:
            selected_seasons = st.multiselect("Season", options=['Summer', 'Winter'])

            use_year_filter = st.checkbox("Filter by Year Range")
            year_range = None
            if use_year_filter:
                years = [int(y) for y in df['Year'] if y and y != 'NA']
                year_range = st.slider("Year Range", min(years), max(years), (min(years), max(years)))

    filters = {
        'sports': selected_sports,
        'medals': selected_medals,
        'countries': selected_countries,
        'genders': selected_genders,
        'seasons': selected_seasons,
        'year_range': year_range,
        'age_range': None
    }

    filtered_df = filter_dataframe(df, filters)

    st.markdown("---")
    show_statistics(filtered_df)

    st.markdown("---")
    st.subheader("📋 Data Table")
    max_rows = st.selectbox("Max rows", options=[50, 100, 250, 500], index=1)
    display_dataframe(filtered_df, max_rows=max_rows)


def tab_medal_analysis():
    """Tab for medal analysis using MedalAnalyzer class."""
    st.header("🏅 Medal Analysis")
    st.markdown("Analyze medals by age, sport, and demographics")

    data_path = os.path.join(os.path.dirname(__file__), 'athlete_events.csv')
    analyzer = MedalAnalyzer(data_path)

    analysis_type = st.selectbox(
        "Select Analysis Type",
        ["Medals by Age Range", "Medals by Sport", "Sport-Specific Age Analysis"]
    )

    if analysis_type == "Medals by Age Range":
        st.subheader("Medal Distribution by Age Range")

        with st.spinner("Analyzing medals by age..."):
            df = analyzer.athletes_df
            medal_winners = df.filter(lambda row: row['Medal'] in ['Gold', 'Silver', 'Bronze'])

            age_ranges = {
                '10-19 (Teens)': (10, 19),
                '20-29 (Twenties)': (20, 29),
                '30-39 (Thirties)': (30, 39),
                '40-49 (Forties)': (40, 49),
                '50+ (Fifties+)': (50, 100)
            }

            age_data = {r: {'Gold': 0, 'Silver': 0, 'Bronze': 0} for r in age_ranges}

            ages = medal_winners['Age']
            medals = medal_winners['Medal']

            for i in range(len(medal_winners)):
                age_str = ages[i]
                medal = medals[i]

                if age_str and age_str != 'NA':
                    try:
                        age = int(float(age_str))
                        for range_name, (min_age, max_age) in age_ranges.items():
                            if min_age <= age <= max_age:
                                age_data[range_name][medal] += 1
                                break
                    except (ValueError, TypeError):
                        pass

            for range_name, data in age_data.items():
                total = sum(data.values())
                if total > 0:
                    st.markdown(f"### {range_name}")
                    col1, col2, col3, col4 = st.columns(4)
                    col1.metric("Total", total)
                    col2.metric("🥇 Gold", data['Gold'])
                    col3.metric("🥈 Silver", data['Silver'])
                    col4.metric("🥉 Bronze", data['Bronze'])

    elif analysis_type == "Medals by Sport":
        st.subheader("Top Sports by Medal Count")
        top_n = st.slider("Number of sports to show", 5, 30, 15)

        with st.spinner("Analyzing sports..."):
            df = analyzer.athletes_df
            medal_winners = df.filter(lambda row: row['Medal'] in ['Gold', 'Silver', 'Bronze'])
            sport_stats = medal_winners.group_by('Sport').agg({'Medal': 'count'})

            sports = sport_stats['Sport']
            counts = [int(c) for c in sport_stats['Medal']]
            sport_data = sorted(zip(sports, counts), key=lambda x: x[1], reverse=True)[:top_n]

            for idx, (sport, count) in enumerate(sport_data, 1):
                st.markdown(f"**{idx}. {sport}** - {count:,} medals")

    else:  # Sport-Specific Age Analysis
        df = analyzer.athletes_df
        all_sports = get_unique_values(df, 'Sport')
        selected_sport = st.selectbox("Select Sport", options=all_sports[:100])

        if st.button("Analyze"):
            with st.spinner(f"Analyzing {selected_sport}..."):
                sport_medals = df.filter(
                    lambda row: row['Sport'] == selected_sport and row['Medal'] in ['Gold', 'Silver', 'Bronze']
                )

                if len(sport_medals) > 0:
                    ages = [float(a) for a in sport_medals['Age'] if a and a != 'NA' and is_numeric(a)]

                    if ages:
                        col1, col2, col3 = st.columns(3)
                        col1.metric("Youngest", f"{min(ages):.0f} years")
                        col2.metric("Average", f"{sum(ages)/len(ages):.1f} years")
                        col3.metric("Oldest", f"{max(ages):.0f} years")
                    else:
                        st.warning("No valid age data for this sport")
                else:
                    st.warning(f"No medal data found for {selected_sport}")


def tab_multi_olympic():
    """Tab for multi-Olympic athletes analysis."""
    st.header("🔁 Multi-Olympic Athletes")
    st.markdown("Analyze athletes who competed in multiple Olympic Games")

    data_path = os.path.join(os.path.dirname(__file__), 'athlete_events.csv')
    analyzer = MultipleOlympicAnalyzer(data_path)

    min_olympics = st.slider("Minimum number of Olympics", 2, 8, 3)

    if st.button("Find Multi-Olympic Athletes"):
        with st.spinner("Analyzing..."):
            df = analyzer.athletes_df
            relevant_df = df[['Name', 'Year', 'NOC', 'Sport', 'Medal']]

            athlete_data = {}
            names = relevant_df['Name']
            years = relevant_df['Year']
            nocs = relevant_df['NOC']
            medals = relevant_df['Medal']

            for i in range(len(relevant_df)):
                name = names[i]
                if name not in athlete_data:
                    athlete_data[name] = {
                        'years': set(),
                        'noc': nocs[i],
                        'medals': []
                    }
                athlete_data[name]['years'].add(years[i])
                if medals[i] and medals[i] != 'NA':
                    athlete_data[name]['medals'].append(medals[i])

            multi_olympic = {
                name: data for name, data in athlete_data.items()
                if len(data['years']) >= min_olympics
            }

            sorted_athletes = sorted(
                multi_olympic.items(),
                key=lambda x: len(x[1]['years']),
                reverse=True
            )

            st.success(f"Found {len(multi_olympic):,} athletes who competed in {min_olympics}+ Olympics")

            for idx, (name, data) in enumerate(sorted_athletes[:20], 1):
                num_olympics = len(data['years'])
                year_list = sorted(data['years'])
                num_medals = len(data['medals'])

                with st.expander(f"{idx}. {name} - {num_olympics} Olympics"):
                    col1, col2 = st.columns(2)
                    col1.write(f"**Country:** {data['noc']}")
                    col1.write(f"**Years:** {', '.join(year_list)}")
                    if num_medals > 0:
                        col2.write(f"**Total Medals:** {num_medals}")
                        col2.write(f"Gold: {data['medals'].count('Gold')}, "
                                 f"Silver: {data['medals'].count('Silver')}, "
                                 f"Bronze: {data['medals'].count('Bronze')}")


def tab_grouping_aggregation():
    """Tab for grouping and aggregation examples."""
    st.header("📊 Grouping & Aggregation")
    st.markdown("Advanced data aggregations and grouping operations")

    athlete_path = os.path.join(os.path.dirname(__file__), 'athlete_events.csv')
    noc_path = os.path.join(os.path.dirname(__file__), 'noc_regions.csv')
    app = GroupingApp(athlete_path, noc_path)

    analysis_type = st.selectbox(
        "Select Analysis",
        ["Medal Counts by Country", "Medal Counts by Sport", "Medal Counts by Country & Sport"]
    )

    if analysis_type == "Medal Counts by Country":
        top_n = st.slider("Top N countries", 5, 50, 25)

        if st.button("Analyze"):
            with st.spinner("Analyzing..."):
                medal_winners = app.athletes_df.filter(
                    lambda row: row['Medal'] in ['Gold', 'Silver', 'Bronze']
                )
                medals_df = medal_winners[['NOC', 'Medal']]
                noc_groups = medals_df.group_by('NOC')
                medal_counts = noc_groups.agg({'Medal': 'count'})
                with_names = medal_counts.join(app.noc_df, on='NOC', how='left')

                nocs = with_names['NOC']
                regions = with_names['region']
                counts = [int(c) for c in with_names['Medal']]
                data = sorted(zip(nocs, regions, counts), key=lambda x: x[2], reverse=True)[:top_n]

                st.subheader(f"Top {top_n} Countries by Medal Count")
                for idx, (noc, region, count) in enumerate(data, 1):
                    region_display = region if region else noc
                    st.markdown(f"**{idx}. {noc}** - {region_display}: **{count:,}** medals")

    elif analysis_type == "Medal Counts by Sport":
        top_n = st.slider("Top N sports", 5, 30, 20)

        if st.button("Analyze"):
            with st.spinner("Analyzing..."):
                medal_winners = app.athletes_df.filter(
                    lambda row: row['Medal'] in ['Gold', 'Silver', 'Bronze']
                )
                sport_medals = medal_winners[['Sport', 'Medal']]
                sport_groups = sport_medals.group_by('Sport')
                sport_counts = sport_groups.agg({'Medal': 'count'})

                sports = sport_counts['Sport']
                counts = [int(c) for c in sport_counts['Medal']]
                data = sorted(zip(sports, counts), key=lambda x: x[1], reverse=True)[:top_n]

                st.subheader(f"Top {top_n} Sports by Medal Count")
                for idx, (sport, count) in enumerate(data, 1):
                    st.markdown(f"**{idx}. {sport}**: **{count:,}** medals")

    else:  # Country & Sport
        if st.button("Analyze"):
            with st.spinner("Analyzing..."):
                medal_winners = app.athletes_df.filter(
                    lambda row: row['Medal'] in ['Gold', 'Silver', 'Bronze']
                )
                medals_df = medal_winners[['NOC', 'Sport', 'Medal']]
                groups = medals_df.group_by(['NOC', 'Sport'])
                counts = groups.agg({'Medal': 'count'})

                nocs = counts['NOC']
                sports = counts['Sport']
                medal_counts = [int(c) for c in counts['Medal']]
                data = sorted(zip(nocs, sports, medal_counts), key=lambda x: x[2], reverse=True)[:30]

                st.subheader("Top 30 Country-Sport Combinations")
                for idx, (noc, sport, count) in enumerate(data, 1):
                    st.markdown(f"**{idx}. {noc}** - {sport}: **{count:,}** medals")


def tab_summary_reports():
    """Tab for comprehensive summary reports."""
    st.header("📋 Summary Reports")
    st.markdown("Comprehensive analysis reports")

    athlete_path = os.path.join(os.path.dirname(__file__), 'athlete_events.csv')
    noc_path = os.path.join(os.path.dirname(__file__), 'noc_regions.csv')
    reporter = SummaryReport(athlete_path, noc_path)

    report_type = st.selectbox(
        "Select Report",
        [
            "Top Countries by Gold Medals",
            #"Top Countries by Total Medals",
            "Most Successful Athletes",
            #"Gold Medal Champions",
            "Overall Statistics"
        ]
    )

    if st.button("Generate Report"):
        with st.spinner("Generating report..."):
            if report_type == "Top Countries by Gold Medals":
                gold_medals = reporter.athletes_df.filter(lambda row: row['Medal'] == 'Gold')
                gold_by_country = gold_medals[['NOC', 'Medal']]
                country_groups = gold_by_country.group_by('NOC')
                gold_counts = country_groups.agg({'Medal': 'count'})
                with_names = gold_counts.join(reporter.noc_df, on='NOC', how='left')

                nocs = with_names['NOC']
                regions = with_names['region']
                counts = [int(c) for c in with_names['Medal']]
                data = sorted(zip(nocs, regions, counts), key=lambda x: x[2], reverse=True)[:10]

                st.subheader("🥇 Top 10 Countries by Gold Medals")
                for idx, (noc, region, count) in enumerate(data, 1):
                    region_display = region if region else noc
                    st.markdown(f"**{idx}. {noc}** - {region_display}: **{count:,}** gold medals")

            elif report_type == "Most Successful Athletes":
                medal_winners = reporter.athletes_df.filter(
                    lambda row: row['Medal'] in ['Gold', 'Silver', 'Bronze']
                )
                athlete_medals = medal_winners[['Name', 'NOC', 'Medal']]
                athlete_groups = athlete_medals.group_by('Name')
                athlete_counts = athlete_groups.agg({'Medal': 'count'})

                names = athlete_counts['Name']
                counts = [int(c) for c in athlete_counts['Medal']]
                data = sorted(zip(names, counts), key=lambda x: x[1], reverse=True)[:10]

                st.subheader("🏆 Top 10 Most Successful Athletes")
                for idx, (name, count) in enumerate(data, 1):
                    st.markdown(f"**{idx}. {name}**: **{count}** medals")

            elif report_type == "Overall Statistics":
                medal_winners = reporter.athletes_df.filter(
                    lambda row: row['Medal'] in ['Gold', 'Silver', 'Bronze']
                )

                medals = medal_winners['Medal']
                total = len(medals)
                gold = medals.count('Gold')
                silver = medals.count('Silver')
                bronze = medals.count('Bronze')

                col1, col2, col3 = st.columns(3)
                col1.metric("🥇 Gold Medals", f"{gold:,}")
                col2.metric("🥈 Silver Medals", f"{silver:,}")
                col3.metric("🥉 Bronze Medals", f"{bronze:,}")

                st.metric("Total Medals", f"{total:,}")
                st.metric("Unique Athletes", f"{len(set(medal_winners['Name'])):,}")
                st.metric("Countries", f"{len(set(medal_winners['NOC'])):,}")


def main():
    st.title("🏅 Olympic Athletes Analysis Platform")
    st.markdown("### Comprehensive data analysis using custom DataFrame operations")

    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 Data Explorer",
        "🏅 Medal Analysis",
        "🔁 Multi-Olympic",
        "📈 Aggregations",
        "📋 Reports",
        "About"
    ])

    with tab1:
        tab_data_explorer()

    with tab2:
        tab_medal_analysis()

    with tab3:
        tab_multi_olympic()

    with tab4:
        tab_grouping_aggregation()

    with tab5:
        tab_summary_reports()

    with tab6:
        st.header("About This Application")
        st.markdown("""
        ### Olympic Athletes Analysis Platform

        This application provides comprehensive analysis of historical Olympic athlete data using
        custom DataFrame implementations and various analytical classes.

        #### Classes Used:
        - **DataFrame** - Custom data structure with filter, project, group_by, agg, join operations
        - **MedalAnalyzer** - Analyze medals by age, sport, and demographics
        - **MultipleOlympicAnalyzer** - Find and analyze multi-Olympic athletes
        - **ProjectionApplication** - Grouping and aggregation examples
        - **SummaryReport** - Generate comprehensive reports
        - **JoinExamples** - Join operations demonstration

        #### Features:
        - 🔍 Interactive data filtering
        - 📊 Statistical analysis
        - 🏅 Medal distribution analysis
        - 🔁 Multi-Olympic athlete tracking
        - 📈 Grouping and aggregation
        - 📋 Comprehensive reporting
        - 💾 Data export capabilities

        #### Data Source:
        Historical Olympic athlete events dataset covering multiple Olympic Games
        """)


if __name__ == "__main__":
    main()
