import os
import pandas as pd
import pytz
import requests
import time

from datetime import datetime
from espn_api.football import League
from sqlalchemy import create_engine
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

#  This initializes the league, so we have the data as Python objects
def initialize_league():
    league_id = int(os.environ.get('LEAGUE_ID'))
    year = int(os.environ.get('YEAR'))
    espn_s2 = os.environ.get('ESPN_S2')
    swid = os.environ.get('ESPN_SWID')
    league = League(league_id=league_id, year=year, espn_s2=espn_s2, swid=swid)
    print(f"Successfully Initialized League, leagueId: {league_id} in the year: {year}")
    return league, league_id, year


#  This creates the team DataFrame, as well as the time DataFrame
def create_teams_and_times(teams, last_week, current_as_of, league_id, year):
    week_score_tup_map = {}
    for i in range(last_week):
        week_score_tup_map[i+1] = []
    
    teams_table = []
    time_checked_table = []

    for t in teams:
        team_row = {}
        team_row['id'] = t.team_id
        team_row['owner'] = t.owner
        team_row['points_for'] = t.points_for
        team_row['points_against'] = t.points_against
        team_row['head_to_head_wins'] = t.wins
        team_row['head_to_head_losses'] = t.losses
        
        # Initializing to 0, because we need some computations
        team_row['points_for_wins'] = 0
        team_row['points_for_losses'] = 0
        team_row['weeks_top_scorer'] = 0
        team_row['weeks_bottom_scorer'] = 0
        team_row['avg_weekly_ranking'] = 0

        teams_table.append(team_row)

        for j in range(last_week):
            week_score_tup_map[j+1].append((t.team_id, t.scores[j]))

        time_checked = {'current_as_of': current_as_of, 'week_number': last_week + 1, 'league_id': league_id, 'year': year, 'team_id': t.team_id}
        time_checked_table.append(time_checked)

    team_df = pd.DataFrame(teams_table)
    time_df = pd.DataFrame(time_checked_table)
    
    return week_score_tup_map, team_df, time_df

#  This creates the past metrics for the current standings and what the current matchup is looking like.
def create_rankings_and_scores(league, last_week, team_df, week_score_tup_map):
    for _, tup_ar in week_score_tup_map.items():
        sorted_scores = sorted(tup_ar, key = lambda x: x[1], reverse=True)
        for index, tup in enumerate(sorted_scores):
            team_id = tup[0]
            weeklyRanking = index + 1
            if weeklyRanking < 7:
                team_df.loc[team_df['id'] == team_id,'points_for_wins'] += 1
                if weeklyRanking == 1:  team_df.loc[team_df['id'] == team_id,'weeks_top_scorer'] += 1
            else:
                team_df.loc[team_df['id'] == team_id,'points_for_losses'] += 1
                if weeklyRanking == 12:  team_df.loc[team_df['id'] == team_id,'weeks_bottom_scorer'] += 1
            team_df.loc[team_df['id'] == team_id,'avg_weekly_ranking'] += weeklyRanking / last_week

    team_df['avg_weekly_ranking'].apply(lambda x: round(x,2))
    team_df['total_league_points'] = team_df['points_for_wins'] + team_df['head_to_head_wins']

    current_week_scores = []

    box_scores = league.box_scores(last_week + 1)

    for game_i, bs in enumerate(box_scores):
        home_row, away_row = {}, {}
        
        home_row['team_id'] = bs.home_team.team_id
        home_row['team_owner'] = bs.home_team.owner
        home_row['team_score'] = bs.home_score
        home_row['team_projected_score'] = bs.home_projected
        home_row['game_id'] = game_i + 1
        home_row['home_away'] = 'home'
        current_week_scores.append(home_row)
        
        away_row['team_id'] = bs.away_team.team_id
        away_row['team_owner'] = bs.away_team.owner
        away_row['team_score'] = bs.away_score
        away_row['team_projected_score'] = bs.away_projected
        away_row['game_id'] = game_i + 1
        away_row['home_away'] = 'away'
        current_week_scores.append(away_row)

    games_df = pd.DataFrame(current_week_scores)

    return team_df, games_df


#  This creates the SQL Alchemy engine to write to Snowflake
def create_snowflake_engine():
    snowflake_user = os.environ.get('SF_USER')
    snowflake_password = os.environ.get('SF_PASSWORD')
    snowflake_warehouse = os.environ.get('SF_WH')
    snowflake_database = os.environ.get('SF_DB')
    snowflake_account = os.environ.get('SF_ACCOUNT')
    snowflake_schema = os.environ.get('SF_SCHEMA')
    engine_str = f'snowflake://{snowflake_user}:{snowflake_password}@{snowflake_account}/{snowflake_database}/{snowflake_schema}?warehouse={snowflake_warehouse}'
    engine = create_engine(engine_str)
    print("Created Snowflake engine")
    return engine

#  This takes the pandas DataFrames and writes them to Snowflake tables
def write_to_snowflake(team_df, games_df, time_df):
    engine = create_snowflake_engine()
    
    team_table_name = 'fantasy_teams'
    team_df.to_sql(team_table_name, engine, index=False, if_exists='replace')
    print(f"Wrote {team_table_name} to Snowflake.")

    scores_table_name = 'fantasy_scores'
    games_df.to_sql(scores_table_name, engine, index=False, if_exists='replace')
    print(f"Wrote {scores_table_name} to Snowflake.")

    time_table_name = 'fantasy_time'
    time_df.to_sql(time_table_name, engine, index=False, if_exists='replace')
    print(f"Wrote {time_table_name} to Snowflake.")


#  This extracts the data from ESPN Fantasy and loads it into Snowflake
def run_el_script():
    league, league_id, year = initialize_league()
    
    dt_pt = datetime.now(pytz.timezone('America/Los_Angeles'))
    current_as_of = dt_pt.strftime("%D %H:%M %p")

    teams = league.teams

    # This pulls the index of the first week that has not been finalized
    last_week = teams[0].outcomes.index('UNDECIDED')

    week_score_tup_map, team_df, time_df = create_teams_and_times(teams, last_week, current_as_of, league_id, year)

    team_df, games_df = create_rankings_and_scores(league, last_week, team_df, week_score_tup_map)

    write_to_snowflake(team_df, games_df, time_df)

#  Part 5 about syncing data where you want!
def trigger_census_syncs():
    total_census_syncs = int(os.environ.get('TOTAL_CENSUS_SYNCS'))
    for i in range(total_census_syncs):
        sync_num = os.environ.get(f'CENSUS_SYNC_{i+1}')
        url = 'https://bearer:secret-token:'+os.environ.get('CENSUS_SECRET')+'@app.getcensus.com/api/v1/syncs/'+sync_num+'/trigger'
        requests.post(url)
        print(f"Triggered Census sync number {i+1}.")  


run_el_script()
time.sleep(2)
trigger_census_syncs()
print("Done with script!")
