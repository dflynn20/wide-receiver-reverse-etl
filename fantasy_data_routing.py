import os
import pandas as pd
import pandas as pd
import pytz
import requests
import time

from datetime import datetime
from espn_api.football import League
from sqlalchemy import create_engine
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


def write_to_snowflake():
    snowflakeUser = os.environ.get('SF_USER')
    snowflakePassword = os.environ.get('SF_PASSWORD')
    snowflakeWarehouse = os.environ.get('SF_WH')
    snowflakeDatabase = os.environ.get('SF_DB')
    snowflakeAccount = os.environ.get('SF_ACCOUNT')
    snowflakeSchema = os.environ.get('SF_SCHEMA')
    league_id = int(os.environ.get('LEAGUE_ID'))
    year = int(os.environ.get('YEAR'))
    espn_s2 = os.environ.get('ESPN_S2')
    swid = os.environ.get('ESPN_SWID')

    engineStr = f'snowflake://{snowflakeUser}:{snowflakePassword}@{snowflakeAccount}/{snowflakeDatabase}/{snowflakeSchema}?warehouse={snowflakeWarehouse}'
    engine = create_engine(engineStr)

    league = League(league_id=league_id, year=year, espn_s2=espn_s2, swid=swid)
    print(f"Successfully Initialized League, leagueId: {league_id} in the year: {year}")

    dt_pt = datetime.now(pytz.timezone('America/Los_Angeles'))
    current_as_of = dt_pt.strftime("%D %H:%M %p")

    teams = league.teams

    last_week = teams[0].outcomes.index('UNDECIDED')

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

    for k, tup_ar in week_score_tup_map.items():
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

    team_df.to_sql('fantasy_teams', engine, index=False, if_exists='replace')
    print("Wrote to fantasy_teams to Snowflake.")

    games_df.to_sql('fantasy_scores', engine, index=False, if_exists='replace')
    print("Wrote to fantasy_scores to Snowflake.")

    time_df.to_sql('fantasy_time', engine, index=False, if_exists='replace')
    print("Wrote to fantasy_time to Snowflake.")

write_to_snowflake()

print("Sleeping for 5 seconds")
time.sleep(5)

#  Part 5 about syncing data where you want!

def trigger_census_syncs(census_sync_numbers):
    for i, sync_num in enumerate(census_sync_numbers):
        url = 'https://bearer:secret-token:'+os.environ.get('CENSUS_SECRET')+'@app.getcensus.com/api/v1/syncs/'+str(sync_num)+'/trigger'
        requests.post(url)
        print(f"Triggered Census sync number {i+1}.")

# Insert Census sync urls here
census_sync_numbers = [8248,8250,8251]

trigger_census_syncs(census_sync_numbers)
