import pandas as pd
from pathlib import Path
from itertools import product
from urllib.request import urlopen
import tarfile
from typing import Union, Sequence, Optional, List
from io import BytesIO, TextIOWrapper
import csv
import streamlit as st
from nba_api.stats.endpoints import shotchartdetail,leaguedashplayerstats,leaguegamelog,boxscoretraditionalv2


st.set_page_config(page_title="NBA xPTS")


@st.cache_data
def load_nba_data(path: Union[Path, str] = Path.cwd(),
                  seasons: Union[Sequence, int] = range(1996, 2024),
                  data: Union[Sequence, str] = ("datanba", "nbastats", "pbpstats",
                                                "shotdetail", "cdnnba", "nbastatsv3"),
                  seasontype: str = 'rg',
                  league: str = 'nba',
                  untar: bool = False,
                  in_memory: bool = False,
                  use_pandas: bool = True) -> Optional[Union[List, pd.DataFrame]]:
    """
    Loading a nba play-by-play dataset from github repository https://github.com/shufinskiy/nba_data

    Args:
        path (Union[Path, str]): Path where downloaded file should be saved on the hard disk. Not used if in_memory = True
        seasons (Union[Sequence, int]): Sequence or integer of the year of start of season
        data (Union[Sequence, str]): Sequence or string of data types to load
        seasontype (str): Part of season: rg - Regular Season, po - Playoffs
        league (str): Name league: NBA or WNBA
        untar (bool): Logical: do need to untar loaded archive. Not used if in_memory = True
        in_memory (bool): Logical: If True dataset is loaded into workflow, without saving file to disk
        use_pandas (bool): Logical: If True dataset is loaded how pd.DataFrame, else List[List[str]]. Ignore if in_memory=False

    Returns:
        Optional[pd.DataFrame, List]: If in_memory=True and use_pandas=True return dataset how pd.DataFrame.
        If use_pandas=False return dataset how List[List[str]]
        If in_memory=False return None
    """
    if isinstance(path, str):
        path = Path(path).expanduser()
    if isinstance(seasons, int):
        seasons = (seasons,)
    if isinstance(data, str):
        data = (data,)

    if (len(data) > 1) & in_memory:
        raise ValueError("Parameter in_memory=True available only when loading a single data type")

    if seasontype == 'rg':
        need_data = tuple(["_".join([data, str(season)]) for (data, season) in product(data, seasons)])
    elif seasontype == 'po':
        need_data = tuple(["_".join([data, seasontype, str(season)]) \
                           for (data, seasontype, season) in product(data, (seasontype,), seasons)])
    else:
        need_data_rg = tuple(["_".join([data, str(season)]) for (data, season) in product(data, seasons)])
        need_data_po = tuple(["_".join([data, seasontype, str(season)]) \
                              for (data, seasontype, season) in product(data, ('po',), seasons)])
        need_data = need_data_rg + need_data_po
    if league.lower() == 'wnba':
        need_data = ['wnba_' + x for x in need_data]

    check_data = [file + ".csv" if untar else "tar.xz" for file in need_data]
    not_exists = [not path.joinpath(check_file).is_file() for check_file in check_data]

    need_data = [file for (file, not_exist) in zip(need_data, not_exists) if not_exist]

    with urlopen("https://raw.githubusercontent.com/shufinskiy/nba_data/main/list_data.txt") as f:
        v = f.read().decode('utf-8').strip()

    name_v = [string.split("=")[0] for string in v.split("\n")]
    element_v = [string.split("=")[1] for string in v.split("\n")]

    need_name = [name for name in name_v if name in need_data]
    need_element = [element for (name, element) in zip(name_v, element_v) if name in need_data]

    if in_memory:
        if use_pandas:
            table = pd.DataFrame()
        else:
            table = []
    for i in range(len(need_name)):
        with urlopen(need_element[i]) as response:
            if response.status != 200:
                raise Exception(f"Failed to download file: {response.status}")
            file_content = response.read()
            if in_memory:
                with tarfile.open(fileobj=BytesIO(file_content), mode='r:xz') as tar:
                    csv_file_name = "".join([need_name[i], ".csv"])
                    csv_file = tar.extractfile(csv_file_name)
                    if use_pandas:
                        table = pd.concat([table, pd.read_csv(csv_file)], axis=0, ignore_index=True)
                    else:
                        csv_reader = csv.reader(TextIOWrapper(csv_file, encoding="utf-8"))
                        for row in csv_reader:
                            table.append(row)
            else:
                with path.joinpath("".join([need_name[i], ".tar.xz"])).open(mode='wb') as f:
                    f.write(file_content)
                if untar:
                    with tarfile.open(path.joinpath("".join([need_name[i], ".tar.xz"]))) as f:
                        f.extract("".join([need_name[i], ".csv"]), path)

                    path.joinpath("".join([need_name[i], ".tar.xz"])).unlink()
    if in_memory:
        return table
    else:
        return None

def get_game_ids_from_date(date):

    # Fetch scoreboard for the specified date
    try:        
        game_ids = total_games_df[total_games_df['GAME_DATE']==date]['GAME_ID'].unique()
        return game_ids
    except Exception as e:
        st.write(f"An error occurred: {e}")
        return []

def get_box_scores(game_id):
    """
    Fetch traditional box scores for a given game ID.
    """
    try:
        # Fetch the box scores
        boxscore = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
        
        # Retrieve dataframes for player and team stats
        player_stats = boxscore.player_stats.get_data_frame()

        return player_stats
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def get_fg(player_season_df, shot_type, shot_zone_basic, shot_area):

    xPTS = 0
    shots_df = player_season_df[(player_season_df['SHOT_ZONE_BASIC']==shot_zone_basic) & (player_season_df['SHOT_ZONE_AREA']== shot_area) & (player_season_df['ACTION_TYPE']==shot_type)]
    vol = len(shots_df)
    #st.write(player_season_df['PLAYER_NAME'].values[0],shot_type, shot_zone_basic, shot_area, f"volume :{vol}")
    if vol > 5:
        xPTS = len(shots_df[shots_df['SHOT_MADE_FLAG']==1]) / vol
        #st.write(f"add {min(vol/150,.25)} to {xPTS}  total = {xPTS+min(vol/150,.25)}")
        return min(xPTS+min(vol/150,.25),.95)
    else:
        if len(player_season_df[player_season_df['SHOT_ZONE_BASIC']==shot_zone_basic])==0:
            return 0
        else :
            xPTS = len(player_season_df[(player_season_df['SHOT_ZONE_BASIC']==shot_zone_basic) & (shots_df['SHOT_MADE_FLAG']==1)]) / len(player_season_df[player_season_df['SHOT_ZONE_BASIC']==shot_zone_basic])
            #st.write(f"add {min(vol/250,.25)} to {xPTS}  total = {xPTS+min(vol/250,.25)}")
            return min(xPTS+min(vol/250,.25),.95)
        
def get_player_xpts(player_id,game_shotchart):
        
    xPTS=0
    #player_name = game_boxscore.loc[player_id]['PLAYER_NAME']
    player_season_shotchart = df[df['PLAYER_ID']==player_id]
    player_game_shotchart = game_shotchart[game_shotchart['PLAYER_ID']==player_id]

    for _,shot in player_game_shotchart.iterrows():
        shot_xpts = get_fg(player_season_shotchart,shot['ACTION_TYPE'] , shot['SHOT_ZONE_BASIC'], shot['SHOT_ZONE_AREA'])
        #st.write(player_name,shot['ACTION_TYPE'] , shot['SHOT_ZONE_BASIC'], shot['SHOT_ZONE_AREA'],int(shot['SHOT_TYPE']),shot_xpts)
        xPTS += int(shot['SHOT_TYPE']) * shot_xpts
        #st.write(f"THEREFORE xPTS = {int(shot['SHOT_TYPE']) * shot_xpts}")
   
    #st.write(f"ADD FT : {round(game_boxscore.loc[player_id,'FTA'] * player_FTpct_df.loc[player_id,'FT_PCT'],1)}")    
    xPTS += game_boxscore.loc[player_id,'FTA'] * player_FTpct_df.loc[player_id,'FT_PCT']

    return round(xPTS,1)


team_logos = {
    'ATL': 'https://loodibee.com/wp-content/uploads/nba-atlanta-hawks-logo.png',
    'BOS': 'https://loodibee.com/wp-content/uploads/nba-boston-celtics-logo.png',
    'BKN': 'https://loodibee.com/wp-content/uploads/nba-brooklyn-nets-logo.png',
    'CHA': 'https://loodibee.com/wp-content/uploads/nba-charlotte-hornets-logo.png',
    'CHI': 'https://loodibee.com/wp-content/uploads/nba-chicago-bulls-logo.png',
    'CLE': 'https://loodibee.com/wp-content/uploads/Clevelan-Cavaliers-logo-2022.png',
    'DAL': 'https://loodibee.com/wp-content/uploads/nba-dallas-mavericks-logo.png',
    'DEN': 'https://loodibee.com/wp-content/uploads/nba-denver-nuggets-logo-2018.png',
    'DET': 'https://loodibee.com/wp-content/uploads/nba-detroit-pistons-logo.png',
    'GSW': 'https://loodibee.com/wp-content/uploads/nba-golden-state-warriors-logo.png',
    'HOU': 'https://loodibee.com/wp-content/uploads/houston-rockets-logo-symbol.png',
    'IND': 'https://loodibee.com/wp-content/uploads/nba-indiana-pacers-logo.png',
    'LAC': 'https://loodibee.com/wp-content/uploads/NBA-LA-Clippers-logo-2024.png',
    'LAL': 'https://loodibee.com/wp-content/uploads/nba-los-angeles-lakers-logo.png',
    'MEM': 'https://loodibee.com/wp-content/uploads/nba-memphis-grizzlies-logo.png',
    'MIA': 'https://loodibee.com/wp-content/uploads/nba-miami-heat-logo.png',
    'MIL': 'https://loodibee.com/wp-content/uploads/nba-milwaukee-bucks-logo.png',
    'MIN': 'https://loodibee.com/wp-content/uploads/nba-minnesota-timberwolves-logo.png',
    'NOP': 'https://loodibee.com/wp-content/uploads/nba-new-orleans-pelicans-logo.png',
    'NYK': 'https://loodibee.com/wp-content/uploads/nba-new-york-knicks-logo.png',
    'OKC': 'https://loodibee.com/wp-content/uploads/nba-oklahoma-city-thunder-logo.png',
    'ORL': 'https://loodibee.com/wp-content/uploads/nba-orlando-magic-logo.png',
    'PHI': 'https://loodibee.com/wp-content/uploads/nba-philadelphia-76ers-logo.png',
    'PHX': 'https://loodibee.com/wp-content/uploads/nba-phoenix-suns-logo.png',
    'POR': 'https://loodibee.com/wp-content/uploads/nba-portland-trail-blazers-logo.png',
    'SAC': 'https://loodibee.com/wp-content/uploads/nba-sacramento-kings-logo.png',
    'SAS': 'https://loodibee.com/wp-content/uploads/nba-san-antonio-spurs-logo.png',
    'TOR': 'https://loodibee.com/wp-content/uploads/nba-toronto-raptors-logo.png',
    'UTA': 'https://loodibee.com/wp-content/uploads/nba-utah-jazz-logo.png',
    'WAS': 'https://loodibee.com/wp-content/uploads/nba-washington-wizards-logo.png'
}


st.write('# Game xPTS')
total_games_df = pd.concat([leaguegamelog.LeagueGameLog().get_data_frames()[0],leaguegamelog.LeagueGameLog(season_type_all_star="Playoffs").get_data_frames()[0]])
total_games_df=total_games_df.sort_values(['GAME_DATE'], ascending=True)


df = load_nba_data(
    seasons=2024,
    data="shotdetail",
    in_memory=True,
    seasontype = 'rg'
)

df=df[['PLAYER_NAME','LOC_X','LOC_Y','SHOT_TYPE','ACTION_TYPE','SHOT_ZONE_BASIC', 'SHOT_ZONE_AREA','SHOT_MADE_FLAG','PLAYER_ID']]
# Reverse left-right because of data gathering from the NBA is the other way around.
df['LOC_X'] = df['LOC_X'].apply(lambda x:-x)


player_FTpct_df = leaguedashplayerstats.LeagueDashPlayerStats(season='2024-25').get_data_frames()[0][['PLAYER_ID','FT_PCT']]
player_FTpct_df = player_FTpct_df.set_index('PLAYER_ID')


selected_date = st.selectbox(
    "Select the date",
    total_games_df['GAME_DATE'].unique()[::-1],
    index=0,
    placeholder="Select a date ...")


game_ids = get_game_ids_from_date(selected_date)
rectangles = []
try:

    for game_id in game_ids:

        game_boxscore = get_box_scores(game_id)[['PLAYER_ID','TEAM_ID','TEAM_ABBREVIATION','PLAYER_NAME','PTS','FGM','FGA','FG3M','FG3A','FTM','FTA','MIN']]

        game_boxscore=game_boxscore.dropna().set_index('PLAYER_ID')
        game_boxscore['MIN'] = game_boxscore['MIN'].apply(lambda min: min.split('.')[0] )

        game_shotchart = pd.concat([
        shotchartdetail.ShotChartDetail(
        team_id=0,
        player_id=0,
        game_id_nullable=game_id,
        context_measure_simple='FGA',
        season_type_all_star="Regular Season"
        ).get_data_frames()[0],

        shotchartdetail.ShotChartDetail(
        team_id=0,
        player_id=0,
        game_id_nullable=game_id,
        context_measure_simple='FGA',
        season_type_all_star="Playoffs"
        ).get_data_frames()[0]])

        game_shotchart['SHOT_TYPE'] = game_shotchart['SHOT_TYPE'].apply(lambda x: x[0])
        game_shotchart['LOC_X'] = game_shotchart['LOC_X'].apply(lambda x:-x)



        xPTS_list = []
        for player_id,player in game_boxscore.iterrows():

            if (player['FGA'] + player['FTA']) == 0:
                xPTS_list.append(0)
            else:
                xPTS_list.append(get_player_xpts(player_id,game_shotchart))

        game_boxscore.insert(4,'xPTS',xPTS_list)

        home_team,away_team = game_boxscore['TEAM_ABBREVIATION'].unique()

        home_team_boxscore = game_boxscore[game_boxscore['TEAM_ABBREVIATION']==home_team].drop(['TEAM_ID','TEAM_ABBREVIATION'],axis=1)
        away_team_boxscore = game_boxscore[game_boxscore['TEAM_ABBREVIATION']==away_team].drop(['TEAM_ID','TEAM_ABBREVIATION'],axis=1)


        rectangles.append(
            {
                "home_team": f"{home_team}",
                "away_team": f"{away_team}",
                "score": f"{round(home_team_boxscore['PTS'].sum())} - {round(away_team_boxscore['PTS'].sum())}",
                "x_score": f"{round(home_team_boxscore['xPTS'].sum(),1)} - {round(away_team_boxscore['xPTS'].sum(),1)}",
                "Home_boxscore": home_team_boxscore,
                "Away_boxscore": away_team_boxscore
            })
    

    rectangles_df = pd.DataFrame(rectangles)

    for _, game in rectangles_df.iterrows():

        st.markdown("---")
        with st.container():
            col1, col2, col3 = st.columns([1, 4, 1])

            # Team logos
            with col1:
                st.image(team_logos[game['home_team']], use_container_width =True)

            with col3:
                st.image(team_logos[game['away_team']], use_container_width =True)

            # Game score
            with col2:
                st.markdown(
                    f"""
                    <div style='text-align: center; font-size: 110px;'>
                        {game["score"]}
                    </div>
                    <div style='text-align: center; color: gray; font-size: 60px;'>
                        {game["x_score"]}
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

            # Expandable boxscore
            with st.expander(f"{game['home_team']} @ {game['away_team']} Boxscore "):
                with st.container():
                    c1, c2 = st.columns([2, 2])
                    with c1:
                        st.write(game["Home_boxscore"].sort_values(by=['PTS','xPTS'],ascending=False).reset_index(drop=True))
                    with c2:
                        st.write(game["Away_boxscore"].sort_values(by=['PTS','xPTS'],ascending=False).reset_index(drop=True))



except Exception as e:
    st.write(f"An error occurred: {e}")
    st.write(f"No games on {selected_date}")

