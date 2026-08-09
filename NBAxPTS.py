"""
NBA xPTS - Game Expected Points App
====================================

METHODOLOGY NOTES (read before touching the modeling functions below)
-----------------------------------------------------------------------
The original version of this app computed "expected points" for a shot using
that SAME player's OWN season shooting percentage in the same zone/action
bucket. Three problems fell out of that design, and all three are fixed here:

1. CIRCULARITY: if "expected" comes from the player's own outcomes, then
   actual-vs-expected can never separate shot quality (were these good looks)
   from shot-making skill (did he convert at a normal rate) - it just measures
   whether he was hot or cold relative to himself. Fix: expected value now
   comes from a LEAGUE-WIDE baseline per bucket (all players pooled), which is
   independent of who's shooting. That gives us a real "shot quality" signal.

2. LEAKAGE: the old baseline used the full season (including the very game
   being evaluated, and including future games relative to whatever date was
   selected). Fix: every baseline (league rate AND player's own shrunk rate)
   is now built ONLY from shots that happened strictly BEFORE the selected
   game's date. This makes the model legitimate as a "did this outperform
   what we already knew going in" indicator instead of using hindsight.

3. AD HOC VOLUME BONUS: the old code added up to +0.25 to a bucket's FG% just
   because a player had >150 shots there, capped at .95, with no statistical
   basis. Fix: replaced with EMPIRICAL BAYES SHRINKAGE. A player's own bucket
   rate is blended with the league rate for that bucket, weighted by how many
   shots the player has actually taken there (a `k`-shot "prior strength").
   Few shots in a bucket -> rate is pulled hard toward league average.
   Many shots -> rate trusts the player's own sample more. This is a standard,
   explainable regression-to-the-mean technique instead of an arbitrary bonus.

OUTPUT SPLIT
------------
Because we now compute both a league-wide rate and a player-shrunk rate per
shot, we can report two separate numbers instead of one blended "xPTS":

  - SQ_xPTS  ("Shot Quality" points): value of the shots taken if converted
    at the LEAGUE-AVERAGE rate for that bucket. This isolates SHOT SELECTION
    - it answers "were these good looks," independent of who took them.

  - SK_xPTS  ("Skill-adjusted" points): value of the shots taken converted at
    the player's own SHRUNK rate (his prior history in that bucket, blended
    with the league prior). This isolates FINISHING SKILL going into the game
    - it answers "given what we already knew about this player's shooting,
    how many points should this shot diet have produced."

  Actual PTS - SK_xPTS  = did he outperform his own established shooting
                            ability tonight (a legitimate hot/cold read, since
                            SK_xPTS was built without using tonight's shots).
  SQ_xPTS vs league-average diet = shot selection quality (best read as a
                            season-to-date aggregate across games, not a
                            single game, since one game is a tiny sample of
                            "diet").

FALLBACK FOR THIN DATA
-----------------------
Early season, "shots strictly before this date" can be a very small sample
(or empty, for opening night). We cascade the baseline: exact bucket
(zone+area+action) -> zone-only -> league-global average, so we never divide
by zero or trust a 3-shot sample as if it were stable. See `lookup_rate`.
"""

import pandas as pd
from pathlib import Path
from itertools import product
from urllib.request import urlopen
import tarfile
from typing import Union, Sequence, Optional, List
from io import BytesIO, TextIOWrapper
import csv
import streamlit as st
from nba_api.stats.endpoints import (
    shotchartdetail,
    leaguedashplayerstats,
    leaguegamelog,
    boxscoretraditionalv2,
)

st.set_page_config(page_title="NBA xPTS", layout="wide")

# Shrinkage strength: number of "league-average pseudo-shots" blended into
# every player-bucket estimate. Higher k = trust the league prior more /
# require more of the player's own shots before believing his rate deviates
# from league average. 15-20 is a reasonable starting point for FG buckets;
# treat this as a tunable knob, not a fixed truth.
SHRINKAGE_K = 15


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
    (unchanged from original - this is just data acquisition, not modeling)
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
    try:
        game_ids = total_games_df[total_games_df['GAME_DATE'] == date]['GAME_ID'].unique()
        return game_ids
    except Exception as e:
        st.write(f"An error occurred: {e}")
        return []


def get_box_scores(game_id):
    """Fetch traditional box scores for a given game ID. (unchanged)"""
    try:
        boxscore = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
        player_stats = boxscore.player_stats.get_data_frame()
        return player_stats
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


# ---------------------------------------------------------------------------
# NEW MODELING CORE
# ---------------------------------------------------------------------------

BUCKET_COLS = ['SHOT_ZONE_BASIC', 'SHOT_ZONE_AREA', 'ACTION_TYPE']


def build_baseline_tables(shots_before: pd.DataFrame):
    """
    Build three levels of league-wide FG% baselines from shots that occurred
    strictly before the game being evaluated:

      1. bucket-level  (SHOT_ZONE_BASIC, SHOT_ZONE_AREA, ACTION_TYPE)
      2. zone-level    (SHOT_ZONE_BASIC only) - fallback when a bucket has
                        too few prior league-wide attempts to trust
      3. global rate   - final fallback if even the zone has no prior data
                        (can happen on literally the first game of a season)

    Returns a dict with all three, plus attempt counts so lookup_rate() can
    decide which level is trustworthy.
    """
    bucket = (shots_before.groupby(BUCKET_COLS)['SHOT_MADE_FLAG']
              .agg(makes='sum', attempts='count'))
    bucket['rate'] = bucket['makes'] / bucket['attempts']

    zone = (shots_before.groupby('SHOT_ZONE_BASIC')['SHOT_MADE_FLAG']
            .agg(makes='sum', attempts='count'))
    zone['rate'] = zone['makes'] / zone['attempts']

    if len(shots_before) > 0:
        global_rate = shots_before['SHOT_MADE_FLAG'].mean()
    else:
        # Absolute last resort (e.g. very first game of a season with zero
        # prior shots at all) - a generic average FG% placeholder so the app
        # doesn't crash. Flagged clearly rather than silently guessed.
        global_rate = 0.45

    return {'bucket': bucket, 'zone': zone, 'global': global_rate}


def lookup_league_rate(bucket_key, baseline, min_bucket_attempts=10):
    """
    League-wide expected FG% for a shot, cascading down when the exact
    bucket doesn't have enough prior league-wide volume to be stable.
    """
    zone_basic = bucket_key[0]
    if bucket_key in baseline['bucket'].index and \
       baseline['bucket'].loc[bucket_key, 'attempts'] >= min_bucket_attempts:
        return baseline['bucket'].loc[bucket_key, 'rate']
    if zone_basic in baseline['zone'].index:
        return baseline['zone'].loc[zone_basic, 'rate']
    return baseline['global']


def build_player_shrunk_rates(shots_before: pd.DataFrame, baseline: dict, k: int = SHRINKAGE_K):
    """
    Empirical Bayes shrinkage of each player's own prior-to-date bucket rate
    toward the league rate for that bucket:

        shrunk_rate = (player_makes + k * league_rate) / (player_attempts + k)

    k acts like injecting k "league-average" shots into every player-bucket
    combination before computing the rate. A player with 2 career shots in a
    bucket ends up very close to league average; a player with 300 shots
    there ends up very close to his own true rate. This replaces the old
    +min(vol/150,.25) bonus with a technique that has an actual statistical
    justification (it's the posterior mean under a Beta-Binomial model with
    a league-average prior).
    """
    player_bucket = (shots_before.groupby(['PLAYER_ID'] + BUCKET_COLS)['SHOT_MADE_FLAG']
                      .agg(makes='sum', attempts='count')).reset_index()

    def league_rate_for_row(row):
        return lookup_league_rate(
            (row['SHOT_ZONE_BASIC'], row['SHOT_ZONE_AREA'], row['ACTION_TYPE']),
            baseline
        )

    player_bucket['league_rate'] = player_bucket.apply(league_rate_for_row, axis=1)
    player_bucket['shrunk_rate'] = (
        (player_bucket['makes'] + k * player_bucket['league_rate']) /
        (player_bucket['attempts'] + k)
    )
    player_bucket = player_bucket.set_index(['PLAYER_ID'] + BUCKET_COLS)
    return player_bucket


def lookup_player_shrunk_rate(player_id, bucket_key, player_rates, baseline):
    """
    Shrunk expected FG% for a specific player + bucket. Falls back to the
    plain league rate (i.e. zero personal history) if we have literally no
    prior shots from this player anywhere before this date - e.g. a rookie's
    debut game. That's the correct behavior: with zero prior data, the best
    estimate of a new player's shooting IS the league average.
    """
    full_key = (player_id,) + bucket_key
    if full_key in player_rates.index:
        return player_rates.loc[full_key, 'shrunk_rate']
    return lookup_league_rate(bucket_key, baseline)


def get_player_game_values(player_id, game_shotchart, baseline, player_rates,
                            ft_pct_asof, game_boxscore):
    """
    Compute the two separated metrics for one player in one game:

      sq_pts (Shot Quality points)   -> league-average value of shots taken
      sk_pts (Skill-adjusted points) -> player's own shrunk-rate value of
                                          shots taken

    Both exclude the game itself and any future games by construction, since
    `baseline` and `player_rates` were built only from shots_before the
    selected date (enforced upstream, not here - this function just consumes
    whatever baseline it's given).
    """
    sq_pts = 0.0
    sk_pts = 0.0

    player_shots = game_shotchart[game_shotchart['PLAYER_ID'] == player_id]

    for _, shot in player_shots.iterrows():
        bucket_key = (shot['SHOT_ZONE_BASIC'], shot['SHOT_ZONE_AREA'], shot['ACTION_TYPE'])
        pt_value = int(shot['SHOT_TYPE'])  # '2' or '3', see PTS_VALUE extraction below

        league_rate = lookup_league_rate(bucket_key, baseline)
        shrunk_rate = lookup_player_shrunk_rate(player_id, bucket_key, player_rates, baseline)

        sq_pts += pt_value * league_rate
        sk_pts += pt_value * shrunk_rate

    # Free throws: still season-to-date FT%, but now fetched AS OF the day
    # before the selected game (see date_to_nullable usage below) rather than
    # full-season-inclusive, for the same no-leakage reason as the FG model.
    if player_id in ft_pct_asof.index and player_id in game_boxscore.index:
        fta = game_boxscore.loc[player_id, 'FTA']
        ft_rate = ft_pct_asof.loc[player_id, 'FT_PCT']
        sq_pts += fta * ft_rate  # league-average FT baseline not modeled separately;
        sk_pts += fta * ft_rate  # using the player's own prior FT% for both is reasonable
        # since FT shot quality doesn't vary by defense/look the way FG shot quality does -
        # there's no "shot selection" dimension to a free throw.

    return round(sq_pts, 1), round(sk_pts, 1)


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
st.caption(
    "SQ_xPTS = shot-quality points (value of the looks taken, at league-average "
    "rates). SK_xPTS = skill-adjusted points (value of those same looks at the "
    "player's own shooting history, shrunk toward league average). Both are "
    "computed using ONLY shots from before this game's date - tonight's shots "
    "never leak into the baseline used to evaluate tonight."
)

total_games_df = pd.concat([
    leaguegamelog.LeagueGameLog().get_data_frames()[0],
    leaguegamelog.LeagueGameLog(season_type_all_star="Playoffs").get_data_frames()[0]
])
total_games_df = total_games_df.sort_values(['GAME_DATE'], ascending=True)

# NOTE: added GAME_DATE (needed for the no-leakage temporal cutoff) alongside
# the columns the original code kept. If this historical dataset's schema
# doesn't include GAME_DATE under that exact name, adjust the column list
# below to match - the modeling logic is unaffected either way.
df = load_nba_data(
    seasons=2024,
    data="shotdetail",
    in_memory=True,
    seasontype='rg'
)

df = df[['PLAYER_NAME', 'LOC_X', 'LOC_Y', 'SHOT_TYPE', 'ACTION_TYPE',
         'SHOT_ZONE_BASIC', 'SHOT_ZONE_AREA', 'SHOT_MADE_FLAG', 'PLAYER_ID',
         'GAME_DATE']]
df['LOC_X'] = df['LOC_X'].apply(lambda x: -x)
df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'])

# Extract point value (2 or 3) once, up front, instead of re-deriving it
# inside the scoring loop each time - same underlying trick as the original
# (first character of the SHOT_TYPE string), just centralized and commented.
df['PTS_VALUE'] = df['SHOT_TYPE'].astype(str).str[0].astype(int)


selected_date = st.selectbox(
    "Select the date",
    total_games_df['GAME_DATE'].unique()[::-1],
    index=0,
    placeholder="Select a date ...")

selected_date_ts = pd.to_datetime(selected_date)

# --- Build the no-leakage baselines for THIS date, once, up front ---------
# Everything downstream (league rates, player shrunk rates) is built only
# from shots strictly before selected_date. This is the actual fix for
# problem #2 (leakage / self-inclusion) - it happens here, not inside the
# per-shot lookup functions, so there's a single place to audit the cutoff.
shots_before = df[df['GAME_DATE'] < selected_date_ts]

if len(shots_before) < 200:
    st.warning(
        f"Only {len(shots_before)} prior shots available league-wide before "
        f"{selected_date_ts.date()} (likely very early season). Baselines will "
        "lean heavily on zone-level / global fallbacks and will be noisy - "
        "treat SQ_xPTS/SK_xPTS for these games as low-confidence."
    )

baseline = build_baseline_tables(shots_before)
player_rates = build_player_shrunk_rates(shots_before, baseline, k=SHRINKAGE_K)

# Free throw % as of the day before the selected game, NOT full-season -
# same no-leakage principle applied to FT shooting.
day_before = (selected_date_ts - pd.Timedelta(days=1)).strftime('%m/%d/%Y')
player_FTpct_df = leaguedashplayerstats.LeagueDashPlayerStats(
    season='2024-25', date_to_nullable=day_before
).get_data_frames()[0][['PLAYER_ID', 'FT_PCT']]
player_FTpct_df = player_FTpct_df.set_index('PLAYER_ID')


game_ids = get_game_ids_from_date(selected_date)
rectangles = []
try:
    for game_id in game_ids:

        game_boxscore = get_box_scores(game_id)[
            ['PLAYER_ID', 'TEAM_ID', 'TEAM_ABBREVIATION', 'PLAYER_NAME',
             'PTS', 'FGM', 'FGA', 'FG3M', 'FG3A', 'FTM', 'FTA', 'MIN']
        ]
        game_boxscore = game_boxscore.dropna().set_index('PLAYER_ID')
        game_boxscore['MIN'] = game_boxscore['MIN'].apply(lambda m: m.split('.')[0])

        game_shotchart = pd.concat([
            shotchartdetail.ShotChartDetail(
                team_id=0, player_id=0, game_id_nullable=game_id,
                context_measure_simple='FGA', season_type_all_star="Regular Season"
            ).get_data_frames()[0],
            shotchartdetail.ShotChartDetail(
                team_id=0, player_id=0, game_id_nullable=game_id,
                context_measure_simple='FGA', season_type_all_star="Playoffs"
            ).get_data_frames()[0]
        ])
        game_shotchart['SHOT_TYPE'] = game_shotchart['SHOT_TYPE'].apply(lambda x: x[0])
        game_shotchart['LOC_X'] = game_shotchart['LOC_X'].apply(lambda x: -x)

        sq_list, sk_list = [], []
        for player_id, player in game_boxscore.iterrows():
            if (player['FGA'] + player['FTA']) == 0:
                sq_list.append(0.0)
                sk_list.append(0.0)
            else:
                sq, sk = get_player_game_values(
                    player_id, game_shotchart, baseline, player_rates,
                    player_FTpct_df, game_boxscore
                )
                sq_list.append(sq)
                sk_list.append(sk)

        game_boxscore.insert(4, 'SQ_xPTS', sq_list)
        game_boxscore.insert(5, 'SK_xPTS', sk_list)
        game_boxscore.insert(6, 'PTS_vs_SK', (game_boxscore['PTS'] - game_boxscore['SK_xPTS']).round(1))

        home_team, away_team = game_boxscore['TEAM_ABBREVIATION'].unique()

        home_team_boxscore = game_boxscore[game_boxscore['TEAM_ABBREVIATION'] == home_team].drop(
            ['TEAM_ID', 'TEAM_ABBREVIATION'], axis=1)
        away_team_boxscore = game_boxscore[game_boxscore['TEAM_ABBREVIATION'] == away_team].drop(
            ['TEAM_ID', 'TEAM_ABBREVIATION'], axis=1)

        # Team-level headline number uses SK_xPTS (skill-adjusted), not
        # SQ_xPTS. The question this app is actually meant to answer is
        # "based on each player's own known shooting ability, which team
        # should have won" - that requires each player's own (shrunk) rate,
        # not a hypothetical league-average shooter. SK_xPTS already IS each
        # player's own rate for high-volume players (shrinkage only matters
        # when a player's own sample is thin), so it's the correct headline
        # number. SQ_xPTS remains available in the boxscore expander as a
        # separate diagnostic for shot-selection quality, independent of who
        # took the shot - a different question from "who should've won."
        rectangles.append({
            "home_team": f"{home_team}",
            "away_team": f"{away_team}",
            "score": f"{round(home_team_boxscore['PTS'].sum())} - {round(away_team_boxscore['PTS'].sum())}",
            "x_score": f"{round(home_team_boxscore['SK_xPTS'].sum(), 1)} - {round(away_team_boxscore['SK_xPTS'].sum(), 1)}",
            "Home_boxscore": home_team_boxscore,
            "Away_boxscore": away_team_boxscore
        })

    rectangles_df = pd.DataFrame(rectangles)

    for _, game in rectangles_df.iterrows():
        st.markdown("---")
        with st.container():
            col1, col2, col3 = st.columns([1, 4, 1])
            with col1:
                st.image(team_logos[game['home_team']], use_container_width=True)
            with col3:
                st.image(team_logos[game['away_team']], use_container_width=True)
            with col2:
                st.markdown(
                    f"""
                    <div style='text-align: center; font-size: 110px;'>
                        {game["score"]}
                    </div>
                    <div style='text-align: center; color: gray; font-size: 40px;'>
                        {game["x_score"]} (expected, by known ability)
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            with st.expander(f"{game['home_team']} @ {game['away_team']} Boxscore "):
                with st.container():
                    c1, c2 = st.columns([2, 2])
                    with c1:
                        st.write(game["Home_boxscore"].sort_values(
                            by=['PTS', 'SK_xPTS'], ascending=False).reset_index(drop=True))
                    with c2:
                        st.write(game["Away_boxscore"].sort_values(
                            by=['PTS', 'SK_xPTS'], ascending=False).reset_index(drop=True))

except Exception as e:
    st.write(f"An error occurred: {e}")
    st.write(f"No games on {selected_date}")
