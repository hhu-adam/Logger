import pandas
import os
import sys

"""
Reads the game-access logs for the first time 
and groups results by Anon-IP and Game.
Values for n (number of measurement occurances) is
initialized to 1. 
Finally write aggregated results to .csv file.
"""
HOME_PAGE_GAMES = ['leanprover-community/nng4',
                   'hhu-adam/robo',
                   'djvelleman/stg4',
                   'trequetrum/lean4game-logic',
                   'jadabouhawili/knightsandknaves-lean4game']


def update_n(old_df: pandas.DataFrame, new_df: pandas.DataFrame) -> pandas.DataFrame:
    """
    Update n values by joining dataframes wrt. ip and game and
    adding up n values of respected tables.
    """
    assert list(old_df.columns) == [
        'anon-ip', 'game', 'n'], f"Columns  of doc. DataFrame must be {['anon-ip', 'game', 'n']} but were {old_df.columns}"
    assert list(new_df.columns) == [
        'anon-ip', 'game', 'n'], f"Columns  of new DataFrame must be {['anon-ip', 'game', 'n']} but were {new_df.columns}"

    # Replace NaN values with zero.
    old_df = old_df.merge(new_df, how="outer", on=[
                          'anon-ip', 'game'], suffixes=['_documented', '_measured']).fillna(0)
    # Create a new column 'n'
    old_df['n'] = old_df['n_documented'] + old_df['n_measured']
    # Drop the columns 'n_documented' and 'n_measured'
    old_df = old_df.drop(['n_documented', 'n_measured'], axis=1)
    return old_df


def standardize_to_lower_case_game(df: pandas.DataFrame) -> pandas.DataFrame:
    assert list(df.columns) == ['date', 'anon-ip', 'game',
                                'lang'], f"Columns  of DataFrame must be {['date','anon-ip', 'game', 'lang']} but were {df.columns}"
    df['game'] = df['game'].apply(lambda game: game.lower())
    return df


def filter_home_page_accesses(df: pandas.DataFrame) -> pandas.DataFrame:
    assert list(df.columns) == ['date', 'anon-ip', 'game',
                                'lang'], f"Columns  of DataFrame must be {['date','anon-ip', 'game', 'lang']} but were {df.columns}"
    unique = pandas.DataFrame(df.drop_duplicates())
    unique['count'] = unique.groupby(
        ['date', 'anon-ip', 'lang']).transform('count')
    # TODO: Check also for name of games
    home_page_access = unique[unique["count"] == 5].reset_index()
    potential_home_page_games = set(home_page_access['game'].to_list())
    home_page_games = set(HOME_PAGE_GAMES)

    # There should be only five entries for a home page access and only the games in HOME_PAGE_GAMES
    # should be access.
    if len(home_page_access) == 5 and len(list(potential_home_page_games - home_page_games)) == 0:
        # Here an "anti join" is performed.
        # 1. Perform an outer join between the homepage access and the original measurement dataframe.
        home_page_access = home_page_access.astype(object)
        outer_join = df.merge(home_page_access, how='outer', on=[
                              'date', 'anon-ip', 'game', 'lang'], indicator=True)
        # 2. Disqualify every row with indicator "both" from the outer join and remove columns added in previous step.
        anti_join = outer_join[~(outer_join._merge == 'both')].drop(
            ['_merge', 'count', 'index'], axis=1).reset_index()
        return anti_join

    return df[['date', 'anon-ip', 'game', 'lang']]


def aggregate_measurements(df: pandas.DataFrame) -> pandas.DataFrame:
    """
    Initialize measurement aggregation by aggregating by ip and game
    and adding measurement count n.
    """
    df = standardize_to_lower_case_game(df)
    df = filter_home_page_accesses(df)
    return df.groupby(
        ['anon-ip', 'game']).agg(
        n=pandas.NamedAgg(column='game', aggfunc='size')).reset_index()


def is_measurement_doc_empty(doc_measurements_path: str):
    """
    Check if there are any lines in the documented measurements.
    """
    return os.stat(doc_measurements_path).st_size == 0


def measure_access(doc_measurements_path: str, new_measurements_path) -> pandas.DataFrame:
    """
    Either write create aggregated measurements DataFrame because of empty documentation file
    or update existing measurements DataFrame by new ones.
    """

    new_df = pandas.read_csv(new_measurements_path,
                             delimiter=";", index_col=False)
    new_df = aggregate_measurements(new_df)

    if is_measurement_doc_empty(doc_measurements_path):
        return new_df.sort_values(['n', 'game'], ascending=False).reset_index(drop=True)

    doc_df = pandas.read_csv(doc_measurements_path,
                             delimiter=";", index_col=False)

    return update_n(doc_df, new_df).sort_values('n', ascending=False).reset_index(drop=True).astype({'n': 'int64'})


def update_measurements(ips_documented: str, ips_measured: str) -> None:
    # Check if access have been measured by Lean4Game
    if not os.path.exists(ips_measured):
        print('No access to games recorded.', file=sys.stderr)
        return

    print(f"Starting measuring source:  {ips_measured}")
    cur_measurement_df = measure_access(ips_documented, ips_measured)
    cur_measurement_df.to_csv(ips_documented, sep=';', index=False)
    os.remove(ips_measured)
    print(f"Cleared source: {ips_measured}")
