import requests
import pandas
import json
import datetime
import os
import sys

"""
Reads the game-access logs for the first time 
and groups results by Anon-IP and Game.
Values for n (number of measurement occurances) is
initialized to 1. 
Finally write aggregated results to .csv file.
"""

class LocationMeter():

    def __init__(self) -> None:
        self.API = os.environ.get("API")

        self.HOME_PAGE_GAMES = ['leanprover-community/nng4',
                        'hhu-adam/robo',
                        'djvelleman/stg4',
                        'trequetrum/lean4game-logic',
                        'jadabouhawili/knightsandknaves-lean4game']
        self.MEASUREMENT_COLUMNS = ['date', 'anon-ip', 'game','lang']
        self.DOCUMENTED_COLUMNS = ['anon-ip', 'game', 'n']

    def update_n(self, old_df: pandas.DataFrame, new_df: pandas.DataFrame) -> pandas.DataFrame:
        """
        Update n values by joining dataframes wrt. ip and game and
        adding up n values of respected tables.
        """
        assert list(old_df.columns) == self.DOCUMENTED_COLUMNS, f"Columns  of doc. DataFrame must be {self.DOCUMENTED_COLUMNS} but were {old_df.columns}"
        assert list(new_df.columns) == self.DOCUMENTED_COLUMNS, f"Columns  of new DataFrame must be {self.DOCUMENTED_COLUMNS} but were {new_df.columns}"
        assert list(old_df.dtypes) == list(new_df.dtypes), f"Datatypes of DataFrames have to be identical but were {list(old_df.dtypes)} and {list(new_df.dtypes)}"
        
        # Replace NaN values with zero.
        old_df = old_df.merge(new_df, how="outer", on=[
                            'anon-ip', 'game'], suffixes=['_documented', '_measured']).fillna(0)
        # Create a new column 'n'
        old_df['n'] = old_df['n_documented'] + old_df['n_measured']
        # Drop the columns 'n_documented' and 'n_measured'
        old_df = old_df.drop(['n_documented', 'n_measured'], axis=1)
        return old_df


    def standardize_to_lower_case_game(self, df: pandas.DataFrame) -> pandas.DataFrame:
        assert list(df.columns) == self.MEASUREMENT_COLUMNS, f"Columns  of DataFrame must be {self.MEASUREMENT_COLUMNS} but were {df.columns}"
        df['game'] = df['game'].apply(lambda game: game.lower())
        return df


    def filter_home_page_accesses(self, df: pandas.DataFrame) -> pandas.DataFrame:
        assert list(df.columns) == self.MEASUREMENT_COLUMNS, f"Columns  of DataFrame must be {self.MEASUREMENT_COLUMNS} but were {df.columns}"
        unique = pandas.DataFrame(df.drop_duplicates())
        unique['count'] = unique.groupby(
            ['date', 'anon-ip', 'lang']).transform('count')
        # TODO: Check also for name of games
        home_page_access = unique[unique["count"] == 5].reset_index()
        potential_home_page_games = set(home_page_access['game'].to_list())
        home_page_games = set(self.HOME_PAGE_GAMES)

        # There should be only five entries for a home page access and only the games in HOME_PAGE_GAMES
        # should be access.
        if len(home_page_access) == 5 and len(list(potential_home_page_games - home_page_games)) == 0:
            # Here an "anti join" is performed.
            # 1. Perform an outer join between the homepage access and the original measurement dataframe.
            home_page_access = home_page_access.astype(object)
            outer_join = df.merge(home_page_access, how='outer', on=self.MEASUREMENT_COLUMNS, indicator=True)
            # 2. Disqualify every row with indicator "both" from the outer join and remove columns added in previous step.
            anti_join = outer_join[~(outer_join._merge == 'both')].drop(
                ['_merge', 'count', 'index'], axis=1).reset_index()
            return anti_join

        return df[self.MEASUREMENT_COLUMNS]


    def aggregate_measurements_by_game_and_ip(self, df: pandas.DataFrame) -> pandas.DataFrame:
        """
        Initialize measurement aggregation by aggregating by ip and game
        and adding measurement count n.
        """
        df = self.standardize_to_lower_case_game(df)
        df = self.filter_home_page_accesses(df)
        return df.groupby(
            ['anon-ip', 'game']).agg(
            n=pandas.NamedAgg(column='game', aggfunc='size')).reset_index()

    def aggregate_measurements_by_ip(self, df: pandas.DataFrame) -> pandas.DataFrame:
        """
        Initialize measurement aggregation by aggregating by ip
        and adding measurement count n.
        """
        df = self.standardize_to_lower_case_game(df)
        df = self.filter_home_page_accesses(df)
        return df.groupby(['anon-ip']).size().reset_index(name="n")

    def is_measurement_doc_empty(doc_measurements_path: str):
        """
        Check if there are any lines in the documented measurements.
        """
        return os.stat(doc_measurements_path).st_size == 0


    def get_measurement(self) -> dict:
        """
        Calls API to receive information about current game
        session measurements
        """

        assert len(self.API) != 0, "API string is empty!"

        response = requests.get(self.API, timeout=2000)
        if response.status_code != 200:
            print('API call for open game sessions failed.', file=sys.stderr)
            return
        
        return response.json()

    def measure_access_sec_by_sec(self) -> pandas.DataFrame:
        """
        Measure each individual game-ip combination and sum them up as the number
        of current users in during the moment of measurement.
        """
        datatype_map = {'anon-ip': 'object','game': 'object','n': 'int64'}
        measurement = self.get_measurement()
        # adjust column name from 'anon_Ip' to 'anon-ip'
        assert 'anon_Ip' in measurement.keys(), "There is no key anon_Ip in the measurement!"
        measurement['anon-ip'] = measurement.pop('anon_Ip')
        
        # Adjust order of columns after creating DataFrame from dict
        new_df = pandas.DataFrame.from_dict(measurement)[['date', 'anon-ip', 'game', 'lang']]
        
        new_df = self.aggregate_measurements_by_game_and_ip(new_df).astype(datatype_map)

        # Sum up n column to a single value and add timestap to this measure
        count = new_df['n'].sum()
        timestamp = pandas.to_datetime('now').replace(microsecond=0)

        result = pandas.DataFrame({'Timestamp': [timestamp], 'Users': [count]})

        return result

    def gather_sec_by_sec_measurements(self, gathered_measurements: pandas.DataFrame):
        """
        Take a dataframe of second by second user counts and append a new measurement to the bottom of it.
        """

        new_measurement = self.measure_access_sec_by_sec()
        return pandas.concat([gathered_measurements, new_measurement])

    def measure_access(self, doc_df: pandas.DataFrame) -> pandas.DataFrame:
        """
        Either write create aggregated measurements DataFrame because of empty documentation file
        or update existing measurements DataFrame by new ones.
        """
        datatype_map = {'anon-ip': 'object','game': 'object','n': 'int64'}
        measurement = self.get_measurement()
        # adjust column name from 'anon_Ip' to 'anon-ip'
        assert 'anon_Ip' in measurement.keys(), "There is no key anon_Ip in the measurement!"
        measurement['anon-ip'] = measurement.pop('anon_Ip')
        
        # Adjust order of columns after creating DataFrame from dict
        new_df = pandas.DataFrame.from_dict(measurement)[['date', 'anon-ip', 'game', 'lang']]
        
        new_df = self.aggregate_measurements_by_game_and_ip(new_df).astype(datatype_map)

        if doc_df.empty: #is_measurement_doc_empty(documented_measurements):
            return new_df.sort_values(['n', 'game'], ascending=False).reset_index(drop=True).astype(datatype_map)

        #doc_df = pandas.read_csv(documented_measurements, delimiter=";", index_col=False).astype(datatype_map)

        return self.update_n(doc_df, new_df).sort_values('n', ascending=False).reset_index(drop=True).astype({'n': 'int64'})


    def update_measurements(self, documented_measurements: pandas.DataFrame) -> pandas.DataFrame:
        # print("Starting measuring open game sessions.")
        cur_measurement_df = self.measure_access(documented_measurements)
        # cur_measurement_df.to_csv(ips_documented, sep=';', index=False)
        print(f"[{datetime.datetime.now()}] Updated game session log.")
        return cur_measurement_df
