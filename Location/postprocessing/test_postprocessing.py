from postprocessing import lower_game_names
import unittest
import pandas
import os

# Same as in LocationService
def to_cross_table(df: pandas.DataFrame) -> pandas.DataFrame:
    assert list(df.columns) == ['country', 'game', 'n'], f"You can only convert Dataframes with columns {['country', 'game', 'n']} not {list(df.columns)}"
    countries = df['country']
    split_column = pandas.DataFrame(df['game'].str.split("/", expand=True))
    users, games = split_column[0], split_column[1]
    ten_min_blocks = df['n']

    ctab = pandas.crosstab(index=countries, columns=[users, games], values=ten_min_blocks, margins=True, aggfunc='sum')
    to_hours = lambda x: round(x*(1/6), 2)
    hctab = ctab.map(to_hours)
    return hctab


CWD = os.getcwd()

class TestPostProcessing(unittest.TestCase):

    def test_that_case_duplicates_from_csv_are_lowered(self):
        locations = pandas.read_csv(f"{CWD}/test_files/test_locations.log", delimiter=';', index_col=False)
        exp_df = pandas.read_csv(f"{CWD}/test_files/exp_locations.log", delimiter=';', index_col=False)
        
        res_df = lower_game_names(locations)

        self.assertTrue(exp_df.equals(res_df))
    

    def test_recorded_time_does_not_change(self):        
        exp_path = f"{CWD}/test_files/exp_locations.log"
        exp_df = pandas.read_csv(exp_path, delimiter=';', index_col=False)
        exp_ct = to_cross_table(exp_df)
        
        test_path = f"{CWD}/test_files/test_locations.log"
        test_df = pandas.read_csv(test_path, delimiter=';', index_col=False)
        res_df = lower_game_names(test_df)
        res_ct = to_cross_table(res_df)

        self.assertTrue(exp_ct.equals(res_ct))


if __name__ == "__main__":
    unittest.main()