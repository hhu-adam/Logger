import os
import sys
from pandas import DataFrame, read_csv

def lower_game_names(df: DataFrame) -> DataFrame:
    """
    Functions converts every value of the 'game' column
    of a given Dataframe into lowercase.
    """
    assert list(df.columns) == ['country', 'game', 'n'], f"You can only postprocess Dataframes with columns {['country', 'game', 'n']} not {list(df.columns)}"
    df['game'] = df['game'].str.lower()
    return df

if __name__ == '__main__':
    log_path = sys.argv[1]
    locations = read_csv(log_path, delimiter=';', index_col=False)
    lower_df = lower_game_names(locations)
    lower_df.to_csv(log_path, sep=';', index=False)

