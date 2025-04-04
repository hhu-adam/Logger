import requests
import pandas
import sys
import os

"""
Generate a dataframe of n locations which are derived from
n randomly chosen rows of conncetion data.
"""


def _ip_to_country(ip: str, cache: dict[str, str]) -> str:
    """
    Translates anonymized IPs into country abbreviations.
    During script execution translated IPs are cached to minimize API-calls.
    """

    if cache.get(ip) == None:
        location = requests.get(f"https://ipinfo.io/{ip}")
        assert location.status_code == 200, "Failed to receive information from given IP-address"
        country = location.json().get('country', "UNKNOWN")
        cache[ip] = country
        return country
    else:
        return cache.get(ip)


def _sort_by_n(df: pandas.DataFrame) -> pandas.DataFrame:
    return df.sort_values(['n'], ascending=False).reset_index(drop=True)


def translate(df: pandas.DataFrame, cache: dict[str, str]) -> pandas.DataFrame:
    """
    Translate IPs in descending order of visitionations to their
    respective country abbreviations. IPs that cant not be translated due
    to API-call limitations should get the abbreviation "??". 
    """

    assert list(df.columns) == [
        'anon-ip', 'game', 'n'], "Given dataframe needs to have anon-ip, game and n as attributes!"

    rows = len(df.index)
    country_rows = 1000
    unknown_rows = abs(rows - country_rows)

    df = df.sort_values(['n'], ascending=False)
    df.loc[df.head(country_rows).index, 'anon-ip'] = df.loc[df.head(
        country_rows).index, 'anon-ip'].apply(lambda x: _ip_to_country(x, cache))

    # Only mark the last rows as unknown if the dataset is bigger than the specified
    # amount of country rows.
    if rows - country_rows > 0:
        df.loc[df.tail(unknown_rows).index, 'anon-ip'] = df.loc[df.tail(
            unknown_rows).index, 'anon-ip'].apply(lambda: "??")

    df.rename(columns={'anon-ip': 'country'}, inplace=True)

    return df


def update_n(old_df: pandas.DataFrame, new_df: pandas.DataFrame) -> pandas.DataFrame:
    assert list(old_df.columns) == [
        'country', 'game', 'n'], "Given dataframe needs to have country, game and n as attributes!"
    assert list(new_df.columns) == [
        'country', 'game', 'n'], "Given dataframe needs to have country, game and n as attributes!"

    old_df = old_df.merge(new_df, how="outer", on=['country', 'game'], suffixes=[
                          '_documented', '_measured']).fillna(0)
    old_df['n'] = old_df['n_documented'] + old_df['n_measured']
    old_df = old_df.drop(['n_documented', 'n_measured'], axis=1)

    return old_df


def write_translation_log(ip_df: pandas.DataFrame, log_path: str, cache: dict[str, str]) -> None:
    # os.stat(log_path).st_size == 0 or not os.path.exists(log_path):
    if not os.path.exists(log_path):
        df = extract_usage_statistics(ip_df, cache)
        df.to_csv(log_path, sep=';', index=False)
    else:
        t_df = pandas.read_csv(log_path, delimiter=';', index_col=False)
        updated_df = update_usage_statistics(ip_df, t_df, cache)
        updated_df.to_csv(log_path, sep=';', index=False)


def update_usage_statistics(m_df: pandas.DataFrame, t_df: pandas.DataFrame, cache: dict[str, str]) -> pandas.DataFrame:
    new_t_df = translate(m_df, cache)
    # aggregate DataFrame by country code and sum up n
    new_t_df = new_t_df.groupby(['country', 'game'])['n'].sum().reset_index()
    new_t_df = update_n(t_df, new_t_df)
    return _sort_by_n(new_t_df)


def extract_usage_statistics(m_df: pandas.DataFrame, cache: dict[str, str]) -> pandas.DataFrame:
    new_t_df = translate(m_df, cache)
    # aggregate DataFrame by country code and sum up n
    new_t_df = new_t_df.groupby(['country', 'game'])['n'].sum().reset_index()
    return _sort_by_n(new_t_df)


def create_translation(ips_documented: str, path_translated_ips: str) -> None:
    if not os.path.exists(ips_documented):
        print("No measurements available to translate!", file=sys.stderr)
        return

    cache = {}
    ip_df = pandas.read_csv(ips_documented, delimiter=';', index_col=False)
    write_translation_log(ip_df, path_translated_ips, cache)
    print("Measurements translated.")

    f = open(ips_documented, 'r+')
    f.truncate(0)
    print("Daily measurements cleared.")
