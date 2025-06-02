import json
from unittest import TestCase, main
from unittest.mock import patch
import pandas
from measurement import aggregate_measurements, update_n, measure_access


class TestMeasurement(TestCase):

    def __init__(self, methodName="runTest"):
        super().__init__(methodName)
        self.doc_df = pandas.read_csv("./test_files/doc_meas.csv", delimiter=';', index_col=False)
        self.empty_doc_df = pandas.DataFrame({"anon-ip": [], "game": [], "n": []})
        self.new_measurement = "./test_files/new_meas.csv"
        self.new_measurement_with_home_page_access_non_shuffled = "./test_files/new_meas_with_home_page_access_non_shuffled.csv"
        self.new_measurement_with_home_page_access_shuffled = "./test_files/new_meas_with_home_page_access_shuffled.csv"
        self.expected_updated_documentation = "./test_files/exp_doc_new.csv"
        self.expected_updated_documentation_without_home_page_access = "./test_files/exp_doc_new_with_home_page_access_non_shuffled.csv"
        self.expected_initialized_documentation = "./test_files/exp_empty_doc_new.csv"
        self.expected_initialized_documentation_without_home_page_access = "./test_files/exp_empty_doc_new_with_home_page_access_non_shuffled.csv"

    @patch('measurement.get_measurement')
    def test_measuring_access_with_empty_documented_measurements(self, mock_get_measurement):
        exp_df = pandas.read_csv(
            self.expected_initialized_documentation, delimiter=';', index_col=False)
        
        mock_data = json.loads(pandas.read_csv(self.new_measurement, delimiter=';').to_json())
        mock_get_measurement.return_value = mock_data

        res_df = measure_access(self.empty_doc_df)
        self.assertTrue(exp_df.equals(res_df))

    @patch('measurement.get_measurement')
    def test_measuring_access_with_empty_documented_measurements_with_home_page_access_non_shuffled(self, mock_get_measurement):
        exp_df = pandas.read_csv(
            self.expected_initialized_documentation_without_home_page_access, delimiter=';', index_col=False)
        
        mock_data = json.loads(pandas.read_csv(self.new_measurement_with_home_page_access_non_shuffled, delimiter=';').to_json())
        mock_get_measurement.return_value = mock_data

        res_df = measure_access(self.empty_doc_df)
        self.assertTrue(exp_df.equals(res_df))

    @patch('measurement.get_measurement')
    def test_measuring_access_with_empty_documented_measurements_with_home_page_access_shuffled(self, mock_get_measurement):
        exp_df = pandas.read_csv(
            self.expected_initialized_documentation_without_home_page_access, delimiter=';', index_col=False)
        
        mock_data = json.loads(pandas.read_csv(self.new_measurement_with_home_page_access_shuffled, delimiter=';').to_json())
        mock_get_measurement.return_value = mock_data

        res_df = measure_access(self.empty_doc_df)
        self.assertTrue(exp_df.equals(res_df))

    @patch('measurement.get_measurement')
    def test_updating_already_existing_documentation(self, mock_get_measurement):
        exp_df = pandas.read_csv(
            self.expected_updated_documentation, delimiter=';', index_col=False)
        
        mock_data = json.loads(pandas.read_csv(self.new_measurement, delimiter=';').to_json())
        mock_get_measurement.return_value = mock_data

        res_df = measure_access(self.doc_df)
        self.assertTrue(exp_df.equals(res_df))

    @patch('measurement.get_measurement')
    def test_updating_already_existing_documentation_with_home_page_access_shuffled(self, mock_get_measurement):
        exp_df = pandas.read_csv(
            self.expected_updated_documentation_without_home_page_access, delimiter=';', index_col=False)
        
        mock_data = json.loads(pandas.read_csv(self.new_measurement_with_home_page_access_shuffled, delimiter=';').to_json())
        mock_get_measurement.return_value = mock_data

        res_df = measure_access(self.doc_df)
        self.assertTrue(exp_df.equals(res_df))

    @patch('measurement.get_measurement')
    def test_updating_already_existing_documentation_with_home_page_access_non_shuffled(self, mock_get_measurement):
        exp_df = pandas.read_csv(
            self.expected_updated_documentation_without_home_page_access, delimiter=';', index_col=False)
        
        mock_data = json.loads(pandas.read_csv(self.new_measurement_with_home_page_access_non_shuffled, delimiter=';').to_json())
        mock_get_measurement.return_value = mock_data

        res_df = measure_access(self.doc_df)
        self.assertTrue(exp_df.equals(res_df))

    def test_with_two_players_accessing_two_games(self):
        old_data = {'anon-ip': ['001.002.111.0',
                                '007.007.111.0'],
                    'game': ['mz1',
                             'mz2'],
                    'n': [2,
                          1]}

        new_data = {'date': ['date1',
                             'date2',
                             'date3',
                             'date4'],
                    'anon-ip': ['001.002.111.0',
                                '001.002.111.0',
                                '007.007.111.0',
                                '007.007.111.0'],
                    'game': ['mz1',
                             'mz1',
                             'mz2',
                             'mz2'],
                    'lang': ['en',
                             'zh',
                             'de',
                             'nl']}

        exp_data = {'anon-ip': ['001.002.111.0',
                                '007.007.111.0'],
                    'game': ['mz1',
                             'mz2'],
                    'n': [4, 3]}

        doc_df = pandas.DataFrame(old_data)
        new_df = aggregate_measurements(pandas.DataFrame(new_data))
        res_df = update_n(doc_df, new_df)
        exp_df = pandas.DataFrame(exp_data)

        self.assertTrue(exp_df.equals(res_df))

    def test_with_two_players_accessing_two_games_lower_and_upper_case(self):
        old_data = {'anon-ip': ['001.002.111.0',
                                '007.007.111.0'],
                    'game': ['mz1',
                             'mz2'],
                    'n': [2,
                          1]}

        new_data = {'date': ['date1',
                             'date2',
                             'date3',
                             'date4'],
                    'anon-ip': ['001.002.111.0',
                                '001.002.111.0',
                                '007.007.111.0',
                                '007.007.111.0'],
                    'game': ['MZ1',
                             'mz1',
                             'mz2',
                             'MZ2'],
                    'lang': ['en',
                             'zh',
                             'de',
                             'nl']}

        exp_data = {'anon-ip': ['001.002.111.0',
                                '007.007.111.0'],
                    'game': ['mz1',
                             'mz2'],
                    'n': [4, 3]}

        doc_df = pandas.DataFrame(old_data)
        new_df = aggregate_measurements(pandas.DataFrame(new_data))
        res_df = update_n(doc_df, new_df)
        exp_df = pandas.DataFrame(exp_data)

        self.assertTrue(exp_df.equals(res_df))

    def test_count_for_strictly_distinct_ip_game_pairs_is_1_for_each_pair(self):

        new_data = {'date': ['Day Mon XY 20ZZ 0:0:0 GMT+0000 (Coordinated Universal Time)',
                             'Day Mon XY 20ZZ 0:0:0 GMT+0000 (Coordinated Universal Time)',
                             'Day Mon XY 20ZZ 0:0:0 GMT+0000 (Coordinated Universal Time)'],
                    'anon-ip': ['1',
                                '2',
                                '3'],
                    'game': ['leanprover-community/nng4',
                             'leanprover-community/nng4',
                             'leanprover-community/nng4'],
                    'lang': ['en', 'en', 'zh']}

        expected_aggregation = {'anon-ip': ['1',
                                            '2',
                                            '3'],
                                'game': ['leanprover-community/nng4',
                                         'leanprover-community/nng4',
                                         'leanprover-community/nng4'],
                                'n': [1, 1, 1]}

        new_df = pandas.DataFrame(new_data)
        agg_df = aggregate_measurements(new_df)

        expected_df = pandas.DataFrame(expected_aggregation)
        self.assertTrue(expected_df.equals(agg_df))

    def test_count_for_n_equal_ip_game_pairs_is_n_for_each_pair(self):

        new_data = {'date': ['Day Mon XY 20ZZ 0:0:0 GMT+0000 (Coordinated Universal Time)',
                             'Day Mon XY 20ZZ 0:0:0 GMT+0000 (Coordinated Universal Time)',
                             'Day Mon XY 20ZZ 0:0:0 GMT+0000 (Coordinated Universal Time)'],
                    'anon-ip': ['1',
                                '1',
                                '3'],
                    'game': ['leanprover-community/nng4',
                             'leanprover-community/nng4',
                             'leanprover-community/nng4'],
                    'lang': ['en', 'en', 'zh']}

        expected_aggregation = {'anon-ip': ['1',
                                            '3'],
                                'game': ['leanprover-community/nng4',
                                         'leanprover-community/nng4'],
                                'n': [2, 1]}

        new_df = pandas.DataFrame(new_data)
        agg_df = aggregate_measurements(new_df)

        expected_df = pandas.DataFrame(expected_aggregation)
        self.assertTrue(expected_df.equals(agg_df))

    def test_update_of_two_strictly_distinct_measurements_should_increment_each_n_by_1(self):

        old_data = {'anon-ip': ['1',
                                '2',
                                '3'],
                    'game': ['leanprover-community/nng4',
                             'leanprover-community/nng4',
                             'leanprover-community/nng4'],
                    'n': [1,
                          1,
                          1]}

        new_data = {'anon-ip': ['1',
                                '2',
                                '3'],
                    'game': ['leanprover-community/nng4',
                             'leanprover-community/nng4',
                             'leanprover-community/nng4'],
                    'n': [1,
                          1,
                          1]}

        exp_data = {'anon-ip': ['1',
                                '2',
                                '3'],
                    'game': ['leanprover-community/nng4',
                             'leanprover-community/nng4',
                             'leanprover-community/nng4'],
                    'n': [2, 2, 2]}

        new_df = pandas.DataFrame(new_data)
        old_df = pandas.DataFrame(old_data)
        exp_df = pandas.DataFrame(exp_data)

        updated_df = update_n(old_df, new_df)

        self.assertTrue(exp_df.equals(updated_df))

    def test_update_with_two_measurements_should_update_each_recurring_IP_game_pair_by_the_sum_of_both_measurements(self):

        old_data = {'anon-ip': ['1',
                                '2',
                                '3'],
                    'game': ['leanprover-community/nng4',
                             'leanprover-community/nng4',
                             'leanprover-community/nng4'],
                    'n': [3,
                          5,
                          7]}

        new_data = {'anon-ip': ['1',
                                '2',
                                '3'],
                    'game': ['leanprover-community/nng4',
                             'leanprover-community/nng4',
                             'leanprover-community/nng4'],
                    'n': [7,
                          5,
                          3]}

        exp_data = {'anon-ip': ['1',
                                '2',
                                '3'],
                    'game': ['leanprover-community/nng4',
                             'leanprover-community/nng4',
                             'leanprover-community/nng4'],
                    'n': [10, 10, 10]}

        new_df = pandas.DataFrame(new_data)
        old_df = pandas.DataFrame(old_data)
        exp_df = pandas.DataFrame(exp_data)

        updated_df = update_n(old_df, new_df)

        self.assertTrue(exp_df.equals(updated_df))

    def test_that_access_at_the_same_time_to_games_on_home_page_are_filtered_out_with_only_one_time_stamp(self):
        access_to_home_page = {'date': ['Day Mon XY 20ZZ 12:13:0 GMT+0000 (Coordinated Universal Time)',
                                        'Day Mon XY 20ZZ 12:13:0 GMT+0000 (Coordinated Universal Time)',
                                        'Day Mon XY 20ZZ 12:13:0 GMT+0000 (Coordinated Universal Time)',
                                        'Day Mon XY 20ZZ 12:13:0 GMT+0000 (Coordinated Universal Time)',
                                        'Day Mon XY 20ZZ 12:13:0 GMT+0000 (Coordinated Universal Time)'],
                               'anon-ip': ['1', '1', '1', '1', '1'],
                               'game': ['leanprover-community/nng4',
                                        'hhu-adam/robo',
                                        'djvelleman/stg4',
                                        'trequetrum/lean4game-logic',
                                        'jadabouhawili/knightsandknaves-lean4game'],
                               'lang': ['en', 'en', 'en', 'en', 'en']}

        expected_aggregation = {'anon-ip': [],
                                'game': [],
                                'n': []}

        new_df = pandas.DataFrame(access_to_home_page)
        exp_df = pandas.DataFrame(expected_aggregation).astype(
            dtype={'anon-ip': 'object', 'game': 'object', 'n': 'int64'})

        agg_df = aggregate_measurements(new_df)

        self.assertTrue(exp_df.equals(agg_df))

    def test_that_access_at_the_same_time_to_games_on_home_page_are_filtered_out_in_data_with_different_timestamps_non_shuffled(self):
        access_to_home_page = {'date': ['Day Thu XY 20ZZ 16:13:0 GMT+1000 (Coordinated Universal Time)',
                                        'Day Tue XY 20ZZ 02:13:0 GMT+0000 (Coordinated Universal Time)',
                                        'Day Mon XY 20ZZ 12:13:0 GMT+0000 (Coordinated Universal Time)',
                                        'Day Mon XY 20ZZ 12:13:0 GMT+0000 (Coordinated Universal Time)',
                                        'Day Mon XY 20ZZ 12:13:0 GMT+0000 (Coordinated Universal Time)',
                                        'Day Mon XY 20ZZ 12:13:0 GMT+0000 (Coordinated Universal Time)',
                                        'Day Mon XY 20ZZ 12:13:0 GMT+0000 (Coordinated Universal Time)',
                                        'Day Fri XY 20ZZ 14:13:0 GMT+0000 (Coordinated Universal Time)'],
                               'anon-ip': ['1', '2', '1', '1', '1', '1', '1', '3'],
                               'game': ['hhu-adam/robo',
                                        'djvelleman/stg4',
                                        'leanprover-community/nng4',
                                        'hhu-adam/robo',
                                        'djvelleman/stg4',
                                        'trequetrum/lean4game-logic',
                                        'jadabouhawili/knightsandknaves-lean4game',
                                        'trequetrum/lean4game-logic'],
                               'lang': ['de', 'de', 'en', 'en', 'en', 'en', 'en', 'de']}

        expected_aggregation = {'anon-ip': ['1', '2', '3'],
                                'game': ['hhu-adam/robo',
                                         'djvelleman/stg4',
                                         'trequetrum/lean4game-logic'],
                                'n': [1, 1, 1]}

        new_df = pandas.DataFrame(access_to_home_page)
        exp_df = pandas.DataFrame(expected_aggregation).astype(
            dtype={'anon-ip': 'object', 'game': 'object', 'n': 'int64'})

        agg_df = aggregate_measurements(new_df)

        self.assertTrue(exp_df.equals(agg_df))

    def test_that_access_at_the_same_time_to_games_on_home_page_are_filtered_out_in_data_with_different_timestamps_shuffled(self):
        access_to_home_page = {'date': ['Day Thu XY 20ZZ 16:13:0 GMT+1000 (Coordinated Universal Time)',
                                        'Day Mon XY 20ZZ 12:13:0 GMT+0000 (Coordinated Universal Time)',
                                        'Day Mon XY 20ZZ 12:13:0 GMT+0000 (Coordinated Universal Time)',
                                        'Day Mon XY 20ZZ 12:13:0 GMT+0000 (Coordinated Universal Time)',
                                        'Day Tue XY 20ZZ 02:13:0 GMT+0000 (Coordinated Universal Time)',
                                        'Day Mon XY 20ZZ 12:13:0 GMT+0000 (Coordinated Universal Time)',
                                        'Day Fri XY 20ZZ 14:13:0 GMT+0000 (Coordinated Universal Time)',
                                        'Day Mon XY 20ZZ 12:13:0 GMT+0000 (Coordinated Universal Time)',],
                               'anon-ip': ['2',
                                           '1',
                                           '1',
                                           '1',
                                           '3',
                                           '1',
                                           '4',
                                           '1'],
                               'game': ['hhu-adam/robo',
                                        'leanprover-community/nng4',
                                        'hhu-adam/robo',
                                        'djvelleman/stg4',
                                        'djvelleman/stg4',
                                        'trequetrum/lean4game-logic',
                                        'trequetrum/lean4game-logic',
                                        'jadabouhawili/knightsandknaves-lean4game'],
                               'lang': ['de', 'en', 'en', 'en', 'en', 'en', 'de', 'en']}

        expected_aggregation = {'anon-ip': ['2', '3', '4'],
                                'game': ['hhu-adam/robo',
                                         'djvelleman/stg4',
                                         'trequetrum/lean4game-logic'],
                                'n': [1, 1, 1]}

        new_df = pandas.DataFrame(access_to_home_page)
        exp_df = pandas.DataFrame(expected_aggregation).astype(
            dtype={'anon-ip': 'object', 'game': 'object', 'n': 'int64'})

        agg_df = aggregate_measurements(new_df)

        self.assertTrue(exp_df.equals(agg_df))


if __name__ == '__main__':
    main()
