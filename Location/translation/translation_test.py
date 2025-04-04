import unittest
from unittest.mock import Mock, patch
import pandas
import os
from translation import update_usage_statistics, write_translation_log

class TestTranslation(unittest.TestCase):

    #@patch('project.services.requests.get')
    #def test_measurement_to_log_translation(self):
    #    mes_path = "/home/adam/adam-dashboard/Logging/Location/translation/tests/test_measurements.log"
    #    log_path = "/home/adam/adam-dashboard/Logging/Location/translation/tests/test_locations.log"
    #    f_l = open(log_path, "w")
    #    Mock.mokc
    #    pass

    def test_translate_and_write_to_empty_log_distinct_countries(self):
        # Create log-file
        log_path = "./test_files/test_locations.log"
        f = open(log_path, "w")

        cache = {'1': 'US', 
                 '2': 'CN', 
                 '3': 'DE'}

        new_data = {'anon-ip': ['1', '2', '3'],
                    'game': ['leanprover-community/nng4', 
                             'leanprover-community/nng4', 
                             'leanprover-community/nng4'],
                    'n': [3.0, 2.0, 1.0]}
        
        expected_data = {'country': ['US', 'CN', 'DE'],
                          'game': ['leanprover-community/nng4', 
                                   'leanprover-community/nng4', 
                                   'leanprover-community/nng4'],
                          'n': [3.0, 2.0, 1.0]}

        new_df = pandas.DataFrame(new_data)
        expected_df = pandas.DataFrame(expected_data)

        # Write to log-file
        write_translation_log(new_df, log_path, cache)
        logged_df = pandas.read_csv(log_path, delimiter=';', index_col=False)
        
        # Delete log-file
        f.close()
        os.remove(log_path)

        self.assertTrue(expected_df.equals(logged_df))

    def test_translate_and_write_to_empty_log_repeating_countries(self):
        # Create log-file
        log_path = "./test_files/test_locations.log"
        f = open(log_path, "w")

        cache = {'1': 'US', 
                 '2': 'CN', 
                 '3': 'DE'}

        new_data = {'anon-ip': ['1', '1', '3'],
                    'game': ['leanprover-community/nng4', 
                             'leanprover-community/nng4', 
                             'leanprover-community/nng4'],
                    'n': [3.0, 2.0, 1.0]}
        
        expected_data = {'country': ['US', 'DE'],
                          'game': ['leanprover-community/nng4', 
                                   'leanprover-community/nng4'],
                          'n': [5.0, 1.0]}

        new_df = pandas.DataFrame(new_data)
        expected_df = pandas.DataFrame(expected_data)

        # Write to log-file
        write_translation_log(new_df, log_path, cache)
        logged_df = pandas.read_csv(log_path, delimiter=';', index_col=False)
        
        # Delete log-file
        f.close()
        os.remove(log_path)

        self.assertTrue(expected_df.equals(logged_df))
    
    def test_n_update_with_distinct_countries(self):
        cache = {'1': 'US', 
                 '2': 'CN', 
                 '3': 'DE'}

        old_data = {'country': ['US', 'CN', 'DE'],
                    'game': ['leanprover-community/nng4', 
                             'leanprover-community/nng4', 
                             'leanprover-community/nng4'],
                    'n': [14.0, 13.0, 12.0]}
        
        new_data = {'anon-ip': ['1', '2', '3'],
                    'game': ['leanprover-community/nng4', 
                             'leanprover-community/nng4', 
                             'leanprover-community/nng4'],
                    'n': [3.0, 2.0, 1.0]}
        
        expected_data = {'country': ['US', 'CN', 'DE'],
                    'game': ['leanprover-community/nng4', 
                             'leanprover-community/nng4', 
                             'leanprover-community/nng4'],
                    'n': [17.0, 15.0, 13.0]}
        
        old_df = pandas.DataFrame(old_data)
        new_df = pandas.DataFrame(new_data)
        expected_df = pandas.DataFrame(expected_data)
        
        updated_df = update_usage_statistics(new_df, old_df, cache)

        self.assertTrue(expected_df.equals(updated_df))

    def test_n_update_with_repeating_countries(self):
        cache = {'1': 'US', 
                 '2': 'CN', 
                 '3': 'DE'}

        old_data = {'country': ['US', 'CN', 'DE'],
                    'game': ['leanprover-community/nng4', 
                             'leanprover-community/nng4', 
                             'leanprover-community/nng4'],
                    'n': [14.0, 13.0, 12.0]}
        
        new_data = {'anon-ip': ['1', '1', '3'],
                    'game': ['leanprover-community/nng4', 
                             'leanprover-community/nng4', 
                             'leanprover-community/nng4'],
                    'n': [3.0, 2.0, 1.0]}
        
        expected_data = {'country': ['US', 'CN', 'DE'],
                    'game': ['leanprover-community/nng4',
                             'leanprover-community/nng4', 
                             'leanprover-community/nng4'],
                    'n': [19.0, 13.0, 13.0]}
        
        old_df = pandas.DataFrame(old_data)
        new_df = pandas.DataFrame(new_data)
        expected_df = pandas.DataFrame(expected_data)
        
        updated_df = update_usage_statistics(new_df, old_df, cache)

        self.assertTrue(expected_df.equals(updated_df))


if __name__ == '__main__':
    unittest.main()