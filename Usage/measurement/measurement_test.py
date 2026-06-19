from unittest import TestCase, main
from unittest.mock import patch
import pandas
from measurement import UsageMeter


class TestMeasurement(TestCase):

    def __init__(self, methodName="runTest"):
        super().__init__(methodName)
        self.um = UsageMeter()

        #self.doc_df = pandas.read_csv("./test_files/doc_meas.csv", delimiter=';', index_col=False)
        self.expected_empty_documentation = "Usage/measurement/test_files/empty_hardware_measurement.csv"
        self.new_hardware_measurement = "Usage/measurement/test_files/new_hardware_measurement.csv"
        self.new_user_measurement = "test_files/new_user_measurement.csv"

        self.exp_ft_combined_measurement = "test_files/expected_first_time_combined_measurement.csv"

        self.empty_doc_df = pandas.DataFrame({"Timestamp": [], 
                                              "CPU": [], 
                                              "MEM": []})
        
        #self.new_measurement_with_home_page_access_non_shuffled = "./test_files/new_meas_with_home_page_access_non_shuffled.csv"
        #self.new_measurement_with_home_page_access_shuffled = "./test_files/new_meas_with_home_page_access_shuffled.csv"
        #self.expected_updated_documentation = "./test_files/exp_doc_new.csv"
        #self.expected_updated_documentation_without_home_page_access = "./test_files/exp_doc_new_with_home_page_access_non_shuffled.csv"

        #self.expected_initialized_documentation_without_home_page_access = "./test_files/exp_empty_doc_new_with_home_page_access_non_shuffled.csv"

    
    @patch('measurement.UsageMeter.get_max_cpu_usage_over_ten_min')
    @patch('measurement.UsageMeter.get_max_ram_usage_over_ten_min')
    @patch('measurement.UsageMeter.get_timestamp_now')
    def test_first_time_updating_combined_user_and_hardware_measurements(self, 
                                                                         mock_get_timestamp_now,
                                                                         mock_get_max_ram_usage_over_ten_min,
                                                                         mock_get_max_cpu_usage_over_ten_min):
        exp_df = pandas.read_csv(self.exp_ft_combined_measurement)
        exp_df = self.um.apply_measurement_dtypes(exp_df)
        sbs_users = pandas.read_csv(self.new_user_measurement)

        mock_get_max_cpu_usage_over_ten_min.return_value = 0.98924
        mock_get_max_ram_usage_over_ten_min.return_value = 0.82792
        mock_get_timestamp_now.return_value = pandas.Timestamp(year=2026, month=1, day=22, hour=12, minute=10)

        daily_hardware_user_log: pandas.DataFrame = pandas.DataFrame({
            "Timestamp": [], 
            "Max_usr": [], 
            "Max_cpu": [],
            "Max_mem": []})
        
        res_df = self.um.update_measurements(daily_hardware_user_log,
                                             sbs_users)

        self.assertTrue(exp_df.equals(res_df))
    
        
if __name__ == '__main__':
    main()
