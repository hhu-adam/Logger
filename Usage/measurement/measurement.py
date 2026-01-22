import datetime
import os
import pandas
import subprocess

from io import StringIO


"""
Reads the game-access logs for the first time 
and groups results by Anon-IP and Game.
Values for n (number of measurement occurances) is
initialized to 1. 
Finally write aggregated results to .csv file.
"""

class UsageMeter:
    def __init__(self) -> None:
        self.API = os.environ.get("API")
        self.HARDWARE_SCRIPT = os.environ.get("HARDWARE_SCRIPT")
        self.HOME_PAGE_GAMES = ['leanprover-community/nng4',
                        'hhu-adam/robo',
                        'djvelleman/stg4',
                        'trequetrum/lean4game-logic',
                        'jadabouhawili/knightsandknaves-lean4game']
        self.MEASUREMENT_COLUMNS = ['date', 'anon-ip', 'game','lang']
        self.HW_COLUMNS = ['Timestamp', 'CPU', 'MEM']
        self.DOCUMENTED_COLUMNS = ['timestamp', 'num_useres', 'cpu', 'ram']

    def get_measurement(self) -> dict:
        """
        Call hardware usage script and save values to dict.
        """
        assert len(self.HARDWARE_SCRIPT) != 0, "Please set environment variable for the hardware script"
        usage: str = subprocess.check_output(['sh', self.HARDWARE_SCRIPT]).decode('UTF-8')
        usage_measurement: pandas.DataFrame = pandas.read_csv(StringIO(usage), sep=',')
        usage_measurement.insert(0, 'Timestamp', pandas.to_datetime('now').replace(microsecond=0))
        # Remove leading space from MEM column name
        usage_measurement.columns = usage_measurement.columns.str.lstrip()
        return usage_measurement

    def measure_hardware(self, gathered_measurements: pandas.DataFrame):
        """
        Take a dataframe of second by second hardware measurements and append a new meausre
        meant to the bottom of it.
        """
        assert list(gathered_measurements.columns) == self.HW_COLUMNS, f"Columns of DataFrame must be {self.HW_COLUMNS} but were {gathered_measurements.columns}"
        new_measurement = self.get_measurement()
        return pandas.concat([gathered_measurements, new_measurement])
    
    def update_measurements(self,
                            doc_measurements: pandas.DataFrame, 
                            sbs_hardware: pandas.DataFrame, 
                            sbs_users: pandas.DataFrame) -> pandas.DataFrame:
                
        max_cpu = sbs_hardware['CPU'].max()
        max_mem = sbs_hardware['MEM'].max()
        max_usr = sbs_users['Users'].max()
        timestamp = self.get_timestamp_now()

        result = pandas.DataFrame({'Timestamp': [timestamp],
                                   'Max_usr': [max_usr],
                                   'Max_cpu': [max_cpu],
                                   'Max_mem': [max_mem]})
        
        result = self.apply_measurement_dtypes(result)

        print(f"[{datetime.datetime.now()}] Updated user-hardware log.")

        return pandas.concat([doc_measurements, result])
    
    def apply_measurement_dtypes(self, dataframe: pandas.DataFrame):
        datatype_map = {'Timestamp': 'string',
                        'Max_usr': 'float64',
                        'Max_cpu': 'float64',
                        'Max_mem': 'float64'}
        
        return dataframe.astype(datatype_map)


    def get_timestamp_now(self) -> str:
        return pandas.to_datetime('now').strftime("%y-%m-%d %H:%M:%S")
    
    def add_timestamp(self,dataframe: pandas.DataFrame):
        dataframe.insert(0, 'Timestamp', self.get_timestamp_now)
        return dataframe


