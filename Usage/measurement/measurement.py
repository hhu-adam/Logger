"""
Reads the game-access logs for the first time 
and groups results by Anon-IP and Game.
Values for n (number of measurement occurances) is
initialized to 1. 
Finally write aggregated results to .csv file.
"""

import datetime
import os
import pandas
import subprocess
from prometheus_api_client.prometheus_connect import PrometheusConnect
import requests

from io import StringIO

from Location.measurement.measurement import LocationMeter
from Usage.measurement.cpu_meter import CpuMeter
from Usage.measurement.ram_meter import RamMeter

HOME_PAGE_GAMES = ['leanprover-community/nng4',
                   'hhu-adam/robo',
                   'djvelleman/stg4',
                   'trequetrum/lean4game-logic',
                   'jadabouhawili/knightsandknaves-lean4game']

#MEASUREMENT_COLUMNS = ['date', 
#                       'anon-ip', 
#                       'game',
#                       'lang']

#HW_COLUMNS = ['Timestamp', 
#              'CPU', 
#              'MEM']


class UsageMeter:
    def __init__(self) -> None:
        # self.API = os.environ.get("API")
        #self.HARDWARE_SCRIPT = os.environ.get("HARDWARE_SCRIPT")
        self.hardware_info_file = os.environ.get("HARDWARE_INFO_FILE")
        self.prom_con = PrometheusConnect(url="http://localhost:9090", disable_ssl=True)
        # Zero-initialize list of 40 15-sec idle measurements
        # this list corresponds to all idle percentages over the 
        # last ten minutes.
        #self.cpu_idle_percentages = [0.0]*40
        #self.old_avg_idle_time = 0.0
        self.cpu_meter = CpuMeter(prometheus_connection=self.prom_con)
        self.ram_meter = RamMeter(prometheus_connection=self.prom_con)
        self.loc_meter = LocationMeter()
            
    def update_measurements(self,
                            doc_measurements: pandas.DataFrame) -> pandas.DataFrame:
                
        result = self.get_measurement()
        print(f"[{datetime.datetime.now()}] Updated user-hardware log.")
        return pandas.concat([doc_measurements, result])

    def update_cpu_measurement(self):
        self.cpu_meter.update_cpu_idle_percentages()

    def get_measurement(self):
        max_cpu = self.cpu_meter.get_max_cpu_usage_over_last_ten_min()
        print(f"[CPU] Max cpu: {max_cpu}")
        max_mem = self.ram_meter.get_max_ram_usage_over_ten_min()
        print(f"[RAM] Max ram: {max_mem:.2f}%")
        # max_usr = sbs_users['Users'].max()
        max_usr = self.loc_meter.get_max_users_over_ten_minutes()
        print(f"[USERS] Max users: {max_usr}")
        timestamp = self.get_timestamp_now()
        print(f"Timestamp: {timestamp}")

        result = pandas.DataFrame({'Timestamp': [timestamp],
                                   'Max_usr': [max_usr],
                                   'Max_cpu': [max_cpu],
                                   'Max_mem': [max_mem]})
        
        result = self.apply_measurement_dtypes(result)
        return result
    
    
    def apply_measurement_dtypes(self, dataframe: pandas.DataFrame):
        datatype_map = {'Timestamp': 'string',
                        'Max_usr': 'float64',
                        'Max_cpu': 'float64',
                        'Max_mem': 'float64'}
        
        return dataframe.astype(datatype_map)


    def get_timestamp_now(self) -> str:
        return pandas.to_datetime('now').strftime("%y-%m-%d %H:%M:%S")
    

    def add_timestamp(self,dataframe: pandas.DataFrame):
        dataframe.insert(0, 'Timestamp', self.get_timestamp_now())
        return dataframe


