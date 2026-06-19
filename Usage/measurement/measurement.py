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
        self.API = os.environ.get("API")
        #self.HARDWARE_SCRIPT = os.environ.get("HARDWARE_SCRIPT")
        self.hardware_info_file = os.environ.get("HARDWARE_INFO_FILE")
        self.prom_con = PrometheusConnect(url="http://localhost:9090", disable_ssl=True)
    
    def get_max_ram_usage_over_ten_min(self) -> float:
        ram_query = """
        (1 - min_over_time(
            (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)
            [10m:]
        )) * 100
        """

        result_list = self.prom_con.custom_query(ram_query)
        assert len(result_list) == 1, f"[UsageMeter] Expected 1 result, got {len(result_list)}"
        ram_result = result_list[0]

        instance = ram_result["metric"].get("instance", "unknown")
        max_ram = float(ram_result["value"][1])
        print(f"[RAM] Instance: {instance} | Max usage: {max_ram:.2f}%")
        return max_ram
    
    def get_max_cpu_usage_over_ten_min(self) -> float:
        # 1) Count the amount of seconds each CPU is in idle-mode
        # 2) Compute with rate the idle-fraction per core, smoothed over a minute
        # 3) Average the idle-rates over all cores
        # 4) Compile minimum average idle-rate of the last then minutes
        # 5) Subtract minimum average idle-rate from 1 and multiply by 100 
        # to get maximum average usage for the last ten minutes
        cpu_query = """
        (1 - min_over_time(
            avg by(instance) (rate(node_cpu_seconds_total{mode="idle"}[1m]))
            [10m:]
        )) * 100
        """

        result_list = self.prom_con.custom_query(cpu_query)
        assert len(result_list) == 1, f"[UsageMeter] Expected 1 result, got {len(result_list)}"
        cpu_result = result_list[0]

        instance = cpu_result["metric"].get("instance", "unknown")
        max_cpu = float(cpu_result["value"][1])
        print(f"[CPU] Instance: {instance} | Max usage: {max_cpu:.2f}%")
        return max_cpu

    
    def update_measurements(self,
                            doc_measurements: pandas.DataFrame, 
                            sbs_users: pandas.DataFrame) -> pandas.DataFrame:
                
        result = self.get_measurement(sbs_users)
        print(f"[{datetime.datetime.now()}] Updated user-hardware log.")
        return pandas.concat([doc_measurements, result])

    def get_measurement(self, sbs_users):
        max_cpu = self.get_max_cpu_usage_over_ten_min()
        max_mem = self.get_max_ram_usage_over_ten_min()
        max_usr = sbs_users['Users'].max()
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


