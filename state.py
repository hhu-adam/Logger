import pandas
import threading

data_lock = threading.Lock()

class State:
    def __init__(self) -> None:
        self.daily_game_user_log: pandas.DataFrame = pandas.DataFrame({"anon-ip": [], 
                                                                "game": [], 
                                                                "n": []})

        self.daily_hardware_user_log: pandas.DataFrame = pandas.DataFrame({"Timestamp": [], 
                                                                "Max_usr": [], 
                                                                "Max_cpu": [],
                                                                "Max_mem": []})

        self.sec_by_sec_user_count: pandas.DataFrame = pandas.DataFrame({"Timestamp": [],
                                                                "Users": []})
        
state = State()