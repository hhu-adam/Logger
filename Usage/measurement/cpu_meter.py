from prometheus_api_client.prometheus_connect import PrometheusConnect


class CpuMeter:
    def __init__(self, prometheus_connection: PrometheusConnect) -> None:
        self.cpu_idle_percentages = [0.0]*40
        self.old_avg_idle_time = 0.0
        self.prom_con = prometheus_connection

    def get_max_cpu_usage_over_last_ten_min(self) -> float:
        # 1) Count the amount of seconds each CPU is in idle-mode
        # 2) Compute with rate the idle-fraction per core, smoothed over a minute
        # 3) Average the idle-rates over all cores
        # 4) Compile minimum average idle-rate of the last then minutes
        # 5) Subtract minimum average idle-rate from 1 and multiply by 100 
        # to get maximum average usage for the last ten minutes
        #cpu_query = """
        #(1 - min_over_time(
        #    avg by(instance) (rate(node_cpu_seconds_total{mode="idle"}[1m]))
        #    [10m:]
        #)) * 100
        #"""
        
        print(f"GET MINIMUM FROM: {self.cpu_idle_percentages}")
        max_cpu = 1 - min(self.cpu_idle_percentages)
        return max_cpu

    def update_cpu_idle_percentages(self):
        average_idle_seconds_per_core_query = """
        (avg by(instance) (node_cpu_seconds_total{mode="idle"}))
        """

        result_list = self.prom_con.custom_query(average_idle_seconds_per_core_query)
        print(result_list)
        assert len(result_list) == 1, f"[UsageMeter] Expected 1 result, got {len(result_list)}"
        avg_idle_time_result = result_list[0]

        avg_idle_time = float(avg_idle_time_result["value"][1])
        print(f"AVG. IDLE TIME: {avg_idle_time}")
        avg_idle_perc = (avg_idle_time - self.old_avg_idle_time)/15
        print(f"CALCULATE AVG. IDLE PERCENTAGE: ({avg_idle_time} - {self.old_avg_idle_time})/15")
        self.cpu_idle_percentages = self.cpu_idle_percentages[:-1]
        self.cpu_idle_percentages.insert(0, avg_idle_perc)
        print(f"INSERT NEW PERCENTAGE: {self.cpu_idle_percentages}")
        assert len(self.cpu_idle_percentages) == 40, "List of CPU measuremens is not 40 anymore!"
        self.old_avg_idle_time = avg_idle_time