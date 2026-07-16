from prometheus_api_client.prometheus_connect import PrometheusConnect

class RamMeter:
    def __init__(self, prometheus_connection: PrometheusConnect) -> None:
        self.prom_con = prometheus_connection

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
        #print(f"[RAM] Instance: {instance} | Max usage: {max_ram:.2f}%")
        return max_ram