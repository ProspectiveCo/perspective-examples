from generators import StreamGenerator, generator_registry
import pandas as pd
import random
from datetime import datetime, timedelta


POWER_STATIONS: list[dict] = [
    {"name": "Times Sq-42 St", "lat": 40.755290, "lon": -73.987495},
    {"name": "Grand Central-42 St", "lat": 40.752726, "lon": -73.977229},
    {"name": "34 St-Herald Sq", "lat": 40.749567, "lon": -73.987950},
    {"name": "14 St-Union Sq", "lat": 40.735736, "lon": -73.990568},
    {"name": "34 St-Penn Station", "lat": 40.750373, "lon": -73.991057},
    {"name": "Fulton St", "lat": 40.710374, "lon": -74.007582},
    {"name": "59 St-Columbus Circle", "lat": 40.768296, "lon": -73.981736},
    {"name": "Lexington Av/59 St", "lat": 40.762526, "lon": -73.967967},
    {"name": "Lexington Av/53 St", "lat": 40.757552, "lon": -73.969055},
    {"name": "42 St-Bryant Pk", "lat": 40.754222, "lon": -73.984569},
    {"name": "47-50 Sts-Rockefeller Ctr", "lat": 40.758663, "lon": -73.981329},
    {"name": "86 St", "lat": 40.785672, "lon": -73.957519},
    {"name": "72 St", "lat": 40.778453, "lon": -73.981970},
    {"name": "Lexington Av/63 St", "lat": 40.764629, "lon": -73.966113},
    {"name": "23 St", "lat": 40.742878, "lon": -73.992821},
    {"name": "Canal St", "lat": 40.718092, "lon": -74.000494},
    {"name": "Chambers St", "lat": 40.714111, "lon": -74.008585},
    {"name": "125 St", "lat": 40.804138, "lon": -73.937594},
    {"name": "96 St", "lat": 40.793919, "lon": -73.972323},
    {"name": "14 St", "lat": 40.737826, "lon": -74.000201},
    {"name": "Union Sq-14 St", "lat": 40.734673, "lon": -73.989951},
    {"name": "W 4 St-Wash Sq", "lat": 40.732338, "lon": -74.000495},
    {"name": "Jay St-MetroTech", "lat": 40.692338, "lon": -73.987342},
    {"name": "Atlantic Av-Barclays Ctr", "lat": 40.684359, "lon": -73.977666},
    {"name": "Court Sq", "lat": 40.747023, "lon": -73.945264},
    {"name": "Queensboro Plaza", "lat": 40.750582, "lon": -73.940202},
    {"name": "Jackson Hts-Roosevelt Av", "lat": 40.746644, "lon": -73.891338},
    {"name": "74 St-Broadway", "lat": 40.746848, "lon": -73.891394},
    {"name": "Flushing-Main St", "lat": 40.759600, "lon": -73.830030},
    {"name": "Myrtle-Wyckoff Avs", "lat": 40.699814, "lon": -73.912602},
    {"name": "Bedford Av", "lat": 40.717304, "lon": -73.956872},
    {"name": "Broadway Junction", "lat": 40.678334, "lon": -73.905316},
]



class NewYorkSmartGridStreamGenerator(StreamGenerator):
    namespace: str = "new_york_smart_grid"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def get_data(self) -> pd.DataFrame:
        # Generate a DataFrame with random data for the New York Smart Grid
        num_rows = random.randint(self.min_rows, self.max_rows)
        timestamps = [self.current_time + timedelta(seconds=i * self.interval) for i in range(num_rows)]
        data = {
            "timestamp": timestamps,
            "energy_consumption": [random.uniform(0, 100) for _ in range(num_rows)],
            "voltage": [random.uniform(110, 120) for _ in range(num_rows)],
            "current": [random.uniform(0, 10) for _ in range(num_rows)],
            "power_factor": [random.uniform(0.8, 1.0) for _ in range(num_rows)],
            "battery_soc": [random.uniform(20, 100) for _ in range(num_rows)],  # State of Charge
            "battery_charge_rate": [random.uniform(-10, 10) for _ in range(num_rows)],  # Charge rate
            "renewable_power_generation": [random.uniform(0, 50) for _ in range(num_rows)],  # Renewable power
            "transformer_temperature": [random.uniform(20, 80) for _ in range(num_rows)],  # Transformer temperature
        }
        self.current_time = timestamps[-1] + timedelta(seconds=self.interval)
        return pd.DataFrame(data)

    @property
    def schema(self) -> dict:
        return {
            "timestamp": "datetime64[ns]",
            "energy_consumption": "float",
            "voltage": "float",
            "current": "float",
            "power_factor": "float",
            "battery_soc": "float",
            "battery_charge_rate": "float",
            "renewable_power_generation": "float",
            "transformer_temperature": "float",
        }

    @staticmethod
    def required_parameters() -> dict[str, str]:
        return {
            "interval": "float",
            "nrows": "int or tuple[int, int]",
            "start_time": "str or datetime",
            "end_time": "str or datetime",
            "loopback": "bool",
            "data_callback_function": "callable",
        }

    @staticmethod
    def from_config(config: dict) -> 'NewYorkSmartGridStreamGenerator':
        return NewYorkSmartGridStreamGenerator(**config)
    

# add the generator to the registry
generator_registry["new_york_smart_grid"] = NewYorkSmartGridStreamGenerator
