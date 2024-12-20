import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

from perspective_examples.utils import logger
from perspective_examples.generators.base import StreamGenerator
from perspective_examples.generators.utils import RandomWaveGenerator as rwg


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
    {"name": "South Ferry", "lat": 40.701730, "lon": -74.013168},
    {"name": "Wall St", "lat": 40.707557, "lon": -74.011862},
    {"name": "Bowling Green", "lat": 40.704817, "lon": -74.014065},
    {"name": "City Hall", "lat": 40.713282, "lon": -74.006978},
    {"name": "Brooklyn Bridge-City Hall", "lat": 40.713065, "lon": -74.004131},
    {"name": "Spring St", "lat": 40.722301, "lon": -74.003627},
    {"name": "Houston St", "lat": 40.728251, "lon": -74.005367},
    {"name": "Christopher St-Sheridan Sq", "lat": 40.733422, "lon": -74.002906},
    {"name": "14 St-6 Av", "lat": 40.737825, "lon": -73.996353},
    {"name": "23 St-6 Av", "lat": 40.742954, "lon": -73.994704},
    {"name": "34 St-6 Av", "lat": 40.749719, "lon": -73.987823},
    {"name": "42 St-Port Authority Bus Terminal", "lat": 40.757308, "lon": -73.989735},
    {"name": "50 St", "lat": 40.761728, "lon": -73.983849},
    {"name": "57 St", "lat": 40.764664, "lon": -73.980658},
    {"name": "Lexington Av/51 St", "lat": 40.757107, "lon": -73.971920},
    {"name": "68 St-Hunter College", "lat": 40.768141, "lon": -73.964222},
    {"name": "77 St", "lat": 40.773620, "lon": -73.959874},
    {"name": "86 St-2 Av", "lat": 40.777861, "lon": -73.951669},
    {"name": "96 St-2 Av", "lat": 40.784318, "lon": -73.947152},
    {"name": "103 St", "lat": 40.790600, "lon": -73.947478},
    {"name": "110 St", "lat": 40.796092, "lon": -73.944331},
    {"name": "116 St", "lat": 40.802098, "lon": -73.949625},
    {"name": "125 St-2 Av", "lat": 40.804138, "lon": -73.937594},
    {"name": "135 St", "lat": 40.814229, "lon": -73.940770},
    {"name": "145 St", "lat": 40.824783, "lon": -73.944216},
    {"name": "155 St", "lat": 40.830518, "lon": -73.941514},
    {"name": "168 St", "lat": 40.840719, "lon": -73.939561},
    {"name": "175 St", "lat": 40.847391, "lon": -73.939704},
    {"name": "181 St", "lat": 40.851695, "lon": -73.937969},
    {"name": "Dyckman St", "lat": 40.865490, "lon": -73.927271},
    {"name": "207 St", "lat": 40.864621, "lon": -73.918822},
    {"name": "215 St", "lat": 40.869444, "lon": -73.915279},
    {"name": "Marble Hill-225 St", "lat": 40.874560, "lon": -73.909830},
    {"name": "231 St", "lat": 40.878856, "lon": -73.904834},
    {"name": "238 St", "lat": 40.884667, "lon": -73.900870},
    {"name": "Van Cortlandt Park-242 St", "lat": 40.889248, "lon": -73.898583},
    {"name": "Woodlawn", "lat": 40.886037, "lon": -73.878750},
    {"name": "Wakefield-241 St", "lat": 40.903125, "lon": -73.850620},
    {"name": "Nereid Av", "lat": 40.898286, "lon": -73.854315},
    {"name": "233 St", "lat": 40.893193, "lon": -73.857473},
    {"name": "225 St", "lat": 40.888022, "lon": -73.860341},
    {"name": "219 St", "lat": 40.883895, "lon": -73.862633},
    {"name": "Gun Hill Rd", "lat": 40.877850, "lon": -73.866256},
    {"name": "Burke Av", "lat": 40.871356, "lon": -73.867164},
    {"name": "Allerton Av", "lat": 40.865462, "lon": -73.867352},
    {"name": "Pelham Pkwy", "lat": 40.857192, "lon": -73.867615},
    {"name": "Bronx Park East", "lat": 40.848828, "lon": -73.868457},
    {"name": "E 180 St", "lat": 40.841894, "lon": -73.873488},
    {"name": "West Farms Sq-E Tremont Av", "lat": 40.840295, "lon": -73.880049},
    {"name": "174 St", "lat": 40.837195, "lon": -73.887694},
    {"name": "Freeman St", "lat": 40.829993, "lon": -73.891865},
    {"name": "Simpson St", "lat": 40.824073, "lon": -73.893064},
    {"name": "Intervale Av", "lat": 40.822181, "lon": -73.896736},
    {"name": "Prospect Av", "lat": 40.819585, "lon": -73.901850},
    {"name": "Jackson Av", "lat": 40.816104, "lon": -73.907996},
    {"name": "3 Av-149 St", "lat": 40.816109, "lon": -73.917757},
    {"name": "Brook Av", "lat": 40.807566, "lon": -73.919239},
    {"name": "Cypress Av", "lat": 40.805368, "lon": -73.914042},
    {"name": "E 143 St-St Mary's St", "lat": 40.808719, "lon": -73.907657},
    {"name": "E 149 St", "lat": 40.812118, "lon": -73.904098},
    {"name": "Longwood Av", "lat": 40.816104, "lon": -73.896435},
    {"name": "Hunts Point Av", "lat": 40.820948, "lon": -73.890549},
    {"name": "Whitlock Av", "lat": 40.826525, "lon": -73.886283},
    {"name": "Elder Av", "lat": 40.828584, "lon": -73.879159},
    {"name": "Morrison Av-Soundview", "lat": 40.829521, "lon": -73.874516},
    {"name": "St Lawrence Av", "lat": 40.831509, "lon": -73.867618},
    {"name": "Parkchester", "lat": 40.833226, "lon": -73.860816},
    {"name": "Castle Hill Av", "lat": 40.834255, "lon": -73.851222},
    {"name": "Zerega Av", "lat": 40.836488, "lon": -73.847036},
    {"name": "Westchester Sq-E Tremont Av", "lat": 40.839892, "lon": -73.842952},
    {"name": "Middletown Rd", "lat": 40.843863, "lon": -73.836322},
    {"name": "Buhre Av", "lat": 40.846820, "lon": -73.832569},
    {"name": "Pelham Bay Park", "lat": 40.852462, "lon": -73.828121},
]



class NewYorkSmartGridStreamGenerator(StreamGenerator):
    namespace: str = "new_york_smart_grid"

    def __init__(self, 
                 interval: float = 1.0,
                 nrows: int = 1000,                                         # number of random periods to generate data for before looping back
                 num_stations: int = 32,                                    # num of stations to generate data for
                 start_time: str | datetime = datetime.now(),
                 end_time: str | datetime = None,
                 loopback: bool = True,
                 data_callback_function: callable = None,
                 **kwargs
                 ) -> None:
        # call the parent class constructor passing the required present parameters
        super().__init__(interval=interval, nrows=nrows, start_time=start_time, end_time=end_time, loopback=loopback, callback_subscribers=data_callback_function, **kwargs)
        # check the number of stations
        if num_stations > len(POWER_STATIONS):
            logger.warning(f"NewYorkSmartGridStreamGenerator: Number of stations cannot be greater than {len(POWER_STATIONS)}. Setting to {len(POWER_STATIONS)}")
            num_stations = len(POWER_STATIONS)
        self.num_stations = num_stations
        # Initialize the New York Smart Grid data generator
        self._cache = {}
        self._cur_frame = 0
        self._init_generator()

    def _init_generator(self) -> None:
        # Initialize the generator
        self.current_time = self.start_time if self.start_time is not None else datetime.now()
        # initialize the cache with for each station. Generate a random wave for each station that will be used to generate the data
        self._cache = {}
        for i in range(self.num_stations):
            station = {
                "name": POWER_STATIONS[i]["name"],
                "lat": POWER_STATIONS[i]["lat"],
                "lon": POWER_STATIONS[i]["lon"],
                "power_wave": rwg.sinusoidal_wave(wave_mode='full', varying_mode='both', num_points=self.nrows, periods=random.randint(2, 7), amplitude=(2.0, 10.0), phase=np.random.uniform(0.5, 2*np.pi), noise=0.05).tolist(),
                "battery_wave": rwg.sinusoidal_wave(wave_mode='full', varying_mode='both', num_points=self.nrows, periods=random.randint(1, 5), amplitude=(1.0, 10.0), phase=np.random.uniform(0, 2*np.pi)).tolist(),
                "temperature_wave": (55 + rwg.sinusoidal_wave(wave_mode='full', varying_mode='amp', num_points=self.nrows, periods=random.randint(1, 10), amplitude=(10.0, 30.0), phase=np.random.uniform(0, 2*np.pi))).tolist(),
                "fault": 0,
                "power_seed": random.randint(10_000, 15_000),
                "battery_seed": random.randint(8_000, 12_000),
            }
            self._cache[station["name"]] = station
        logger.debug(f"NewYorkSmartGridStreamGenerator: status=initialized, stations={self.num_stations}, nrows={self.nrows}, loopback={self.loopback}")

    def get_data(self) -> pd.DataFrame:
        # check current frame
        if self._cur_frame >= self.nrows:
            if self.loopback:
                self._cur_frame = 0
            else:
                logger.warning("NewYorkSmartGridStreamGenerator: Reached the end of the data stream and loopback is set to False. Returning an empty DataFrame.")
                return pd.DataFrame()
        # check the current time
        if self.end_time and self.current_time >= self.end_time:
            logger.warning("NewYorkSmartGridStreamGenerator: Reached the end time. Returning an empty DataFrame.")
            return pd.DataFrame()
        
        # Generate a DataFrame with random data for the New York Smart Grid
        data = []
        for i, station in enumerate(self._cache.values()):
            timestamp = self.current_time + timedelta(seconds=(self.interval / self.num_stations) * i)
            power_level = (station["power_seed"] + station["power_wave"][self._cur_frame]) * 10000           # power in mega watts
            battery_level = station["battery_wave"][self._cur_frame]               # battery level in kilo watts
            voltage = random.uniform(110, 120)
            current = power_level / voltage
            power_factor = random.uniform(0.8, 1.0)
            battery_soc = 60 + battery_level
            batter_charge_rate = random.uniform(-10, 10)
            renewable_power_generation = station["battery_seed"] + (battery_level * 1000)  # renewable power in watts
            transformer_temperature = station["temperature_wave"][self._cur_frame]
            # check if station is in fault mode. fault duration is random between 10 and 100 frames
            if station["fault"] > 0:
                power_level = 0
                voltage = 0
                current = 0
                power_factor = 0
                # battery_level = 0
                batter_charge_rate = 0
                # renewable_power_generation = 0
                # transformer_temperature = 0
                station["fault"] -= 1
            else:
                # introduce a fault randomly for a duration of 10 to 100 frames
                if random.random() < 0.01:
                    station["fault"] = random.randint(10, 100)
            # create a row of data
            row = {
                "timestamp": timestamp,
                "station_name": station["name"],
                "latitude": station["lat"],
                "longitude": station["lon"],
                "status": "fault" if station["fault"] > 0 else "normal",
                "energy_consumption": power_level,
                "voltage": voltage,
                "current": current,
                "power_factor": power_factor,
                "battery_soc": battery_soc,  # State of Charge
                "battery_charge_rate": batter_charge_rate,  # Charge rate
                "renewable_power_generation": renewable_power_generation,  # Renewable power
                "transformer_temperature": transformer_temperature,  # Transformer temperature
            }
            data.append(row)
        # advance the time and frame
        self.current_time += timedelta(seconds=self.interval)
        self._cur_frame += 1
        # return the data as a DataFrame
        return pd.DataFrame(data)

    @property
    def schema(self) -> dict:
        return {
            "timestamp": "datetime64[ns]",
            "station_name": "string",
            "latitude": "float",
            "longitude": "float",
            "status": "string",
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
        return {}

    @staticmethod
    def from_config(config: dict) -> 'NewYorkSmartGridStreamGenerator':
        return NewYorkSmartGridStreamGenerator(**config)


def test():
    # create a new instance of the NewYorkSmartGridStreamGenerator
    generator = NewYorkSmartGridStreamGenerator(interval=86400, nrows=1000, num_stations=32, start_time=datetime.now().replace(microsecond=0), loopback=True)
    # get the schema
    print(generator.schema)
    frames = []
    # generate 1 frame of data
    for _ in range(100):
        frames.append(generator.get_data())
    # concatenate the frames
    df = pd.concat(frames)
    # get the first 10 rows of data
    # print(df)
    # save to a file to view
    df.to_csv("test.csv", index=False)


if __name__ == "__main__":
    test()
