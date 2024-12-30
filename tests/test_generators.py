import pytest
from datetime import datetime, timedelta
import pandas as pd
from perspective_data.generators.smart_grid import NewYorkSmartGridStreamGenerator

@pytest.fixture
def generator():
    return NewYorkSmartGridStreamGenerator(
        interval=86400, 
        nrows=1000, 
        num_stations=32, 
        start_time=datetime.now().replace(microsecond=0), 
        loopback=True
    )

def test_initialization(generator):
    assert generator.interval == 86400
    assert generator.nrows == 1000
    assert generator.num_stations == 32
    assert generator.loopback is True
    assert isinstance(generator.start_time, datetime)

def test_schema(generator):
    expected_schema = {
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
    assert generator.schema == expected_schema

def test_get_data(generator):
    df = generator.get_data()
    assert isinstance(df, pd.DataFrame)
    assert len(df) == generator.num_stations
    assert list(df.columns) == list(generator.schema.keys())

def test_loopback(generator):
    for _ in range(generator.nrows + 1):
        df = generator.get_data()
    assert generator._cur_frame == 1

def test_end_time(generator):
    generator.end_time = generator.start_time + timedelta(days=1)
    for _ in range(2):
        df = generator.get_data()
    assert df.empty
