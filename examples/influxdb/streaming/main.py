"""
Start a smart_grid streaming data generator and send the data to InfluxDB.
"""

# add the project root dir to the python path for modules to import correctly
import os
import sys
sys.path.append('../../../')

from datetime import datetime, timedelta
from time import sleep
from utils.logger import logger
from utils.config_loader import config
from generators.smart_grid.new_york_smart_grid import NewYorkSmartGridStreamGenerator
from writers.influxdb_writer import InfluxdbWriter


# ============================================
# DEMO CONFIGURATION PARAMETERS
# change these parameters to customize the demo
# ============================================
START_TIME = datetime.now() - timedelta(minutes=0)                  # demo start time
END_TIME = datetime.now() + timedelta(minutes=30, seconds=0)        # demo end time
INFLUXDB_URL = 'http://localhost:8086'
INFLUXDB_TOKEN = 'sudo-banana-404'
INFLUXDB_ORG = 'perspective'
INFLUXDB_BUCKET = 'smart_grid'


def main():
    logger.info('-' * 80)
    logger.info('DEMO: INFLUXDB -> PROSPECTIVE')
    logger.info('Starting smart grid streaming data generator...')
    logger.info('Sending data to InfluxDB...')

    # --------------------------------------------
    # DATA GENERATORS & WRITERS

    smart_grid = NewYorkSmartGridStreamGenerator(
        interval=0.250,         # refresh every 0.25 seconds
        nrows=100,              # number of rows to generate in each batch
        num_stations=32,        # number of stations
        start_time=START_TIME,  # start time
        end_time=END_TIME,      # end time
        loopback=True,          # loopback if we exhaust the generated data
        )

    # Define the InfluxDB writer
    influx_writer = InfluxdbWriter(
        url=INFLUXDB_URL,
        token=INFLUXDB_TOKEN,
        org=INFLUXDB_ORG,
        bucket=INFLUXDB_BUCKET,
        measurement='new_york_smart_grid',
        timestamp_col='timestamp',
        tag_cols=['station_name', 'latitude', 'longitude'],
        field_cols=['energy_consumption', 'current', 'voltage', 'status', 'power_factor', 'battery_soc', 'battery_charge_rate', 'renewable_power_generation', 'transformer_temperature'],
        )
    
    # attach data writers to the generators
    smart_grid.add_subscriber(influx_writer)
    # setup complete
    logger.info('Setup complete. Starting data generation...')

    # --------------------------------------------
    # MAIN STREAMING LOOP
    smart_grid.start()
    # wait for the demo loop to finish or until the user interrupts
    try:
        while smart_grid.is_running():
            sleep(1)
    except KeyboardInterrupt:
        logger.info('Demo: Execution interrupted by the user. Ending demo...')
        smart_grid.stop()
    finally:
        # Ensure the InfluxDB writer is closed properly
        smart_grid.stop()
        influx_writer.close()
        del influx_writer
        del smart_grid
        logger.info('Demo: Ended New York Smart Grid demo.')


if __name__ == '__main__':
    main()
