"""
Description: This file contains the registry of all the generators and writers that are available in the system.
The generator_registry contains the mapping of generator names to the generator classes.
The writer_registry contains the mapping of writer names to the writer classes.
"""

from generators.smart_grid.new_york_smart_grid import NewYorkSmartGridStreamGenerator
from writers.file_writer import FileWriter
from writers.console_writer import ConsoleWriter
from writers.influxdb_writer import InfluxdbWriter


generator_registry = {
    "new_york_smart_grid": NewYorkSmartGridStreamGenerator,
}

writer_registry = {
    "csv": FileWriter,
    "console": ConsoleWriter,
    "influxdb": InfluxdbWriter,
}
