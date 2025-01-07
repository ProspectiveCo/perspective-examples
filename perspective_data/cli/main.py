import click
from perspective_data.utils import load_config_yaml, setup_logging_from_config
from registry import generator_registry, writer_registry

@click.command()
@click.option('--config-file', type=click.Path(exists=True), help='Path to the YAML configuration file.', default=None)
@click.option('--log-level', default='INFO', help='Logging level.')
def main(config_file, log_level):
    pass

if __name__ == '__main__':
    main()