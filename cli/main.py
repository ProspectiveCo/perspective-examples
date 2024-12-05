import click
from utils.config_loader import load_config
from utils.logger import setup_logging
from registry import generator_registry, writer_registry

@click.command()
@click.option('--config', type=click.Path(exists=True), help='Path to the YAML configuration file.')
@click.option('--log-level', default='INFO', help='Logging level.')
def main(config, log_level):
    logger = setup_logging(log_level)
    logger.info("Starting the data generation process")

    config_data = load_config(config)
    generator_config = config_data['generator']
    writer_configs = config_data['writers']

    generator_class = generator_registry[generator_config['type']]
    generator = generator_class.from_config(generator_config)

    writers = []
    for writer_config in writer_configs:
        writer_class = writer_registry[writer_config['type']]
        writers.append(writer_class.from_config(writer_config))

    data = generator.generate()
    for writer in writers:
        writer.write(data)
        writer.close()

    logger.info("Data generation process completed")

if __name__ == '__main__':
    main()