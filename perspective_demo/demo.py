from pydantic import BaseModel, Field, field_validator
from typing import Optional, Any
import yaml

from perspective_demo.utils import settings, logger
import perspective_demo.data_sources as pds


import tornado.websocket
import tornado.web
import tornado.ioloop
from datetime import date, datetime
import perspective
import perspective.handlers.tornado



class StreamDemo(BaseModel):
    name: str = Field(..., description='Name of the Perspective demo.')
    description: Optional[str] = Field(None, description='Description of the Perspective demo.')
    interval: float = Field(1.0, description='Interval in seconds to refresh the data.')
    sources: list[pds.ProspectiveDemoStreamDataSource] = Field(..., description='List of data sources for the Perspective demo.')

    @field_validator('sources')
    def validate_sources(cls, v):
        if not v:
            raise ValueError('sources must be a non-empty list')
        return v
    
    @classmethod
    def from_yaml(cls, filename: str) -> 'StreamDemo':
        with open(filename, 'r') as file:
            data = yaml.safe_load(file)
        return cls(**data)
    
    def init(self):
        raise NotImplementedError('init method must be implemented by subclasses')
    
    def run(self):
        raise NotImplementedError('run method must be implemented by subclasses')
    

class PerspectiveServerStreamDemo(StreamDemo):
    host: str = Field('localhost', description='The host IP address or name to bind to.')
    port: int = Field(8080, description='The port number to bind to.')
    endpoint: str = Field('/perspective', description='The URL endpoint to serve the Perspective demo on.')

    # private attributes
    _perspective_server: Any = None
    _perspective_client: Any = None
    _app: Any = None
    _tables: list[Any] = []

    def init(self):
        # create a new Perspective server
        prsp_server = perspective.Server()
        prsp_client = prsp_server.new_local_client()
        # create the Tornado application
        app = self._make_app()
        app.listen(self.port, self.host)
        # assign to private attributes
        self._perspective_server = prsp_server
        self._perspective_client = prsp_client
        self._app = app
        logger.info(f"Listening on http://{self.host}:{self.port}{self.endpoint}")


    def _make_app(self):
        # make a tornado application
        return tornado.web.Application([
            (
                self.endpoint,
                perspective.handlers.tornado.PerspectiveTornadoHandler,
                {"perspective_server": self._perspective_server},
            ),
        ])
    
    def _create_perspective_tables(self):
        # initialize the perspective_client if not already initialized
        if self._perspective_client is None:
            self._perspective_client = self._perspective_server.new_local_client()
        # create a new Perspective table
        client = self._perspective_client
        for source in self.sources:
            table = client.table(
                source.schema,
                limit=source.limit,
                name=source.name,
            )
            logger.info(f"Created new Perspective table '{source.name}'")
            self._tables.append(table)

    def _refresh_perspective_tables(self):
        # update with new data every 50ms
        def updater():
            for table, source in zip(self._tables, self.sources):
                table.update(source.next())
        # start the periodic callback to update the table data
        callback = tornado.ioloop.PeriodicCallback(callback=updater, callback_time=self.interval * 1000)
        callback.start()

    def run(self):
        pass


if __name__ == "__main__":
    print("hi")
