from pydantic import BaseModel, Field, field_validator
from typing import Optional, Any
import yaml

import perspective_demo.data_sources as pds


class StreamDemo(BaseModel):
    name: str = Field(..., description='Name of the Perspective demo.')
    description: Optional[str] = Field(None, description='Description of the Perspective demo.')
    interval: float = Field(1.0, description='Interval in seconds to refresh the data.')
    sources: list[pds.PerspectiveDemoStreamDataSource] = Field(..., description='List of data sources for the Perspective demo.')

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
    url_path: str = Field('/perspective', description='The URL path to serve the Perspective demo on.')

    def init(self):
        pass

    def run(self):
        pass
