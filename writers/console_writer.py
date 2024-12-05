import pandas as pd
from base import DataWriter

class ConsoleWriter(DataWriter):
    
    def __init__(self, max_rows=None, max_columns=None):
        self.max_rows = max_rows
        self.max_columns = max_columns

    def write(self, df: pd.DataFrame):
        with pd.option_context('display.max_rows', self.max_rows, 'display.max_columns', self.max_columns):
            print(df)