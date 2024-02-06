##
# Copyright (c) 2024, Chad Juliano, Kinetica DB Inc.
##

from typing import Any, List
from pandas import DataFrame

from langchain_core.output_parsers.transform import BaseOutputParser
from langchain_core.outputs import Generation
from langchain_core.pydantic_v1 import Field, BaseModel

from gpudb import GPUdb

class SqlResponse(BaseModel):
    """ Response containing SQL and the fetched data """
    
    sql: str = Field(description="Result SQL")
    dataframe: DataFrame = Field(description="Result Data")

    class Config:
        """Configuration for this pydantic object."""
        arbitrary_types_allowed = True


class KineticaSqlOutputParser(BaseOutputParser[SqlResponse]):
    """ Fetch and return data from the Kinetica LLM """

    kdbc: GPUdb = Field(exclude=True)
    """ Kinetica DB connection. """

    class Config:
        """Configuration for this pydantic object."""
        arbitrary_types_allowed = True

    def parse(self, text: str) -> SqlResponse:
        df = self.kdbc.to_df(text)
        return SqlResponse(sql=text, dataframe=df)
    
    def parse_result(self, result: List[Generation], *, partial: bool = False) -> SqlResponse:
        return self.parse(result[0].text)
    
    @property
    def _type(self) -> str:
        return "kinetica_sql_output_parser"
    
