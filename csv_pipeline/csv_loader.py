from neo4j_graphrag.experimental.components.pdf_loader import DataLoader
from typing import Union,Dict
from pathlib import Path
import pandas as pd
from pydantic import validate_call, BaseModel,Field,model_validator
from typing import Optional, Any
import uuid
from neo4j_graphrag.experimental.pipeline import DataModel
from typing_extensions import Self
class CSVProperty(BaseModel):
    """A chunk of text split from a document by a text splitter.

    Attributes:
        text (str): The raw chunk text.
        index (int): The position of this chunk in the original document.
        metadata (Optional[dict[str, Any]]): Metadata associated with this chunk.
        uid (str): Unique identifier for this chunk.
    """
    name: str
    column_name: str
    index: int
    metadata: Optional[dict[str, Any]] = None
    uid: str = Field(default_factory=lambda: str(uuid.uuid4()))

    @property
    def property_id(self) -> str:
        return self.uid
    
    @model_validator(mode='after')
    def check_postal(self) -> Self:
        if 'postal_code' in self.column_name:
            if isinstance(self.name,float):
                self.name = str(int(self.name))
        return self
    
    

class CSVRow(BaseModel):
    properties: list[CSVProperty]

class CSVFull(DataModel):
    rows: list[CSVRow]
    property_names: Optional[list[str]]


class PandasCSVLoader(DataLoader):
    async def run(
        self,
        filepath: Union[str, Path],
        metadata: Optional[Dict[str, str]] = None,
    )-> CSVFull:
        rows = []
        if not isinstance(filepath, str):
            filepath = str(filepath)
    
        df_ = pd.read_csv(filepath)
        for idx in range(len(df_)):
            row_d = df_.iloc[idx,:].to_dict()
            rows.append(CSVRow(
                properties = [
                    CSVProperty(
                        name = item if isinstance(item, str) else "",
                        column_name = key,
                        index = idx,
                        metadata = dict()
                    ) for key,item in row_d.items()
                ]
            ))
        return CSVFull(rows = rows, property_names = list(df_.columns))