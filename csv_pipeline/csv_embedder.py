from typing import Union,Dict
from pathlib import Path
import pandas as pd
from pydantic import validate_call, BaseModel,Field
from typing import Optional, Any
import uuid
from neo4j_graphrag.embeddings.base import Embedder
from neo4j_graphrag.experimental.components.types import TextChunk, TextChunks
from neo4j_graphrag.experimental.pipeline import Component, DataModel
from csv_pipeline.csv_loader import CSVProperty, CSVRow, CSVFull
import asyncio
import torch

# class HuggingFaceEmbedder(Embedder):
#     def __init__(self, model_name='sentence-transformers/all-MiniLM-L6-v2'):
#         device = 'cuda' if torch.cuda.is_available() else 'cpu'
#         self.model = SentenceTransformer(model_name, device=device)

#     def embed_query(self, text: str) -> list[float]:
#         return self.model.encode(text, convert_to_numpy=True).tolist()

class CSVPropertyEmbedder(Component):
    """Component for creating embeddings from text chunks.

    Args:
        embedder (Embedder): The embedder to use to create the embeddings.

    Example:

    .. code-block:: python

        from neo4j_graphrag.experimental.components.embedder import TextChunkEmbedder
        from neo4j_graphrag.embeddings.openai import OpenAIEmbeddings
        from neo4j_graphrag.experimental.pipeline import Pipeline

        embedder = OpenAIEmbeddings(model="text-embedding-3-large")
        chunk_embedder = TextChunkEmbedder(embedder)
        pipeline = Pipeline()
        pipeline.add_component(chunk_embedder, "chunk_embedder")

    """

    def __init__(self, embedder:Embedder):
        self._embedder = embedder

    async def _embed_property(self, sem:asyncio.Semaphore , property: CSVProperty) -> CSVProperty:
        """Embed a single text chunk.

        Args:
            text_chunk (TextChunk): The text chunk to embed.

        Returns:
            TextChunk: The text chunk with an added "embedding" key in its
            metadata containing the embeddings of the text chunk's text.
        """
        async with sem:
            embedding = self._embedder.embed_query(property.name)
            metadata = property.metadata if property.metadata else dict()
            metadata["embedding"] = embedding
            return CSVProperty(
                name = property.name,
                column_name= property.column_name,
                index = property.index,
                metadata = metadata
            )

    @validate_call
    async def run(self, rows: list[CSVRow], property_names: list[str], max_concurrency: int = 5) -> CSVFull:
        """Embed a list of text chunks.

        Args:
            text_chunks (TextChunks): The text chunks to embed.

        Returns:
            TextChunks: The input text chunks with each one having an added embedding.
        """
        sem = asyncio.Semaphore(max_concurrency)
        for row in rows:
            row_tasks = []
            for property in row.properties:
                row_tasks.append(self._embed_property(sem = sem, property = property))
            row.properties = await asyncio.gather(*row_tasks)
                
        return CSVFull(rows = rows, property_names = property_names)
