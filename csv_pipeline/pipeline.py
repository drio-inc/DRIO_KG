from neo4j_graphrag.experimental.pipeline import Pipeline
from csv_pipeline.csv_loader import PandasCSVLoader
from csv_pipeline.csv_embedder import CSVPropertyEmbedder
from neo4j_graphrag.embeddings.openai import OpenAIEmbeddings
from neo4j_graphrag.embeddings.sentence_transformers import SentenceTransformerEmbeddings
from csv_pipeline.csv2graph import SupplierLocationCSVGraphExtractor
from neo4j_graphrag.experimental.components.kg_writer import Neo4jWriter
from neo4j_graphrag.experimental.components.resolver import SinglePropertyExactMatchResolver
from neo4j import GraphDatabase
from torch import cuda 
import asyncio
device = 'cuda' if cuda.is_available() else 'cpu'

from csv_pipeline.neo4j_cred import NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD, NEO4J_DATABASE
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))
pipe = Pipeline()
pipe.add_component(PandasCSVLoader(), "csv_loader")
pipe.add_component(CSVPropertyEmbedder(SentenceTransformerEmbeddings(device = device)), "embedder")
pipe.add_component(SupplierLocationCSVGraphExtractor(), "graph_extractor")
pipe.add_component(Neo4jWriter(driver=driver, neo4j_database=NEO4J_DATABASE), "writer")
pipe.add_component(
    SinglePropertyExactMatchResolver(driver=driver, neo4j_database=NEO4J_DATABASE),
    "resolver", 
)
pipe.connect("csv_loader", "embedder", input_config={"rows": "csv_loader.rows","property_names": "csv_loader.property_names"})
pipe.connect("embedder", "graph_extractor", input_config={"rows": "embedder.rows", "property_names": "embedder.property_names"})
pipe.connect("graph_extractor", "writer",input_config={"graph": "graph_extractor"})
pipe.connect("writer", "resolver",input_config={})



if __name__ == "__main__":
    asyncio.run(pipe.run({'csv_loader':{'filepath':'/home/raghib/Desktop/DRIO_KD/final_addresses.csv'}}))