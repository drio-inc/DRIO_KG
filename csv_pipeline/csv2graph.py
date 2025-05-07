from neo4j_graphrag.experimental.components.types import Neo4jNode, Neo4jRelationship,Neo4jGraph
from csv_pipeline.csv_loader import CSVProperty, CSVRow, CSVFull
from neo4j_graphrag.experimental.pipeline import Component, DataModel
import asyncio 
from tqdm import tqdm

class Row2GraphExtractor(Component):
    async def run_for_row(
            self,
            sem:asyncio.Semaphore,
            row: CSVRow,
            column_names: list[str]
            ) -> Neo4jGraph :
        return Neo4jGraph.model_validate({'nodes': [], 'relationships': []})
# class Neo4jRelationship(BaseModel):
#     """Represents a Neo4j relationship.

#     Attributes:
#         start_node_id (str): The ID of the start node.
#         end_node_id (str): The ID of the end node.
#         type (str): The relationship type.
#         properties (dict[str, Any]): A dictionary of properties attached to the relationship.
#         embedding_properties (Optional[dict[str, list[float]]]): A list of embedding properties attached to the relationship.
#     """
    def combine_row_graphs(
        self, row_graphs: list[Neo4jGraph]
    ) -> Neo4jGraph:
        """Combine sub-graphs obtained for each row into a single Neo4jGraph object"""
        graph = Neo4jGraph()
        for row_graph in row_graphs:
            graph.nodes.extend(row_graph.nodes)
            graph.relationships.extend(row_graph.relationships)
        return graph

    async def run(
            self,
            rows:list[CSVRow],
            property_names:list[str],
            max_concurrency: int = 5,
        ) -> Neo4jGraph:
        sem =  asyncio.Semaphore(max_concurrency)
        tasks = [self.run_for_row(sem = sem, row = row, column_names = property_names) for row in rows]
        row_graphs = list(await asyncio.gather(*tasks))
        graph = self.combine_row_graphs(row_graphs)
        # logger.debug(f"Extracted graph: {prettify(graph)}")
        return graph
    
class SupplierLocationCSVGraphExtractor(Component):
    async def run_for_row(
            self, 
            sem:asyncio.Semaphore, 
            row: CSVRow, 
            column_names: list[str],
            column_node_order: dict[str,int] = {'country':1, 'state':2, 'city':3, 'postal_code':4, 'address':5, 'supplier_name':6, 'owner_company':7}):
        async with sem:
            row_props_sorted = sorted(row['properties'],key = lambda x: column_node_order[x['column_name']])
            # *heirarchial_props, supplier_prop, owner_prop = row_props_sorted
            relationship_labels = ['STATE_OF', 'CITY WITHIN', 'HAS_POSTCODE', 'LOCATED IN', 'OWNED_BY', 'SUPPLYING_TO']
            nodes = []
            relationships = []
            
            for i in tqdm(range(len(row_props_sorted)-2+1)):
                start,end =row_props_sorted[i:i+2]
                type = relationship_labels[i]
                relationships.append(Neo4jRelationship(
                    start_node_id = start['uid'],
                    end_node_id = end['uid'],
                    type = type,
                    properties = {},
                    embedding_properties = {}
                ))
                if i==0:
                    nodes.append(Neo4jNode(
                        id = start['uid'],
                        label = start['column_name'],
                        properties = {'name': start['name']},
                        embedding_properties = {'name_embedding': start['metadata']['embedding']}
                    ))
                    nodes.append(Neo4jNode(
                        id = end['uid'],
                        label = end['column_name'],
                        properties = {'name': end['name']},
                        embedding_properties = {'name_embedding': end['metadata']['embedding']}
                    ))
                else:
                    nodes.append(Neo4jNode(
                        id = end['uid'],
                        label = end['column_name'],
                        properties = {'name': end['name']},
                        embedding_properties = {'name_embedding': end['metadata']['embedding']}
                    ))
            graph = Neo4jGraph.model_validate(dict(nodes=nodes, relationships=relationships))
            return graph
    
    def combine_row_graphs(
        self, row_graphs: list[Neo4jGraph]
    ) -> Neo4jGraph:
        """Combine sub-graphs obtained for each row into a single Neo4jGraph object"""
        graph = Neo4jGraph()
        for row_graph in row_graphs:
            graph.nodes.extend(row_graph.nodes)
            graph.relationships.extend(row_graph.relationships)
        return graph

    # async def run(
    #         self,
    #         csv_loaded:CSVFull,
    #         max_concurrency: int = 5,
    #     ) -> Neo4jGraph:
    #     return super().run(
    #         csv_loaded = csv_loaded,
    #         max_concurrency = max_concurrency
    #     )
    async def run(
            self,
            rows:list[CSVRow],
            property_names:list[str],
            max_concurrency: int = 20,
        ) -> Neo4jGraph:
        # try:
        sem =  asyncio.Semaphore(max_concurrency)
        tasks = [self.run_for_row(sem = sem, row = row, column_names = property_names) for row in rows]
        row_graphs = list(await asyncio.gather(*tasks))
        graph = self.combine_row_graphs(row_graphs)
        # logger.debug(f"Extracted graph: {prettify(graph)}")
        return graph
        # except Exception as e: 
        #     print(csv_loaded)
        #     raise e
        