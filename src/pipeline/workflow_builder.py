from typing import Dict, List, TypedDict, Callable
from langgraph.graph import END, StateGraph
from pipeline.node_func import schema_linking, schema_linking_info, sql_generation, sql_style_refinement, \
    sql_output_refinement, sql_selection
import logging


class GraphState(TypedDict):
    """
    Represents the state of our graph.

    Attributes:
        keys: A dictionary where each key is a string.
    """
    keys: Dict[str, any]


class WorkflowBuilder:
    def __init__(self):
        self.workflow = StateGraph(GraphState)
    
    def build(self, pipeline_nodes:str) -> None:
        """
        Builds the workflow based on the provided pipeline nodes.

        Args:
            pipeline_nodes (str): A string of pipeline node names separated by '+'.
        """
        nodes = pipeline_nodes.split('+')
        logging.info(f"Building workflow with nodes: {nodes}")
        self._add_nodes(nodes)
        self.workflow.set_entry_point(nodes[0])
        self._add_edges([(nodes[i], nodes[i+1]) for i in range(len(nodes) - 1)]) 
        self._add_edges([(nodes[-1], END)])
        logging.info("Workflow built successfully")

    def _add_nodes(self, nodes: List) -> None:
        for node_name in nodes:
            if node_name in globals() and callable(globals()[node_name]):  # 找到全局定义的函数
                self.workflow.add_node(node_name, globals()[node_name])
                logging.info(f"Added node: {node_name}")
            else:
                logging.error(f"Node function '{node_name}' not found in global scope")


    def _add_edges(self, edges: list) -> None:
        """
        Adds edges between nodes in the workflow.

        Args:
            edges (list): A list of tuples representing the edges.
        """
        for src, dst in edges:
            self.workflow.add_edge(src, dst)
            logging.info(f"Added edge from {src} to {dst}") 





def build_pipeline(pipeline_nodes: str) -> Callable:
    """
    Builds and compiles the pipeline based on the provided nodes.

    Args:
        pipeline_nodes (str): A string of pipeline node names separated by '+'.

    Returns:
        Callable: The compiled workflow application.
    """
    builder = WorkflowBuilder()
    builder.build(pipeline_nodes)
    app = builder.workflow.compile()
    logging.info("Pipeline built and compiled successfully")
    return app