from dataclasses import dataclass, field
from typing import Optional, Any, Dict,List

@dataclass
class Task:
    """
    Represents a task with question and database details.

    Attributes:
        question_id (int): The unique identifier for the question.
        db_id (str): The database identifier.
        question (str): The question text.
        evidence (str): Supporting evidence for the question.
        SQL (Optional[str]): The SQL query associated with the task, if any.
        difficulty (Optional[str]): The difficulty level of the task, if specified.
    """
    question_id: int = field(init=False)
    db_id: str = field(init=False)
    question: str = field(init=False)
    SQL: Optional[str] = field(init=False, default=None)
    difficulty: Optional[str] = field(init=False, default=None)

    def __init__(self, task_data: Dict[str, Any]):
        """
        Initializes a Task instance using data from a dictionary.

        Args:
            task_data (Dict[str, Any]): A dictionary containing task data.
        """
        self.question_id = task_data["question_id"]
        self.db_id = task_data["db_id"]
        self.question = task_data["question"]
        self.SQL = task_data.get("SQL", "") 
        self.db_desc = task_data["db_desc"] 
        self.db_desc_info = task_data['db_desc_info']
        self.difficulty = task_data.get("difficulty")
        self.fd_list = task_data.get('fd_list')
        self.consistency_redundant_columns = task_data.get("consistency_redundant_columns")
        self.inconsistency_redundant_columns = task_data.get("inconsistency_redundant_columns")
        self.example = task_data.get("example")
        self.execute_history = set()