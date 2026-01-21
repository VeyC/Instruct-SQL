import json
import os
from pathlib import Path
from typing import Any, Dict, List, Tuple
from database_manager import DatabaseManager
from pipeline.pipeline_manager import PipelineManager
from pipeline.workflow_builder import build_pipeline
from task import Task
from logger import Logger

 
class RunManager:
    RESULT_ROOT_PATH = "results"

    def __init__(self, args:Any, batch_id: int = None) -> None:
        self.args = args
        self.batch_id = batch_id
        self.result_directory = self.get_result_directory()

        print('********'*10)
        print(f'初始化批次 {batch_id} 保存的地址')
        print(self.result_directory)

        self.tasks: List[Task] = []
        self.total_number_of_tasks = 0
        self.processed_tasks = 0
    
    def initialize_tasks(self, dataset:List[Dict[str, Any]]):
        "为每个sample初始化一个task"
        # test_ids = [915, 1040]
        # test_ids = [0] #[964] #[653] #[964]
        # test_ids = sorted(test_ids)
        for i, data in enumerate(dataset):
            # if i not in test_ids:
            #     continue
            # if data['question_id'] < 101:
            #     continue
            task = Task(data)  # 为每条数据分配一个data
            self.tasks.append(task)
        self.total_number_of_tasks = len(self.tasks)
        print(f"Total number of tasks: {self.total_number_of_tasks}")

    def run_tasks(self):
        for task in self.tasks:
            ans = self.worker(task)
            self.task_done(ans)


    def get_result_directory(self) -> str:
        """
        Creates and returns the result directory path based on the input arguments.
        
        Returns:
            str: The path to the result directory.
        """
        data_mode = self.args.mode
        pipeline_nodes = self.args.pipeline_nodes
        dataset_name = Path(self.args.db_root_path).stem
        run_folder_name = str(self.args.run_start_time)
        if self.batch_id is not None:
            run_folder_name = f"{run_folder_name}_batch_{self.batch_id}"
        run_folder_path = Path(self.RESULT_ROOT_PATH) / data_mode / pipeline_nodes / dataset_name / run_folder_name
        
        run_folder_path.mkdir(parents=True, exist_ok=True)
        
        arg_file_path = run_folder_path / "-args.json"
        with arg_file_path.open('w') as file:
            json.dump(vars(self.args), file, indent=4)
        
        log_folder_path = run_folder_path / "logs"
        log_folder_path.mkdir(exist_ok=True)
        
        return str(run_folder_path)


    
    def worker(self, task: Task) -> Tuple[Any, str, int]:

        logger = Logger(db_id=task.db_id, question_id=task.question_id, result_directory=self.result_directory) # 这里保存的json，依靠装饰器
        logger._set_log_level(self.args.log_level)
        logger.log(f"Processing task: {task.db_id} {task.question_id}", "info")

        # 重用预加载的 Manager 实例
        pipeline_manager = PipelineManager(json.loads(self.args.pipeline_setup))  # 初始化，单例
        database_manager = DatabaseManager(db_mode=self.args.mode, db_root_path=self.args.db_root_path, db_id=task.db_id) # 根据db_id重新初始化
        # arctic_manager 已经在 main 中预加载
        initial_state = {"keys": {"task": task, "execution_history": []}} 

        print(f'处理 question id:{task.question_id}. 建立工作流 ...')
        self.app = build_pipeline(self.args.pipeline_nodes)
        print("Pipeline built successfully.")

        if hasattr(self.app, 'nodes'):
            # 获取最后一个节点的键
            if self.app.nodes:  # 确保字典不为空
                last_node_key = list(self.app.nodes.keys())[-1]  # 获取最后一个键
                print('checkpoint final: ',last_node_key)  # 如果你需要查看最后一个节点，可以保留这一行
            else:
                last_node_key = None  # 如果没有节点，设置为 None
        else:
            last_node_key = None  # 如果没有 nodes 属性，设置为 None

        for state in self.app.stream(initial_state):  # 在这里执行工作流
            continue

        return state[last_node_key], task.db_id, task.question_id
    


    def task_done(self, log: Tuple[Any, str, int]):
        """
        Callback function when a task is done.
        
        Args:
            log (tuple): The log information of the task processing.
        """
        state, db_id, question_id = log
        print('-'*20)
        print(state)
        if state is None:
            return

        self.processed_tasks += 1
        
        processed_ratio = self.processed_tasks / self.total_number_of_tasks
        progress_length = int(processed_ratio * 100)
        print('\x1b[1A' + '\x1b[2K' + '\x1b[1A')  # Clear previous line
        print(f"[{'=' * progress_length}>{' ' * (100 - progress_length)}] {self.processed_tasks}/{self.total_number_of_tasks}")

    
    def generate_sql_files(self):  
        """Generates SQL files from the execution history."""
        sqls = {}
        
        print('*'*20)
        print(self.result_directory)
        for file in os.listdir(self.result_directory):
            print(file)
            # 只处理任务结果文件，跳过汇总文件（以 - 开头的文件）
            if file.endswith(".json") and "_" in file and not file.startswith("-"):
                _index = file.find("_")
                question_id = int(file[:_index])
                db_id = file[_index + 1:-5]
                with open(os.path.join(self.result_directory, file), 'r') as f:
                    exec_history = json.load(f)
                    for step in exec_history:
                        if "sqls" in step:
                            node_type = step["node_type"]
                            if node_type not in sqls:
                                sqls[node_type] = {}
                            sqls[node_type][question_id] = step["sqls"]
        print(sqls)    
        for key, value in sqls.items():
            with open(os.path.join(self.result_directory, f"-{key}.json"), 'w') as f:
                json.dump(value, f, indent=2, ensure_ascii=False)
        return self.result_directory
        