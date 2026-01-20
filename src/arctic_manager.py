import re
from typing import List, Dict, Any, Optional
from threading import Lock

import torch
from transformers import AutoTokenizer
from vllm import LLM, SamplingParams


class ArcticManager:
    """
    A singleton class to manage vLLM model inference.
    Loads the model once during initialization and provides inference methods.
    """
    _instance = None
    _lock = Lock()

    def __new__(cls, pretrained_model_name_or_path=None, tensor_parallel_size=None, 
                temperature=None, n=None, **kwargs):
        """
        Singleton pattern implementation with lazy initialization.
        
        Args:
            pretrained_model_name_or_path (str): Path to the pretrained model
            tensor_parallel_size (int): Number of GPUs for tensor parallelism
            temperature (float): Sampling temperature
            n (int): Number of responses to generate
            **kwargs: Additional parameters for model configuration
        """
        if pretrained_model_name_or_path is not None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(ArcticManager, cls).__new__(cls)
                    cls._instance._initialized = False
                
                # Re-initialize if model path changes
                if not cls._instance._initialized or \
                   cls._instance.pretrained_model_name_or_path != pretrained_model_name_or_path:
                    cls._instance._init(
                        pretrained_model_name_or_path=pretrained_model_name_or_path,
                        tensor_parallel_size=tensor_parallel_size,
                        temperature=temperature,
                        n=n,
                        **kwargs
                    )
                return cls._instance
        else:
            if cls._instance is None or not cls._instance._initialized:
                raise ValueError("ArcticManager instance has not been initialized yet.")
            return cls._instance

    def _init(self, pretrained_model_name_or_path: str, 
              tensor_parallel_size: int = 4,
              temperature: float = 1.0,
              n: int = 4,
              max_model_len: int = 16384,
              max_input_len: int = 8192,
              max_output_len: int = 8192,
              gpu_memory_utilization: float = 0.92,
              swap_space: int = 42,
              **kwargs):
        """
        Initializes the ArcticManager instance.

        Args:
            pretrained_model_name_or_path (str): Path to the pretrained model
            tensor_parallel_size (int): Number of GPUs for tensor parallelism
            temperature (float): Sampling temperature
            n (int): Number of responses to generate
            max_model_len (int): Maximum model length
            max_input_len (int): Maximum input length
            max_output_len (int): Maximum output length
            gpu_memory_utilization (float): GPU memory utilization ratio
            swap_space (int): Swap space in GB
            **kwargs: Additional parameters
        """
        self.pretrained_model_name_or_path = pretrained_model_name_or_path
        self.tensor_parallel_size = tensor_parallel_size
        self.temperature = temperature
        self.n = n
        self.max_model_len = max_model_len
        self.max_input_len = max_input_len
        self.max_output_len = max_output_len
        
        # Load tokenizer
        print(f"Loading tokenizer from {pretrained_model_name_or_path}...")
        self.tokenizer = AutoTokenizer.from_pretrained(
            pretrained_model_name_or_path, 
            trust_remote_code=True
        )
        
        # Determine stop token ids based on model name
        self.stop_token_ids = self._get_stop_token_ids(pretrained_model_name_or_path)
        print(f"stop_token_ids: {self.stop_token_ids}")
        
        # Initialize sampling parameters
        self.sampling_params = SamplingParams(
            temperature=self.temperature,
            max_tokens=self.max_output_len,
            n=self.n,
            stop_token_ids=self.stop_token_ids
        )
        
        # Load vLLM model
        print(f"Loading vLLM model from {pretrained_model_name_or_path}...")
        print(f"Tensor parallel size: {tensor_parallel_size}")
        print(f"Max model length: {max_model_len}")
        print(f"Temperature: {temperature}")
        
        self.llm = LLM(
            model=pretrained_model_name_or_path,
            dtype="bfloat16",
            tensor_parallel_size=tensor_parallel_size,
            max_model_len=max_model_len,
            gpu_memory_utilization=gpu_memory_utilization,
            swap_space=swap_space,
            enforce_eager=True,
            disable_custom_all_reduce=True,
            trust_remote_code=True,
        )
        
        self._initialized = True
        print("ArcticManager initialized successfully!")

    @staticmethod
    def _get_stop_token_ids(model_path: str) -> List[int]:
        """
        Determine stop token IDs based on model name.
        
        Args:
            model_path (str): Path to the pretrained model
            
        Returns:
            List[int]: List of stop token IDs
        """
        model_path_lower = model_path.lower()
        
        if "arctic" in model_path_lower:
            return [151645]
        elif "Qwen2.5-" in model_path:
            return [151645]
        elif "OmniSQL-" in model_path:
            return [151645]
        elif "deepseek-coder-" in model_path_lower:
            return [32021]
        elif "DeepSeek-Coder-V2" in model_path:
            return [100001]
        elif "OpenCoder-" in model_path:
            return [96539]
        elif "Meta-Llama-" in model_path:
            return [128009, 128001]
        elif "granite-" in model_path_lower:
            return [0]
        elif "starcoder2-" in model_path_lower:
            return [0]
        elif "Codestral-" in model_path:
            return [2]
        elif "Mixtral-" in model_path:
            return [2]
        else:
            print("Use Qwen2.5's stop tokens by default.")
            return [151645]

    @staticmethod
    def parse_response(response: str) -> str:
        """
        Parse SQL query from response text.
        
        Args:
            response (str): Response text containing SQL query
            
        Returns:
            str: Extracted SQL query
        """
        pattern = r"```sql\s*(.*?)\s*```"
        sql_blocks = re.findall(pattern, response, re.DOTALL)
        
        if sql_blocks:
            # Extract the last SQL query and remove extra whitespace
            last_sql = sql_blocks[-1].strip()
            return last_sql
        else:
            return ""

    def create_sql_prompt(self, db_desc: str, question: str) -> str:
        """
        Create a SQL generation prompt from database description and question.
        
        Args:
            db_desc (str): Database schema description
            question (str): Natural language question
            
        Returns:
            str: Formatted prompt
        """
        cot_info = "Let me solve this step by step. \n<think>"
        instruct_info = """
Please provide a detailed chain-of-thought reasoning process and include your thought process within `<think>` tags. Your final answer should be enclosed within `<answer>` tags.

Ensure that your SQL query follows the correct syntax and is formatted as follows:

```sql
-- Your SQL query here
```

Example format:
<think> Step-by-step reasoning, including self-reflection and corrections if necessary. [Limited by 4K tokens] </think>
<answer> Summary of the thought process leading to the final SQL query. [Limited by 1K tokens]

```sql
Correct SQL query here
```
</answer>""".strip()
        
        messages = [
            {
                "role": "system",
                "content": (
                    "You are a data science expert. Below, you are provided with a database schema and a natural"
                    " language question. Your task is to understand the schema and generate a valid SQL query to"
                    " answer the question."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"""
Database Engine:
SQLite

Database Schema:
{db_desc}
This schema describes the database's structure, including tables, columns, primary keys, foreign keys, and any relevant relationships or constraints.

Question:
{question}

Instructions:
- **Each column name must be enclosed in double quotation marks just like they are in the schema, table name do not need, for example: "age"(column), university.info.student(table).** This is very important as it will directly affect whether your SQL can be executed.
- Make sure you only output the information that is asked in the question. If the question asks for a specific column, make sure to only include that column in the SELECT clause, nothing more.
- The generated query should return all of the information asked in the question without any missing or extra information.
- **MUST replace `SELECT ... value = (SELECT MAX/MIN(column)...)` with `ORDER BY ... LIMIT 1`**
- Before generating the final SQL query, please think through the steps of how to write the query.

Output Format:
{instruct_info}
    """.strip()
                ),
            },
        ]
        
        prompt = self.tokenizer.apply_chat_template(
            messages, 
            add_generation_prompt=True, 
            tokenize=False
        )
        prompt += cot_info
        
        return prompt

    def generate(self, prompts: List[str], 
                 sampling_params: Optional[SamplingParams] = None,
                 use_tqdm: bool = False) -> List[Dict[str, Any]]:
        """
        Generate responses for a list of prompts.
        
        Args:
            prompts (List[str]): List of input prompts
            sampling_params (Optional[SamplingParams]): Custom sampling parameters
            use_tqdm (bool): Whether to show progress bar
            
        Returns:
            List[Dict[str, Any]]: List of results containing responses and parsed SQLs
        """
        if sampling_params is None:
            sampling_params = self.sampling_params
        
        # Generate outputs
        outputs = self.llm.generate(prompts, sampling_params, use_tqdm=use_tqdm)
        
        # Parse results
        results = []
        for output in outputs:
            responses = [o.text for o in output.outputs]
            sqls = [self.parse_response(response) for response in responses]
            
            results.append({
                "responses": responses,
                "pred_sqls": sqls
            })
        
        return results

    def infer(self, input_text: str, 
              db_desc: Optional[str] = None,
              question: Optional[str] = None,
              return_all: bool = False) -> str:
        """
        Main inference method. Takes an input string and returns the generated SQL.
        
        Args:
            input_text (str): Input prompt text. If db_desc and question are provided,
                            this will be ignored and a SQL prompt will be created.
            db_desc (Optional[str]): Database schema description
            question (Optional[str]): Natural language question
            return_all (bool): If True, return all n responses; otherwise return first
            
        Returns:
            str or List[str]: Generated SQL query(ies)
        """
        # Create prompt
        if db_desc is not None and question is not None:
            prompt = self.create_sql_prompt(db_desc, question)
        else:
            prompt = input_text
        
        # Generate
        outputs = self.llm.generate([prompt], self.sampling_params, use_tqdm=False)
        
        # Parse responses
        responses = [o.text for o in outputs[0].outputs]
        sqls = [self.parse_response(response) for response in responses]
        
        if return_all:
            return sqls
        else:
            return sqls[0] if sqls else ""

    def batch_infer(self, input_data: List[Dict[str, str]], 
                    use_tqdm: bool = True) -> List[Dict[str, Any]]:
        """
        Batch inference for multiple inputs.
        
        Args:
            input_data (List[Dict[str, str]]): List of dicts with 'db_desc' and 'question'
            use_tqdm (bool): Whether to show progress bar
            
        Returns:
            List[Dict[str, Any]]: List of results with responses and parsed SQLs
        """
        # Create prompts
        prompts = [
            self.create_sql_prompt(data["db_desc"], data["question"])
            for data in input_data
        ]
        
        # Generate
        return self.generate(prompts, use_tqdm=use_tqdm)