import copy
import sqlite3
from transformers.models.auto.modeling_auto import AutoModelForCausalLM
from transformers.models.auto.tokenization_auto import AutoTokenizer
from typing_extensions import Annotated
import requests, time
import torch
import json
import re
import os
from logger import Logger
from util import extract_sql_from_text, extract_json_from_text, execute_sql
import signal
from contextlib import contextmanager

def model_chose(step,model="gpt-4o"):

    if model.startswith("gpt") or model.startswith("claude") or model.startswith("gemini") or model.startswith("qwen"):
        return gpt_req(step,model)
    if model.startswith("sft"):
        return sft_req()


class req:
    def __init__(self, step, model) -> None:
        self.Cost = 0
        self.model=model
        self.step=step

    def log_record(self,prompt_text,output):
        logger=Logger()
        logger.log_conversation(prompt_text, "Human", self.step)
        logger.log_conversation(output, "AI", self.step)


def request(url,model,messages,temperature,top_p,n,key,**k):
    res = requests.post(
                url=
                url,
                json={
                    "model":
                    model,
                    "messages": messages,
                    "temperature":
                    temperature,
                    "top_p":top_p,
                    "n":n,
                    **k
                },
                headers={
                    "Authorization":
                    key
                }).json()

    return res

class TimeoutException(Exception):
    pass

class gpt_req(req):
    def __init__(self, step,model="gpt-4o") -> None:
        super().__init__(step, model)

        self.MODEL_PRICING = {
            "gpt-4o": {"input": 2.50, "output": 10.00},
            "gpt-4o-mini": {"input": 0.15, "output": 0.60},
            "gpt-4-turbo": {"input": 10.00, "output": 30.00},
            "gpt-4": {"input": 30.00, "output": 60.00},
            "gpt-3.5-turbo": {"input": 0.50, "output": 1.50},
            "gemini-2.5-pro": {"input": 1.25, "output": 10.00},  
            "gpt-5": {"input": 1.25, "output": 10.00},
            "claude-sonnet-4-20250514": {"input":3.00, "output":15.00},
            "gpt-5-codex": {"input":0.73, "output":5.84},
            "qwen3-coder-plus": {"input":0.6, "output":2.4}
        }

    
    
    def parse_action_from_response(self, response: str):
        """
        提取最后一个ActionInput中的函数调用
        格式：execute_sql(sql="...")  get_column_cardinalities 配合 JSON 格式的 ActionInput
        """
        
        # 找到所有的ActionInput块
        action_input_pattern = r'ActionInput:\s*(.+?)(?=\nObservation:|\nThink:|\nAction:|\nFinal Answer:|$)'
        matches = re.findall(action_input_pattern, response, re.DOTALL | re.IGNORECASE)
        
        if not matches:
            return None, None
        
        # 取最后一个ActionInput
        last_action_input = matches[-1].strip()

        # 找到对应的Action
        action_pattern = r'Action:\s*(\w+)'
        action_matches = re.findall(action_pattern, response, re.IGNORECASE)
        
        if not action_matches:
            return None, None
        
        last_action = action_matches[-1].strip()

        # 根据action类型解析ActionInput
        if last_action.lower() == 'execute_sql':
            execute_sqls = extract_sql_from_text(last_action_input)
            if execute_sqls:
                return 'execute_sql', execute_sqls[-1]
        
        elif last_action.lower() == 'get_column_cardinalities':
            try:
                # 尝试解析JSON格式的ActionInput
                column_pairs = extract_json_from_text(last_action_input) # 这里是list
                return 'get_column_cardinalities', column_pairs[-1] if len(column_pairs)>0 else []
            except json.JSONDecodeError:
                print(f"无法解析JSON格式的ActionInput: {last_action_input}")
                return None, None
        
        return None, None
    

    def get_column_cardinalities(self, column_pairs: list, fd_list: list) -> str:
        """
        检查列之间的基数关系
        Args:
            column_pairs: 列对的列表，格式如 [["table1.col1:table2.col2"], ...]
        Returns:
            基数关系的描述字符串
        """
        results = []

        for column_pair in column_pairs:
            if len(column_pair) == 2:
                table1, column1 = column_pair[0].split('.')
                table2, column2 = column_pair[1].split('.')
                flag = False
                if table1 == table2:
                    for knowledge in fd_list:
                        if column_pair[0].replace('`', '') in knowledge.replace('`','') and column_pair[1].replace('`', '') in knowledge.replace('`',''):
                            results.append(knowledge+'\n')
                            flag = True
                            break
                    if not flag:
                        m_n_knowledge = f"The relationship from {table1}.{column1} to {table2}.{column2} is N:M, indicating that multiple items with different {table1}.{column1} values can belong to the different {table2}.{column2} value."
                        results.append(m_n_knowledge)        
                else:
                    results.append(f"Invalid pair format: {column_pair}. The cardinality relationship between two columns must in the same table.")
            else:
                results.append(f"Invalid pair format: {column_pair}. There must be two columns.")

        return "\n".join(results)


    def get_ans(self, messages, temperature=0.0, n=1, top_p=None, single=True, **k):
        count = 0  
        while count < 5:  # 重试5次，仅仅是发送消息的时候
            # print(messages) #保存prompt和答案
            try: 
                res = request(
                url="https://www.dmxapi.com/v1/chat/completions",
                model=self.model,
                messages=messages,
                temperature=temperature,
                top_p=top_p,
                n=n,
                key=os.getenv('OPENAI_API_KEY'),
                **k)

                if n==1 and single:
                    response_clean = res["choices"][0]["message"]["content"]
                else:
                    response_clean = res["choices"]

                if self.step != "prepare_train_queries":  #TODO 暂时不知道这个函数是干嘛的
                    self.log_record(messages, response_clean)  # 记录对话内容
                break

            except Exception as e:
                print('llm message发送连接失败')
                count += 1
                time.sleep(2)
                print(e)
                

        usage = res['usage']
        input_tokens = usage['prompt_tokens']
        output_tokens = usage['completion_tokens']
        model_key = self.model.lower()
        if model_key in self.MODEL_PRICING:
            pricing = self.MODEL_PRICING[model_key]
            input_cost = (input_tokens / 1_000_000) * pricing["input"]
            output_cost = (output_tokens / 1_000_000) * pricing["output"]
            total_cost = input_cost + output_cost
            self.Cost += total_cost
        else:
            # 如果模型不在价格表中，使用默认价格或设为0
            print(f"警告: 未找到模型 {self.model} 的价格信息")

        return response_clean
    

    def get_ans_with_tool(self, messages, fd_list, sqlite_dir, execute_history, max_iterations=6, temperature=0.0, top_p=None, n=1,single=True, **k):
        """
        使用ReAct格式的工具调用
        """
        current_messages = copy.deepcopy(messages)
        iteration = 0
        while iteration < max_iterations:
            # 注意：这里不使用tools参数，让模型纯文本输出
            response = self.get_ans(current_messages)
            
            print(f"=== 迭代 {iteration + 1} ===")
            print("模型输出：")
            print(response)
            
            # 检查是否包含Final Answer
            if "Final Answer:" in response:
                return response
                
            # 解析Action和ActionInput
            action_type, action_input = self.parse_action_from_response(response)
            
            if action_type and action_input:
                if action_type == "execute_sql":
                    # 执行SQL
                    result = execute_sql(action_input.strip(), sqlite_dir, execute_history)
                    observation = result

                    if result[0] == 'Execute Empty':
                        observation = f"""{observation}
The SQL query returns `Empty` result, you should consider whether there is a problem below.
(1) Data Format Error: The values in the question have not been converted to the same format as the values in the database.
(2) Value Error: First, use case-insensitive fuzzy matching (e.g., `LOWER`,`LIKE`) to broaden the search and retrieve a subset of potential values. Then, within this subset, use a strict method (e.g., `=`) to localize and identify the single correct value that best matches the user's intent."""
                    elif result[0] == 'Execute None':
                        observation = f"""{observation}
The SQL query returns `None` result, you should consider whether there is a problem below, and adjust your answer to return valid results.
(1) **Logical error:** Follow the SQL skeleton provided in the example, you should try another reasoning path.
(2) **Exception handling:** Do not introduce additional filters to exclude outliers in order to avoid returning `None` result, unless the question explicitly instructs you to do so.
"""
                    print(f"执行SQL: {action_input}")
                    print(f"观察结果: {observation}")
                
                elif action_type == 'get_column_cardinalities':
                    result = self.get_column_cardinalities(action_input, fd_list)
                    observation = result

                    print(f"检查基数关系: {action_input}")
                    print(f"观察结果: {result}")
                
                else:
                    result = f"Unknown action type: {action_type}"
                    observation = result

                    print(f"未知操作类型: {action_type}")
                # 将观察结果添加到消息中
                updated_response = response + f"\nObservation: {observation}"
                current_messages.append({"role": "assistant", "content": updated_response})
                
                # 继续对话，让模型基于观察结果继续推理
                continue_prompt = "Based on the observation above, continue your reasoning. What should you do next?"
                current_messages.append({"role": "user", "content": continue_prompt})
                
            else:
                # 将当前响应添加到消息历史中
                current_messages.append({"role": "assistant", "content": response})
                
                # 提示模型使用正确的格式
                format_prompt = "Please follow the ReAct format: use 'Action: <TOOL_NAME>' and 'ActionInput:' for tool calls, or provide 'Final Answer:' for the final response."
                current_messages.append({"role": "user", "content": format_prompt})
                        
            iteration += 1
        
        # 如果达到最大迭代次数，进行最后一次调用要求给出最终答案
        final_prompt = "Please provide your Final Answer now based on all the observations above."
        current_messages.append({"role": "user", "content": final_prompt})
        final_response = self.get_ans(current_messages)
        
        return final_response
    

class sft_req(req):

    def __init__(self,model) -> None:
        super().__init__(model)
        self.device = "cuda:0"
        self.tokenizer = AutoTokenizer.from_pretrained(
            "",
            trust_remote_code=True,
            padding_side="right",
            use_fast=True)
        self.tokenizer.pad_token = self.tokenizer.eos_token = "<|EOT|>"
        # drop device_map if running on CPU
        self.model = AutoModelForCausalLM.from_pretrained(
            "",
            torch_dtype=torch.bfloat16,
            device_map=self.device).eval()

    def get_ans(self, text, temperature=0.0):
        messages = [{
            "role":
            "system",
            "content":
            "You are an AI programming assistant, utilizing the DeepSeek Coder model, developed by DeepSeek Company, and you only answer questions related to computer science. For politically sensitive questions, security and privacy issues, and other non-computer science questions, you will refuse to answer."
        }, {
            "role": "user",
            "content": text
        }]
        inputs = self.tokenizer.apply_chat_template(messages,
                                                    add_generation_prompt=True,
                                                    tokenize=False)
        model_inputs = self.tokenizer([inputs],
                                      return_tensors="pt",
                                      max_length=8000).to("cuda")
        # tokenizer.eos_token_id is the id of <|EOT|> token
        generated_ids = self.model.generate(
            model_inputs.input_ids,
            attention_mask=model_inputs["attention_mask"],
            max_new_tokens=800,
            do_sample=False,
            eos_token_id=self.tokenizer.eos_token_id,
            pad_token_id=self.tokenizer.pad_token_id)
        generated_ids = [
            output_ids[len(input_ids):] for input_ids, output_ids in zip(
                model_inputs.input_ids, generated_ids)
        ]

        response = self.tokenizer.decode(generated_ids[0][:-1],
                                         skip_special_tokens=True).strip()
        return response





