'''
1. 输入：每个sample中带有原始ddl，以及后续添加的取值范围，Example示例
   输出：每个sample中除了输入带有的东西外，还有列名释义、列类型格式、表描述；

2. 这个算法可以有两种实现：
 （1）在列类型格式/表描述确定的情况下，也就是bird dev的情况下。当前我已经为bird dev生成了列类型释义和表描述，为了节省token和为了保持跟之前的结果保持一致，选择直接抽取出这部分。
 （2）在列类型格式/表描述不确定的情况下，也就是bird test的情况下。重新开始生成所需的所有内容。
'''

import argparse
import json
import os
import sqlite3
from openai import OpenAI
from tqdm import tqdm
from typing import Dict, List, Tuple, Any, Optional
import re
import pandas as pd

from database_util import *

def get_col_desc_from_variants(column_desc, column_name):
    variants = [
        column_name,
        column_name.replace('`', ''),
        column_name.lower().replace('`', ''),
        column_name.upper().replace('`', ''),
        column_name.capitalize().replace('`', ''),
    ]
    for var in variants:
        if var in column_desc:
            return column_desc[var]
    return None

def fill_table_desc_and_column_format(samples, table_desc_dict, meaning_file):
    """吧column meaning 也加到table_desc_dict进去"""
    column_meaning_dict = load_json_file(meaning_file) if meaning_file != "" else []

    for sample in samples:
        db_id = sample['db_id']
        table_desc = table_desc_dict[db_id]
        origin_desc = sample['db_desc']  # 只有example
        table_text_list = split_ddl(origin_desc)  # 拆分成每个table

        new_table_desc_list = []
        for table_text in table_text_list:

            # 提取表名 - 匹配 CREATE TABLE 后的表名（可能带反引号）
            table_match = re.search(r'CREATE TABLE\s+(`?[\w\s]+`?)\s*\(', table_text, re.IGNORECASE)
            if not table_match:
                return
            
            table_name = table_match.group(1).strip()
            #print(f">>> 提取表名: {table_name}")

            # 分割成行来处理每个列定义
            lines = table_text.split('\n')
            new_lines = []
            for line in lines:
                line = line.strip()
                
                # 跳过空行、CREATE TABLE行和注释行，最后一行
                if not line or line.startswith('CREATE TABLE') or line.startswith('--') or line.startswith(')'):
                    new_lines.append(line)
                    continue
                
                # 匹配列定义：列名 数据类型 ... -- 注释内容
                # 列名可能是普通单词或反引号包裹的多个单词
                column_match = re.match(r'(`[^`]+`|\w+)\s+\w+', line)
                
                if column_match:
                    column_name = column_match.group(1)
                    # print(f"    >> DDL字段行: {line}")
                    # print(f"    >> 提取column_name: {column_name}")
                    if column_name in ["PRIMARY", "TABLE", "CONSTRAINT"]:
                        new_lines.append(line)
                        continue
                    
                    # print(f"table_name={table_name},colunm_name={column_name}")
                    # print('table_desc.keys:', table_desc.keys())
                    # print('table_name:', table_name, repr(table_name))
                    # print("column_desc.keys:", table_desc[table_name]['column_desc'].keys())
                    # print('column_name:', column_name, repr(column_name))
                    format = table_desc[table_name]['column_desc'][column_name]['Format']
                                        
                    try:
                        column_full_name = f"{db_id}|{table_name.replace('`', '')}|{column_name.replace('`', '')}"
                        column_description = column_meaning_dict[column_full_name]
                        line = f"{line}; Description: {column_description} Format:{format}"
                    except:
                        line = f"{line}; Format:{format}"
                    try:
                        range = table_desc[table_name]['column_desc'][column_name]['Range']
                        line = f"{line}; Data Range: {range}"
                    except:
                        pass

                    new_lines.append(line)

            new_table_desc_text = table_desc[table_name]['table_desc'] + '\n'.join(new_lines)        
            new_table_desc_list.append(new_table_desc_text)

        new_database_desc = '\n'.join(new_table_desc_list)   
        sample['db_desc_info'] = new_database_desc
        sample['fd_list'] = table_desc['fd_list']
        sample['consistency_redundant_columns'] = table_desc['consistency_redundant_columns']
        sample['inconsistency_redundant_columns'] = table_desc['inconsistency_redundant_columns']
        #sample['null_column'] = table_desc['null_column']
    
    return



def construct_table_desc_info(samples, db_path, table_desc_file):
    table_desc_dict = {}
    
    for data in tqdm(samples): 
        if data['db_id'] not in table_desc_dict:
            # 1. 得到表描述和列格式类型描述
            database_path = os.path.join(db_path, data['db_id'], data['db_id']+'.sqlite')            
            # 添加列格式描述
            print("正在处理列 ============================")
            #print(f"{data['db_desc']}")
            new_db_column_desc = enhance_schema_with_column_description(data['db_desc'], database_path)
            # print("列处理后的结果 =========================")         去掉打印
            # print(new_db_column_desc)
            # 添加表描述
            print("正在处理表 ============================")
            status = 'Fail'
            MAX_TRY_TIME = 3
            while status == 'Fail' and MAX_TRY_TIME > 0:
                status, new_db_table_desc = enhance_schema_with_table_description(new_db_column_desc)
                MAX_TRY_TIME -= 1
            if MAX_TRY_TIME == 0:
                raise ValueError(f"经过3次尝试，{data['db_id']}数据库还是不能生成正常的table描述。")

            # 2. new_db_table_desc 需要重新解析保存，必须要做的，因为llm生成的是text
            table_text_list = split_ddl(new_db_table_desc)  # 拆分成每个table，table上还有注释的表描述            
            database_result = {}
            for table_text in table_text_list:
                table_desc, ddl_text = table_text.split('CREATE')    
                ddl_text += 'CREATE' + ddl_text
    
                # 提取表名 - 匹配 CREATE TABLE 后的表名（可能带反引号）
                table_match = re.search(r'CREATE TABLE\s+(`?[\w\s]+`?)\s*\(', ddl_text, re.IGNORECASE)
                if not table_match:
                    continue
                
                table_name = table_match.group(1).strip()
                database_result[table_name] = {}
                database_result[table_name]['table_desc'] = table_desc
                database_result[table_name]['column_desc'] = {}
                
                # 分割成行来处理每个列定义
                lines = ddl_text.split('\n')
                
                for line in lines:
                    line = line.strip()
                    
                    # 跳过空行、CREATE TABLE行和注释行
                    if not line or line.startswith('CREATE TABLE') or line.startswith('--'):
                        continue
                    
                    # 匹配列定义：列名 数据类型 ... -- 注释内容
                    # 列名可能是普通单词或反引号包裹的多个单词
                    column_match = re.match(r'(`[^`]+`|\w+)\s+\w+', line)
                    
                    if column_match:
                        column_name = column_match.group(1)

                        if column_name in ["PRIMARY", "TABLE", "CONSTRAINT"]:
                            continue

                        # with open("log_lsx.txt", "a", encoding="utf-8") as logf:
                        #     logf.write(f"debug: before format: {repr(column_match.group(1))}\n")
                        column_name = format_table_column_name(column_name)
                        # with open("log_lsx.txt", "a", encoding="utf-8") as logf:
                        #     logf.write(f"debug: after format: {repr(column_name)}\n")
                        # 3. 得到column的列格式描述
                        format_match = re.search(r'--.*?Format:\s*(.+?)(?:\s--\s|$)', line)

                        if format_match:
                            format_text = format_match.group(1).strip()
                            database_result[table_name]['column_desc'][column_name] = {"Format": format_text}
                        else:
                            # 如果没有Format信息，也添加该列，但Format为空
                            database_result[table_name]['column_desc'][column_name] = {"Format": ""}
                        
                        # 4. 得到column的取值范围描述
                        # 匹配模式：column_name type, -- comment, example: [...]
                        #field_match = re.match(r'(\w+)\s+(integer|real|text|date)\s*,?\s*--\s*(.+)', line, re.IGNORECASE)   修改9
                        field_match = re.match(r'(\w+)\s+(integer|real|text|date)\b(?:\s+[^,\-\-]+)*\s*,?\s*--\s*(.+)', line, re.IGNORECASE)
                        if field_match:
                            column_type = field_match.group(2).lower()
                            should_add_range = (
                                                column_type in ['integer', 'real'] or  # 数值类型
                                                column_type == 'date' or  # 日期类型
                                                (column_type == 'text' and is_date_column(column_name))  # 可能是日期的文本字段
                                            )
                            if should_add_range:
                                # 获取数据范围
                                min_val, max_val = get_data_range(database_path, table_name, column_name, column_type)

                                if min_val is not None and max_val is not None:
                                    # 格式化范围值
                                    if column_type in ['integer', 'real']:
                                        range_str = f"[{min_val}, {max_val}]"
                                    else:
                                        # 日期类型，保持原格式
                                        range_str = f"['{min_val}', '{max_val}']"
                                    database_result[table_name]['column_desc'][column_name]['Range'] = range_str
             
            # 更新数据库信息
            table_desc_dict[data['db_id']] = database_result

            with open(table_desc_file, 'w', encoding='utf-8') as desf:
                json.dump(table_desc_dict, desf, ensure_ascii=False, indent=2)

            # print("总输人：===========================")         去掉打印
            # print(data['db_desc'])
            # print("总输出：===========================")
            # print(new_db_table_desc)
    
    # 保存table desc文件        
    print(f'finish saving table desc file to {table_desc_file}')
    

    # 3. 再得到fd_list和similar column
    get_db_fd_list_and_column_info(table_desc_dict, opt)
    get_similar_column_by_score(opt.meaning_file,table_desc_file)           #这里添加跳过逻辑，如果meaningfile不存在就跳过
    return table_desc_dict


def run_construct(opt):  

    if os.path.exists(opt.output_file):
        print('--- 已存在包含 table info 的 example，即bird_metadata，跳过') 

    else: 
        origin_datas = load_json_file(opt.input_file)
        intemediate_datas = load_json_file(opt.intermediate_file)

        for o_data, i_data in zip(origin_datas, intemediate_datas):
            i_data['db_id'] = o_data['db_id']
            i_data['question_id'] = o_data['question_id']
            i_data['difficulty'] = o_data['difficulty']
        # 加载已经存在的表/列描述
        table_desc_dict = {}
        if os.path.exists(opt.table_desc_file):
            with open(opt.table_desc_file, 'r', encoding='utf-8') as f:
                table_desc_dict = json.load(f)

        if opt.mode == 'dev':
            if not os.path.exists(opt.table_desc_file): # 可能提交的时候把我的文件删了，那我需要重新生成。
                table_desc_dict = construct_table_desc_info(intemediate_datas, opt.db_path, opt.table_desc_file)
            else:
                print(f'成功加载处理好的{opt.table_desc_file}')

            fill_table_desc_and_column_format(intemediate_datas, table_desc_dict, opt.meaning_file)  # 只需要将现在table_desc中的列格式和表描述内容填入就好了。
        
        elif opt.mode == 'test':
            #table_desc_dict = construct_table_desc_info(intemediate_datas, opt.db_path, opt.table_desc_file)        lsx后修改
            if not os.path.exists(opt.table_desc_file): # 可能提交的时候把我的文件删了，那我需要重新生成。
                table_desc_dict = construct_table_desc_info(intemediate_datas, opt.db_path, opt.table_desc_file)
            else:
                print(f'成功加载处理好的{opt.table_desc_file}')
            fill_table_desc_and_column_format(intemediate_datas, table_desc_dict, opt.meaning_file)
        
        else:
            raise ValueError(f"Error mode: '{opt.mode}'. Your mode must be 'dev' or 'test'.")
        
         # === 加载 bird_{mode}.json 并加入 example 字段 ===
        bird_mode_json_path = os.path.join(os.path.dirname(opt.output_file), f"bird_{opt.mode}.json")
        assert os.path.exists(bird_mode_json_path), f"{bird_mode_json_path} does not exist!"
        with open(bird_mode_json_path, "r", encoding="utf-8") as f:
            examples_list = json.load(f)
        assert len(examples_list) == len(intemediate_datas), \
            f"examples({len(examples_list)}) and samples({len(intemediate_datas)})数量不符"
        for sample, ex in zip(intemediate_datas, examples_list):
            sample["example"] = ex["example"]
        # 保持sample文件
        with open(opt.output_file, 'w', encoding='utf-8') as f:
            json.dump(intemediate_datas, f, ensure_ascii=False, indent=2)

        print(f'finish saving file to {opt.output_file}')

    



if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, choices=['dev', 'test'], default="dev")
    parser.add_argument("--input_file", type=str, help="samples的最初文件位置", default='/media/hnu/hnu2024/wangqin/python_work/Text2SQL_SUBMIT_COPY/output/bird/dev.json')
    parser.add_argument("--intermediate_file", type=str, help="samples已经包含example的文件位置",)    
    parser.add_argument("--output_file", type=str, help="用来生成处理后的samples的位置", default='../output/bird/dev/dev_bird_metadata.json')
    parser.add_argument("--meaning_file", type=str, help="column meaning的位置, 如果不提供就不加", default="/media/hnu/hnu2024/wangqin/python_work/Text2SQL_SUBMIT_COPY/output/bird/TA-SQL/column_meaning.json")
    parser.add_argument("--table_desc_file", type=str, help="用来存储表描述和列格式", default='../output/bird/dev/table_desc.json')
    parser.add_argument("--db_path", type=str, help="存放数据库的位置", default='/media/hnu/hnu2024/wangqin/python_work/Text2SQL_SUBMIT_COPY/datasets/bird/dev/dev_databases')
    parser.add_argument("--model_name", type=str, help="模型名称", default='gpt-4o')

    opt = parser.parse_args()

    print(opt)

    run_construct(opt)
