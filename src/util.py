import json
import os
import re
import sqlite3
from typing import Any, Dict, List, Set, Tuple
from itertools import combinations, permutations
import signal
from contextlib import contextmanager

from func_timeout import FunctionTimedOut, func_timeout
import sqlglot

def get_last_node_result(execution_history: List[Dict[str, Any]], node_type: str) -> Dict[str, Any]:
    """
    Retrieves the last result for a specific node type from the execution history.

    Args:
        execution_history (List[Dict[str, Any]]): The execution history.
        node_type (str): The type of node to look for.

    Returns:
        Dict[str, Any]: The result of the last node of the specified type, or None if not found.
    """
    for node in reversed(execution_history):
        if node["node_type"] == node_type:
            return node
    return None

def extract_sql_from_text(text: str) -> List[str]:
    """
    从文本中提取SQL语句     
    Args:
        text: 包含SQL的文本
    Returns:
        提取出的SQL语句列表
    """
    # 匹配 ```sql 和 ``` 之间的内容
    sql_pattern = r'```sql\s*(.*?)\s*```'
    sql_matches = re.findall(sql_pattern, text, re.DOTALL | re.IGNORECASE)
    
    # 清理提取的SQL语句
    cleaned_sqls = []
    for sql in sql_matches:
        # 去除多余的空白字符
        cleaned_sql = sql.strip()
        if cleaned_sql:
            cleaned_sqls.append(cleaned_sql)
    
    return cleaned_sqls

def extract_rule_from_text(text: str) -> List[str]:
    """
    从文本中提取SQL语句     
    Args:
        text: 包含SQL的文本
    Returns:
        提取出的SQL语句列表
    """
    # 匹配 ```sql 和 ``` 之间的内容
    sql_pattern = r'```text\s*(.*?)\s*```'
    sql_matches = re.findall(sql_pattern, text, re.DOTALL | re.IGNORECASE)
    
    # 清理提取的SQL语句
    cleaned_texts = []
    for sql in sql_matches:
        # 去除多余的空白字符
        cleaned_sql = sql.strip()
        if cleaned_sql:
            cleaned_texts.append(cleaned_sql)
    
    return cleaned_texts

def extract_json_from_text(text: str) -> List[str]:
    """
    从文本中提取json语句
    
    Args:
        text: 包含json的文本
        
    Returns:
        提取出的json语句列表
    """
    # 匹配 ```sql 和 ``` 之间的内容
    json_pattern = r'```json\s*(.*?)\s*```'
    json_matches = re.findall(json_pattern, text, re.DOTALL | re.IGNORECASE)
    
    # 清理提取的json语句
    cleaned_jsons = []
    for json_ in json_matches:
        # 去除多余的空白字符
        cleaned_json = json_.strip()
        if cleaned_json:
            try:
                cleaned_json = json.loads(cleaned_json)
                cleaned_jsons.append(cleaned_json)
            except:
                print(f'json格式错误，不能转换: \n {cleaned_json}')

    return cleaned_jsons


def execute_sql(sql, sqlite_dir, execute_history: set):
    """
    SQL执行器
    Args:
        sql: sql str
    Returns:
        执行状态，执行结果
    """
    print("=========== enter tool ===========")
    if not sql:
        return "Execute Failed", "SQL statement is empty"

    def _execute_query():
        """内部执行函数"""
        conn = None
        try:
            conn = sqlite3.connect(sqlite_dir) 
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            return rows
        finally:
            if conn:
                conn.close()

    try:
        # 设置5秒超时
        rows = func_timeout(5, _execute_query)
        len_rows = len(rows)
        if rows == [(0,)]:  # COUNT == 0, 也是不合格的
            result = f"""The SQL statement:
    {sql}
    The execution returned `[(0,)]`. This is likely an invalid result of the aggregation operation.
    """ 
            execute_history.add(("Execute Empty", result))
            return "Execute Empty", result
        elif rows == [(None,)]:
            result = f"""The SQL statement:
    {sql}
    The execution returned `[(None,)]`. This is an invalid result.
    """ 
            execute_history.add(("Execute Empty", result))
            return "Execute None", result
        elif len_rows>0:
            min_len = min(len_rows, 8)
            result = f"""The SQL statement:
    {sql}
    The execution returned {len_rows} rows. 
    The {min_len}/{len_rows} rows is: 
    {rows[:8]}
    """ 
            execute_history.add(("Execute Success", result))
            return "Execute Success", result
        elif len_rows==0:
            result = f"""The SQL statement:
    {sql}
    The execution returned {len_rows} rows. 
    """ 
            execute_history.add(("Execute Empty", result))
            return "Execute Empty", result
    
    except FunctionTimedOut as e:
        result = f"SQL execution timeout."
        print(e)
        return "Execute Failed", result

    except sqlite3.Error as e:
        # 记录哪个数据库失败了
        result = f"failed: {e}"
        print(result)
        execute_history.add(("Execute Failed", result))
        return "Execute Failed", result
    except Exception as e:
        result = f"Unexpected error: {e}"
        print(result)
        execute_history.add(("Execute Failed", result))
        return "Execute Failed", result

def extract_filtered_ddl(ddl_content: str, target_columns: List[str], table_names_only: Set[str]) -> str:
    """
    从DDL中提取包含指定列的表结构，保持主键和外键约束
    
    Args:
        ddl_content: 原始DDL内容
        target_columns: 需要保留的列名列表
    
    Returns:
        过滤后的DDL字符串
    """
    
    # 解析DDL，提取表信息
    tables = parse_ddl(ddl_content)
    # 找出包含目标列的表
    relevant_tables = find_relevant_tables(tables, target_columns)
    relevant_tables_filter = relevant_tables & table_names_only
    
    # 构建过滤后的DDL
    filtered_ddl = build_filtered_ddl(tables, relevant_tables_filter, target_columns)
    
    return filtered_ddl

def parse_ddl(ddl_content: str) -> Dict:
    """解析DDL内容，提取表结构信息"""
    tables = {}
    
    # 使用正则表达式匹配CREATE TABLE语句
    # table_pattern = r'CREATE TABLE (\w+) \((.*?)\);'
    # 修改正则表达式以处理带反引号的表名，并处理可能的空格
    table_pattern = r'CREATE TABLE\s+(`?\w+`?)\s*\((.*?)\);'
    matches = re.findall(table_pattern, ddl_content, re.DOTALL)
    
    for table_name, table_content in matches:
        tables[table_name] = parse_table_content(table_content)
        tables[table_name]['name'] = table_name
    
    return tables

def parse_table_content(content: str) -> Dict:
    """解析单个表的内容"""
    lines = [line.strip() for line in content.split('\n') if line.strip()]
    
    columns = []
    primary_key = None
    foreign_keys = []
    
    for line in lines:
        line = line.rstrip(',')
        
        if line.startswith('PRIMARY KEY'):
            # 提取主键
            pk_match = re.search(r'PRIMARY KEY \(([^)]+)\)', line)
            if pk_match:
                primary_key = [col.strip() for col in pk_match.group(1).split(',')]
        
        elif line.startswith('CONSTRAINT') and 'FOREIGN KEY' in line:
            # 提取外键
            fk_match = re.search(r'FOREIGN KEY \(([^)]+)\) REFERENCES (\w+) \(([^)]+)\)', line)
            if fk_match:
                local_cols = [col.strip() for col in fk_match.group(1).split(',')]
                ref_table = fk_match.group(2)
                ref_cols = [col.strip() for col in fk_match.group(3).split(',')]
                foreign_keys.append({
                    'local_columns': local_cols,
                    'reference_table': ref_table,
                    'reference_columns': ref_cols,
                    'constraint_name': re.search(r'CONSTRAINT (\w+)', line).group(1)
                })
        
        elif not line.startswith('PRIMARY KEY') and not line.startswith('CONSTRAINT'):
            # 解析列定义
            column_info = parse_column_definition(line)
            if column_info:
                columns.append(column_info)
    
    return {
        'columns': columns,
        'primary_key': primary_key,
        'foreign_keys': foreign_keys
    }

def parse_column_definition(line: str) -> Dict:
    """解析列定义"""
    # 处理带反引号的列名
    if line.startswith('`'):
        # 找到列名（在反引号中）
        name_match = re.match(r'`([^`]+)`\s+(\w+)', line)
        if name_match:
            col_name = name_match.group(1)
            col_name = '`'+col_name+'`'
            data_type = name_match.group(2)
        else:
            return None
    else:
        # 普通列名
        parts = line.split()
        if len(parts) >= 2:
            col_name = parts[0]
            data_type = parts[1]
        else:
            return None
    
    return {
        'name': col_name,
        'type': data_type,
        'definition': line
    }

def find_relevant_tables(tables: Dict, target_columns: List[str]) -> Set[str]:
    """找出包含目标列的表"""
    relevant_tables = set()
    target_columns_lower = [col.lower() for col in target_columns]
    
    for table_name, table_info in tables.items():
        for column in table_info['columns']:
            if column['name'].lower() in target_columns_lower:
                relevant_tables.add(table_name)
                break
    
    return relevant_tables

def build_filtered_ddl(tables: Dict, relevant_tables: Set[str], target_columns: List[str]) -> str:
    """构建过滤后的DDL"""
    ddl_parts = []
    target_columns_lower = [col.lower() for col in target_columns]
    
    for table_name in relevant_tables:
        table_info = tables[table_name]
        ddl_parts.append(f"\nCREATE TABLE {table_name} (")
        
        # 收集需要保留的列
        kept_columns = []
        kept_column_names = set()
        
        # 首先添加目标列
        for column in table_info['columns']:
            if column['name'].lower() in target_columns_lower:
                kept_columns.append(column)
                kept_column_names.add(column['name'])
        
        # 添加主键列（如果不在目标列中）
        if table_info['primary_key']:
            for pk_col in table_info['primary_key']:
                if pk_col not in kept_column_names:
                    # 找到主键列的定义
                    for column in table_info['columns']:
                        if column['name'] == pk_col:
                            kept_columns.append(column)
                            kept_column_names.add(column['name'])
                            break
        
        # 添加外键列（如果不在已保留列中）
        for fk in table_info['foreign_keys']:
            for fk_col in fk['local_columns']:
                if fk_col not in kept_column_names:
                    # 找到外键列的定义
                    for column in table_info['columns']:
                        if column['name'] == fk_col:
                            kept_columns.append(column)
                            kept_column_names.add(column['name'])
                            break
        
        # 生成列定义
        for i, column in enumerate(kept_columns):
            ddl_parts.append(f"    {column['definition']},")
        
        # 添加主键约束
        if table_info['primary_key']:
            pk_cols = ', '.join(table_info['primary_key'])
            ddl_parts.append(f"    PRIMARY KEY ({pk_cols}),")
        
        # 添加外键约束（只有当引用的表也在相关表中时）
        for fk in table_info['foreign_keys']:
            if fk['reference_table'] in relevant_tables:
                local_cols = ', '.join(fk['local_columns'])
                ref_cols = ', '.join(fk['reference_columns'])
                ddl_parts.append(f"    CONSTRAINT {fk['constraint_name']} FOREIGN KEY ({local_cols}) REFERENCES {fk['reference_table']} ({ref_cols}),")
        
        # 移除最后一个逗号
        if ddl_parts[-1].endswith(','):
            ddl_parts[-1] = ddl_parts[-1][:-1]
        
        ddl_parts.append(");")
    
    return '\n'.join(ddl_parts)


def format_table_column_name(name_list:List[str]) -> Set[str]:
    '''格式化table和column的名称，有时候会少了`'''

    new_name_list = []
    for name in name_list:
        name = name.replace('"', '`')
        if '`' not in name:
            new_name_list.append(name)
            new_name_list.append('`'+name+'`')
        else:
            new_name_list.append(name)
            new_name_list.append(name.replace('`', ''))
    
    return set(new_name_list)



def process_redundant_columns(columns_used, table_names_only, consistency_redundant_columns, inconsistency_redundant_columns):
    """
    处理冗余列，将匹配的列和表名添加到现有集合中
    
    Args:
        columns_used (set): 当前使用的列名集合
        table_names_only (set): 当前使用的表名集合
        consistency_redundant_columns (list): 一致性冗余列列表
        inconsistency_redundant_columns (list): 不一致性冗余列列表
    
    Returns:
        tuple: 更新后的 (columns_used, table_names_only)
    """
    
    def remove_symbols(text):
        """去除所有引号和反引号符号"""
        return text.replace('"', '').replace('`', '')
    
    # 复制原始集合
    updated_columns = columns_used.copy()
    updated_tables = table_names_only.copy()
    
    # 创建当前表列组合（去除符号）
    current_combinations = set()
    for table in table_names_only:
        for column in columns_used:
            clean_table = remove_symbols(table)
            clean_column = remove_symbols(column)
            current_combinations.add(f"{clean_table}.{clean_column}")
    
    # 处理所有冗余列
    all_redundant_columns = consistency_redundant_columns + inconsistency_redundant_columns
    
    for redundant_group in all_redundant_columns:
        if len(redundant_group) >= 2:
            # 检查第一个和第二个位置的元素
            first_clean = remove_symbols(redundant_group[0])
            second_clean = remove_symbols(redundant_group[1])
            
            # 如果当前组合匹配第一个或第二个位置
            if first_clean in current_combinations or second_clean in current_combinations:
                # 将整个冗余组的表名和列名都加入
                for redundant_item in redundant_group:
                    clean_item = remove_symbols(redundant_item)
                    if '.' in clean_item:
                        table_part, column_part = clean_item.split('.', 1)
                        # 添加表名和列名（如果不存在）
                        if table_part not in updated_tables:
                            updated_tables.add(table_part)
                        if column_part not in updated_columns:  # 保持原有格式
                            updated_columns.add(column_part)
    
    return updated_columns, updated_tables



def get_filter_schema_from_sqls(schema_sqls, schema_info_sqls, db_desc):
    sqls = schema_sqls + schema_info_sqls
    filter_column_set = set()
    table_names_only_set = set()
    filtered_ddl = ""
    for sql in sqls:
        try:
            expression = sqlglot.parse_one(sql, dialect='sqlite')
            columns = expression.find_all(sqlglot.exp.Column)

            columns_used = set(str(col) for col in columns)
            table_names_only = set()
            for table in expression.find_all(sqlglot.exp.Table):
                # 获取表的基本名称，不包含schema
                table_names_only.add(table.name)
            for alias in expression.find_all(sqlglot.exp.TableAlias):
                if alias.this and hasattr(alias.this, 'name'):
                    table_names_only.add(alias.this.name)

            filter_column_list = []
            for t_column in columns_used:
                try:
                    _, t_column = t_column.replace('"', '`').split('.')
                except:
                    t_column = t_column.replace('"', "`")
                filter_column_list.append(t_column)
            # 检查一些字段命名是否合理
            filter_column_set = filter_column_set | format_table_column_name(filter_column_list)
            table_names_only_set = table_names_only_set | format_table_column_name(table_names_only)
            # print("过滤所有涉及的列：", filter_column_list)
            # print("过滤所有涉及的表：", table_names_only)

            # 提取过滤后的DDL
            filtered_ddl = extract_filtered_ddl(db_desc, filter_column_list, table_names_only)
        except:
            pass

    if filtered_ddl == '':
        filtered_ddl = db_desc

    return filtered_ddl