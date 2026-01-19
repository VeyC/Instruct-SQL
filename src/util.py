import json
import os
import re
import sqlite3
from typing import Any, Dict, List, Set, Tuple
from itertools import combinations, permutations
import signal
from contextlib import contextmanager

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

class TimeoutException(Exception):
    pass

def timeout_handler(signum, frame):
    raise TimeoutException("SQL execution timeout")

@contextmanager
def timeout_context(seconds):
    # 设置信号处理器
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)  # 取消定时器


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

    conn = None
    try:
        with timeout_context(30):
            conn = sqlite3.connect(sqlite_dir)
            print(f'connect to {sqlite_dir} ----')
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
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
    
    except TimeoutException as e:
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
    finally:
        # 确保连接被关闭
        if conn:
            conn.close()

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


def extract_sql_keywords_predefined(text):
    """使用预定义的SQL关键字列表进行匹配"""
    comprehensive_sql_keywords = {
        # 查询语句
        'GROUP BY', 'ORDER BY', 'HAVING',
        # 连接类型
        'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN', 'FULL OUTER JOIN', 
        'CROSS JOIN', 'NATURAL JOIN', 'JOIN',
        # 逻辑操作符
        'AND', 'OR', 'NOT',
        # 比较和条件操作符
        'IN', 'EXISTS', 'BETWEEN', 'LIKE', 'IS', 'NULL', 'ISNULL', 'NOTNULL',
        # 聚合函数
        'COUNT', 'SUM', 'AVG', 'MIN', 'MAX',
        # 类型转换函数
        'CAST', 'CONVERT',
        # 数据类型
        'REAL', 'INTEGER', 'INT', 'VARCHAR', 'CHAR', 'TEXT', 'DECIMAL', 
        'FLOAT', 'DOUBLE', 'BOOLEAN', 'DATE', 'TIME', 'DATETIME', 'TIMESTAMP',
        # 排序和限制
        'DESC', 'ASC', 'LIMIT', 'OFFSET', 'TOP',
        # 集合操作
        'UNION', 'UNION ALL', 'INTERSECT', 'EXCEPT', 'MINUS',
        # 别名和修饰符
        'DISTINCT', 'ALL',
        # 条件语句
        'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
        # 其他函数和关键字
        'COALESCE', 'NULLIF', 'ISNULL', 'LEN', 'LENGTH', 'SUBSTRING', 'TRIM',
        'UPPER', 'LOWER', 'CONCAT', 'REPLACE', 'ROUND', 'FLOOR', 'CEILING',
    }
    
    found_keywords = set()
    
    for keyword in comprehensive_sql_keywords:
        if ' '+keyword+' ' in text:
            found_keywords.add(keyword)
    
    return sorted(found_keywords)


def format_table_column_name(name_list:List[str]) -> Set[str]:
    '''格式化table和column的名称，有时候会少了`'''
    sqlite_keywords = {
        "ABORT", "ACTION", "ADD", "AFTER", "ALL", "ALTER", "ANALYZE", "AND",
        "AS", "ASC", "ATTACH", "AUTOINCREMENT",
        "BEFORE", "BEGIN", "BETWEEN", "BY",
        "CASCADE", "CASE", "CAST", "CHECK", "COLLATE", "COLUMN", "COMMIT",
        "CONFLICT", "CONSTRAINT", "CREATE", "CROSS", "CURRENT_DATE",
        "CURRENT_TIME", "CURRENT_TIMESTAMP",
        "DATABASE", "DEFAULT", "DEFERRABLE", "DEFERRED", "DELETE", "DESC",
        "DETACH", "DISTINCT", "DROP",
        "EACH", "ELSE", "END", "ESCAPE", "EXCEPT", "EXCLUSIVE", "EXISTS",
        "EXPLAIN",
        "FAIL", "FOR", "FOREIGN", "FROM", "FULL",
        "GLOB", "GROUP",
        "HAVING",
        "IF", "IGNORE", "IMMEDIATE", "IN", "INDEX", "INITIALLY", "INNER",
        "INSERT", "INSTEAD", "INTERSECT", "INTO", "IS", "ISNULL",
        "JOIN",
        "KEY",
        "LEFT", "LIKE", "LIMIT",
        "MATCH",
        "NATURAL", "NO", "NOT", "NOTNULL", "NULL",
        "OF", "OFFSET", "ON", "OR", "ORDER", "OUTER",
        "PLAN", "PRAGMA", "PRIMARY", "QUERY",
        "RAISE", "RECURSIVE", "REFERENCES", "REGEXP", "REINDEX", "RELEASE",
        "RENAME", "REPLACE", "RESTRICT", "RIGHT", "ROLLBACK", "ROW",
        "SAVEPOINT", "SELECT", "SET",
        "TABLE", "TEMP", "TEMPORARY", "THEN", "TO", "TRANSACTION", "TRIGGER",
        "UNION", "UNIQUE", "UPDATE", "USING",
        "VACUUM", "VALUES", "VIEW", "VIRTUAL",
        "WHEN", "WHERE", "WITH", "WITHOUT"
    }

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


def extract_primary_keys_from_ddl(ddl_content: str) -> List[str]:
    """
    从DDL内容中提取所有主键信息
    返回格式为 table_name.column_name 的列表
    """
    primary_keys = []
    
    # 匹配PRIMARY KEY的正则表达式，支持多种格式
    # 1. PRIMARY KEY (column_name)
    # 2. column_name PRIMARY KEY
    primary_key_patterns = [
        r'PRIMARY\s+KEY\s*\(\s*([^)]+)\s*\)',  # PRIMARY KEY (column_name) 或 PRIMARY KEY (col1, col2)
        r'(\w+)[^,]*?PRIMARY\s+KEY'  # column_name ... PRIMARY KEY
    ]
    
    # 按CREATE TABLE分割内容
    tables = re.split(r'CREATE\s+TABLE', ddl_content, flags=re.IGNORECASE)
    
    for table_section in tables[1:]:  # 跳过第一个空部分
        # 提取表名
        table_match = re.search(r'^\s+(\w+)\s*\(', table_section, re.IGNORECASE)
        if not table_match:
            continue
            
        table_name = table_match.group(1)
        
        # 查找PRIMARY KEY定义
        for pattern in primary_key_patterns:
            matches = re.findall(pattern, table_section, re.IGNORECASE)
            for match in matches:
                # 处理多个列的情况（逗号分隔）
                if ',' in match:
                    columns = [col.strip('` ') for col in match.split(',')]
                else:
                    columns = [match.strip('` ')]
                
                for column in columns:
                    column = column.strip()
                    if column:  # 确保不是空字符串
                        primary_keys.append(f"`{table_name}`.`{column}`")
    
    return primary_keys



def extract_columns_from_ddl(ddl_text):
    """
    从DDL文本中提取所有表的列信息
    返回字典，key是表名，value是该表的列名列表
    """
    # 分割DDL为单独的表定义
    tables = re.split(r'CREATE TABLE', ddl_text)[1:]  # 跳过第一个空元素
    
    result = {}
    
    for table in tables:
        # 提取表名
        lines = table.strip().split('\n')
        first_line = lines[0].strip()
        table_name = first_line.split()[0].rstrip('(').strip()
        
        columns = []
        
        # 找到列定义部分
        for line in lines:
            line = line.strip()
            
            # 跳过空行、PRIMARY KEY、CONSTRAINT等非列定义行
            if (not line or 
                line.startswith('PRIMARY KEY') or 
                line.startswith('CONSTRAINT') or
                line.startswith(')') or
                line.endswith('(') or
                '--' in line.split(',')[0]):  # 跳过只有注释的行
                continue
            
            # 使用正则表达式提取列名
            # 匹配反引号包围的列名或普通的列名（直到第一个空格）
            column_match = re.match(r'(`[^`]+`|\w+)', line)
            if column_match:
                column_name = column_match.group(1).rstrip(',')
                # 进一步验证这是一个列定义行（应该有数据类型）
                # 去掉列名后检查是否还有内容（数据类型）
                remaining = line[len(column_name):].strip().lstrip(',').strip()
                if remaining and not remaining.startswith('--'):  # 确保有数据类型定义
                    columns.append(column_name)
        
        if columns:  # 只添加非空的列列表
            result[table_name] = columns
    
    return result


def flexible_result_comparison(predicted, ground_truth, max_attempts=1000):
    """
    灵活比较两个SQL查询结果
    处理以下情况：
    1. predicted结果包含额外的列
    2. 列的顺序不同（即使列数相同）
    """
    def round_numeric_values(data):
        """将数据中的所有数值四舍五入到2位小数"""
        rounded_data = []
        for row in data:
            rounded_row = []
            for value in row:
                if isinstance(value, (int, float)):
                    # 数值类型，四舍五入到2位小数
                    rounded_value = round(value, 2)
                    # 如果是整数，保持整数形式（避免 1.0 变成 1.00）
                    if isinstance(value, int) or rounded_value.is_integer():
                        rounded_value = int(rounded_value)
                    rounded_row.append(rounded_value)
                else:
                    # 非数值类型，保持原样
                    rounded_row.append(value)
            rounded_data.append(tuple(rounded_row))
        return rounded_data
    
    # 对两个结果集进行数值四舍五入处理
    predicted = round_numeric_values(predicted) if predicted else []
    ground_truth = round_numeric_values(ground_truth) if ground_truth else []

    if not predicted or not ground_truth:
        return len(predicted) == len(ground_truth)
    
    # 获取列数
    pred_cols = len(predicted[0])
    truth_cols = len(ground_truth[0])
    
    # 如果predicted的列数少于ground_truth，直接返回False
    if pred_cols < truth_cols:
        return False
    
    # 如果列数相同，需要考虑所有列的排列
    if pred_cols == truth_cols:
        attempts = 0
        for perm in permutations(range(pred_cols)):
            if attempts >= max_attempts:
                break
            attempts += 1
            
            # 重新排列predicted的列
            reordered_predicted = []
            for row in predicted:
                reordered_row = tuple(row[i] for i in perm)
                reordered_predicted.append(reordered_row)
            
            # 比较重新排列后的结果
            if set(reordered_predicted) == set(ground_truth):
                return True
        return False
    
    # 如果predicted有更多列，需要先选择列组合，再考虑排列
    attempts = 0
    for col_indices in combinations(range(pred_cols), truth_cols):
        if attempts >= max_attempts:
            break
            
        # 对选中的列考虑所有可能的排列
        for perm in permutations(range(truth_cols)):
            attempts += 1
            if attempts > max_attempts:
                break
                
            # 首先按col_indices提取列，然后按perm重新排列
            filtered_predicted = []
            for row in predicted:
                # 先提取指定的列
                selected_cols = [row[col_indices[i]] for i in range(truth_cols)]
                # 再按排列重新组织
                reordered_row = tuple(selected_cols[perm[i]] for i in range(truth_cols))
                filtered_predicted.append(reordered_row)
            
            # 比较结果
            if set(filtered_predicted) == set(ground_truth):
                return True
    
    return False


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





class SQLSubQueryGenerator:
    """从主SQL查询生成有用的子查询"""
    
    def __init__(self, main_sql: str):
        self.main_sql = main_sql.strip()
        self.existing_subqueries = self._extract_existing_subqueries()
        self.tables = self._extract_tables()
        self.columns = self._extract_columns()
        self.joins = self._extract_joins()
        self.where_conditions = self._extract_where_conditions()
        self.order_by = self._extract_order_by()
        self.aggregations = self._extract_aggregations()
    
    def _extract_existing_subqueries(self) -> List[str]:
        """提取SQL中已存在的子查询"""
        subqueries = []
        
        # 匹配括号中的SELECT语句
        pattern = r'\(SELECT\s+.*?\)'
        
        # 使用栈来处理嵌套括号
        i = 0
        while i < len(self.main_sql):
            if self.main_sql[i] == '(':
                # 检查是否是子查询
                remaining = self.main_sql[i:]
                if re.match(r'\(\s*SELECT', remaining, re.IGNORECASE):
                    # 找到匹配的右括号
                    depth = 0
                    start = i
                    for j in range(i, len(self.main_sql)):
                        if self.main_sql[j] == '(':
                            depth += 1
                        elif self.main_sql[j] == ')':
                            depth -= 1
                            if depth == 0:
                                # 提取完整的子查询（包括括号）
                                subquery = self.main_sql[start:j+1]
                                # 去掉外层括号并清理
                                clean_subquery = subquery[1:-1].strip()
                                subqueries.append(clean_subquery)
                                i = j
                                break
            i += 1
        
        return subqueries
        
    def _extract_tables(self) -> List[Tuple[str, str]]:
        """提取表名和别名 [(table_name, alias), ...]"""
        # 先移除子查询，避免干扰
        sql_without_subqueries = self._remove_subqueries(self.main_sql)
        
        tables = []
        # 匹配 FROM 和 JOIN 子句中的表
        pattern = r'(?:FROM|JOIN)\s+(\w+)(?:\s+AS\s+)?(?:\s+(\w+))?'
        matches = re.findall(pattern, sql_without_subqueries, re.IGNORECASE)
        for match in matches:
            table_name = match[0]
            alias = match[1] if match[1] else table_name
            tables.append((table_name, alias))
        return tables
    
    def _remove_subqueries(self, sql: str) -> str:
        """临时移除子查询以便解析主查询结构"""
        # 用占位符替换子查询
        result = sql
        for i, subquery in enumerate(self.existing_subqueries):
            result = result.replace(f"({subquery})", f"__SUBQUERY_{i}__")
        return result
    
    def _extract_columns(self) -> List[str]:
        """提取SELECT中的列"""
        sql_without_subqueries = self._remove_subqueries(self.main_sql)
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql_without_subqueries, re.IGNORECASE | re.DOTALL)
        if not select_match:
            return []
        
        columns_str = select_match.group(1)
        columns = [col.strip() for col in columns_str.split(',')]
        return columns
    
    def _extract_joins(self) -> List[dict]:
        """提取JOIN信息"""
        sql_without_subqueries = self._remove_subqueries(self.main_sql)
        joins = []
        
        # 更灵活的JOIN模式匹配
        pattern = r'(\w+)\s+(?:AS\s+)?(\w+)\s+(?:INNER\s+|LEFT\s+|RIGHT\s+)?JOIN\s+(\w+)\s+(?:AS\s+)?(\w+)\s+ON\s+([\w.]+)\s*=\s*([\w.]+)'
        matches = re.findall(pattern, sql_without_subqueries, re.IGNORECASE)
        
        for match in matches:
            joins.append({
                'left_table': match[0],
                'left_alias': match[1],
                'right_table': match[2],
                'right_alias': match[3],
                'left_key': match[4],
                'right_key': match[5]
            })
        return joins
    
    def _extract_where_conditions(self) -> List[str]:
        """提取WHERE条件"""
        sql_without_subqueries = self._remove_subqueries(self.main_sql)
        where_match = re.search(r'WHERE\s+(.*?)(?:ORDER BY|GROUP BY|LIMIT|$)', 
                               sql_without_subqueries, re.IGNORECASE | re.DOTALL)
        if not where_match:
            return []
        
        conditions = where_match.group(1).strip()
        return [cond.strip() for cond in re.split(r'\s+AND\s+|\s+OR\s+', conditions, flags=re.IGNORECASE)]
    
    def _extract_order_by(self) -> List[Tuple[str, str]]:
        """提取ORDER BY列和方向"""
        sql_without_subqueries = self._remove_subqueries(self.main_sql)
        order_match = re.search(r'ORDER BY\s+(.*?)(?:LIMIT|$)', sql_without_subqueries, re.IGNORECASE)
        if not order_match:
            return []
        
        order_str = order_match.group(1).strip()
        orders = []
        for item in order_str.split(','):
            parts = item.strip().split()
            column = parts[0]
            direction = parts[1] if len(parts) > 1 else 'ASC'
            orders.append((column, direction))
        return orders
    
    def _extract_aggregations(self) -> List[str]:
        """提取可能需要聚合的列"""
        agg_columns = []
        # 从ORDER BY中提取
        for col, _ in self.order_by:
            agg_columns.append(col)
        
        # 从WHERE条件中提取非空检查的列
        for condition in self.where_conditions:
            if 'IS NOT NULL' in condition.upper():
                col = condition.split()[0]
                agg_columns.append(col)
        
        return list(set(agg_columns))
    
    def generate_sub_queries(self) -> List[str]:
        """生成所有子查询"""
        sub_queries = []
        
        # 0. 首先添加已存在的子查询
        sub_queries.extend(self.existing_subqueries)
        
        # 1. 为每个表生成COUNT查询
        sub_queries.extend(self._generate_table_count_queries())
        
        # 2. 为JOIN生成COUNT查询
        sub_queries.extend(self._generate_join_count_queries())
        
        # 3. 为ORDER BY列生成MIN/MAX查询
        sub_queries.extend(self._generate_aggregation_queries())
        
        # 4. 为条件列生成COUNT查询
        sub_queries.extend(self._generate_condition_count_queries())
        
        # 5. 从WHERE条件中提取涉及的列，生成额外的聚合查询
        sub_queries.extend(self._generate_where_aggregation_queries())
        
        # 去重
        unique_queries = []
        seen = set()
        for query in sub_queries:
            normalized = ' '.join(query.split()).upper()
            if normalized not in seen:
                unique_queries.append(query)
                seen.add(normalized)
        
        return unique_queries
    
    def _generate_table_count_queries(self) -> List[str]:
        """为每个表生成COUNT(*)查询"""
        queries = []
        seen = set()
        for table_name, alias in self.tables:
            if table_name not in seen:
                queries.append(f"SELECT COUNT(*) FROM {table_name}")
                seen.add(table_name)
        return queries
    
    def _generate_join_count_queries(self) -> List[str]:
        """为JOIN生成COUNT查询"""
        queries = []
        for join in self.joins:
            query = (f"SELECT COUNT(*) FROM {join['left_table']} {join['left_alias']} "
                    f"INNER JOIN {join['right_table']} {join['right_alias']} "
                    f"ON {join['left_key']} = {join['right_key']}")
            queries.append(query)
        return queries
    
    def _generate_aggregation_queries(self) -> List[str]:
        """为聚合列生成MIN/MAX/AVG查询"""
        queries = []
        
        for col, direction in self.order_by:
            # 提取表别名和列名
            if '.' in col:
                table_alias, col_name = col.split('.')
                # 找到实际的表名
                table_name = None
                for tname, talias in self.tables:
                    if talias == table_alias:
                        table_name = tname
                        break
                
                if table_name:
                    # 根据排序方向生成MIN或MAX
                    if direction.upper() == 'ASC':
                        queries.append(f"SELECT MIN({col_name}) FROM {table_name}")
                    else:
                        queries.append(f"SELECT MAX({col_name}) FROM {table_name}")
        
        return queries
    
    def _generate_condition_count_queries(self) -> List[str]:
        """为WHERE条件中的列生成COUNT查询"""
        queries = []
        
        for condition in self.where_conditions:
            if 'IS NOT NULL' in condition.upper():
                col = condition.split()[0]
                if '.' in col:
                    table_alias, col_name = col.split('.')
                    table_name = None
                    for tname, talias in self.tables:
                        if talias == table_alias:
                            table_name = tname
                            break
                    
                    if table_name:
                        queries.append(f"SELECT COUNT({col_name}) FROM {table_name}")
        
        return queries
    
    def _generate_where_aggregation_queries(self) -> List[str]:
        """从WHERE条件中提取列并生成聚合查询"""
        queries = []
        
        for condition in self.where_conditions:
            # 提取条件中的列名（左侧）
            # 匹配类似 "sat.AvgScrRead = ..." 的模式
            match = re.match(r'([\w.]+)\s*[=<>!]+', condition)
            if match:
                col = match.group(1)
                if '.' in col:
                    table_alias, col_name = col.split('.')
                    table_name = None
                    for tname, talias in self.tables:
                        if talias == table_alias:
                            table_name = tname
                            break
                    
                    if table_name:
                        # 生成MIN和MAX查询
                        queries.append(f"SELECT MIN({col_name}) FROM {table_name}")
                        queries.append(f"SELECT MAX({col_name}) FROM {table_name}")
        
        return queries

