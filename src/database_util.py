# 在构建database的过程中所需要的工具。
import argparse
import json
import os
import sqlite3
from openai import OpenAI
from sentence_transformers import SentenceTransformer,util
from tqdm import tqdm
from typing import Dict, List, Tuple, Any, Optional
import re
import pandas as pd
from db_class import Database


def load_json_file(path: str):
    """
    加载指定路径的 JSON 文件。
    如果文件不存在则抛出 FileNotFoundError。    
    Args:
        path (str): 文件路径

    Returns:
        dict: 解析后的 JSON 数据
    """
    # 判断文件是否存在
    if not os.path.exists(path):
        raise FileNotFoundError(f"File '{path}' does not exist. Please check the path.")

    # 尝试加载 JSON 文件
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to parse JSON file '{path}': {e}")
    except Exception as e:
        raise RuntimeError(f"Unexpected error while reading file '{path}': {e}")


def split_ddl(content: str) -> List[str]:
    """
    解析DDL内容，按空行分割提取每个表的DDL
    
    Args:
        content: DDL文件内容
    
    Returns:
        包含完整DDL语句的字符串列表
    """
    # 按连续的空行分割
    blocks = re.split(r'\n\s*\n', content.strip())
    
    results = []
    for block in blocks:
        block = block.strip()
        if block and 'CREATE TABLE' in block.upper():
            results.append(block)
    
    return results



def is_date_column(column_name: str) -> bool:
    """判断是否为日期列"""
    date_keywords = ['date', 'time', 'created', 'updated', 'release', 'id']
    return any(keyword in column_name.lower() for keyword in date_keywords)

def is_date_format(value: str) -> bool:
    """判断字符串是否为日期格式"""
    if not value:
        return False
    # 常见日期格式模式
    date_patterns = [
        r'\d{6}', #YYYYMM
        r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
        r'\d{4}/\d{1,2}/\d{1,2}',  # YYYY/M/D or YYYY/MM/DD
        r'\d{1,2}/\d{1,2}/\d{4}',  # M/D/YYYY or MM/DD/YYYY
        r'\d{4}-\d{1,2}-\d{1,2}',  # YYYY-M-D
    ]
    return any(re.match(pattern, str(value)) for pattern in date_patterns)




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

def clean_fake_json(json_str: str) -> str:
    # 替换反斜杠加换行（以及连续多空格）为单一空格
    json_str = re.sub(r'\\\s*\n', ' ', json_str)
    # 去除每个元素后的多余逗号
    json_str = re.sub(r',(\s*[}\]])', r'\1', json_str)
    # 必要时还可以加：恢复单引号为双引号（如模型偶尔输出单引号）：
    # json_str = re.sub(r"'", '"', json_str)
    # 1. 把所有 \' 换回 '
    json_str = re.sub(r"\\'", "'", json_str)
    # 2. 去掉所有不合法的反斜杠转义（非标准JSON转义）
    json_str = re.sub(r'\\(?![\\ntfrbu"/])', '', json_str)
    return json_str

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
            # 修复伪json
            cleaned_json = clean_fake_json(cleaned_json)
            try:
                cleaned_json = json.loads(cleaned_json)
                cleaned_jsons.append(cleaned_json)
            except:
                print(f'json格式错误，不能转换: \n {cleaned_json}')

    return cleaned_jsons

def call_llm(input):
    
    API_KEY = os.getenv('OPENAI_API_KEY') # 替换为您的API密钥
    BASE_URL = "https://www.dmxapi.com/v1"  # 如果使用OpenAI官方API，保持为None；如果使用其他兼容的API，设置相应的URL
    MODEL = "gpt-4o"
    client = OpenAI(
            api_key=API_KEY,
            base_url=BASE_URL
        )
    response = client.chat.completions.create(
                    model=MODEL,
                    messages = [
                    {
                        "role": "user", 
                        "content": input
                    }
                ]
                )
                
    return response.choices[0].message.content



def get_data_range(db_path, table_name: str, column_name: str, column_type: str) -> Tuple[Any, Any]:
    """获取指定字段的数据范围"""
    conn = connect_to_database(db_path)
    cursor = conn.cursor()
    
    try:
        # 检查是否有非空值
        cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} IS NOT NULL AND {column_name} != ''")
        count = cursor.fetchone()[0]
        
        if count == 0:
            if(column_name == "id" ):
                print(f"表{table_name}，列id,none情况")
            return None, None
        
        if column_type.upper() in ['INTEGER', 'REAL']:
            # 数值类型：获取最小值和最大值
            cursor.execute(f"SELECT MIN({column_name}), MAX({column_name}) FROM {table_name} WHERE {column_name} IS NOT NULL")
            min_val, max_val = cursor.fetchone()
            if(column_name == "id" and min_val == None):
                print(f"表{table_name}，列id,min情况")
            return min_val, max_val
        else:
            # 文本类型：获取字典序最小和最大的值
            cursor.execute(f"SELECT MIN({column_name}), MAX({column_name}) FROM {table_name} WHERE {column_name} IS NOT NULL AND {column_name} != ''")
            min_val, max_val = cursor.fetchone()
            
            # 如果值太长，截取前50个字符
            if min_val and len(str(min_val)) > 50:
                min_val = str(min_val)[:50] + "..."
            if max_val and len(str(max_val)) > 50:
                max_val = str(max_val)[:50] + "..."
                
            return min_val, max_val
            
    except sqlite3.Error as e:
        print(f"获取 {table_name}.{column_name} 数据范围时出错: {e}")
        return None, None




def connect_to_database(db_path: str) -> sqlite3.Connection:
    """连接到SQLite数据库"""
    try:
        conn = sqlite3.connect(db_path)
        return conn
    except sqlite3.Error as e:
        print(f"连接数据库时出错: {e}")
        raise



def enhance_schema_with_table_description(schema_text: str) -> str:  
    prompt = f"""You are a database expert. Your task is to generate detailed table descriptions for each table in the given database schema. Each table description should include:
1. The purpose and function of the table.
2. Primary keys, foreign keys, or other important constraints (if any).
3. Main use cases or application scenarios of the data.

# Requirements:
- Use the following comment format:

-- Table name : brief purpose
-- Table description:
--   Description of columns and their main functions.
--   Primary key, foreign key, or important constraint information (if any).
--   Main use cases or application scenarios.

- Do not output explanations, reasoning in natural language, or intermediate thoughts.


# Output:
```json
table with table description here.
```
""" + """
# Output Format:
```json
{
    "table1 name": "generated table1 descriptions comments",
    "table2 name": "generated table2 descriptions comments"
}
```

# Example:

Input Schema:
CREATE TABLE student (
    student_id INT PRIMARY KEY, -- already existing comments
    name VARCHAR(50),  -- already existing comments
    gender CHAR(1), -- already existing comments
    birth_date DATE,  -- already existing comments
    admission_year INT, -- already existing comments
    class_id INT, -- already existing comments
    contact_info VARCHAR(100)  -- already existing comments
);
...

Expected Output:
```json 
{"student": " -- Student Information Table: Used to store basic information of school students. \n -- This table contains the student's student ID, name, gender,... (Not required to be written in full) \n -- Each student has a unique student ID as the primary key. \n -- The data is mainly used for student enrollment management, course scheduling, grade recording, etc.",
 "...": ""
}
``` 
"""+ f""" 
Let's start!
Input Schema:
{schema_text}

Output:
"""      
    # print("模型输入 =================================")          去掉打印
    # print(prompt)
    enhanced_schema_reponse = call_llm(prompt)                  
    # print("模型输出 =================================")
    # print(enhanced_schema_reponse)


    enhance_schema_json = extract_json_from_text(enhanced_schema_reponse)[-1]
    #print(enhance_schema_json)
    table_text_list = split_ddl(schema_text)


    #table_text_list = split_ddl(schema_text)
    print(len(table_text_list), len(enhance_schema_json))
    assert len(table_text_list) == len(enhance_schema_json)

    table_text_with_table_description = ""
    # 将表描述加到每个table的前面
    for table_name, table_description in enhance_schema_json.items():
        for table_text in table_text_list:
            if f"CREATE TABLE {table_name} " in table_text or f"CREATE TABLE `{table_name}` " in table_text:
                new_table_text = table_description + '\n' + table_text
                table_text_with_table_description += new_table_text + '\n\n'
                break

    # 执行一下，再回滚
    temp_db_path = "./temp.sqlite"
    conn = sqlite3.connect(temp_db_path) 
    cursor = conn.cursor()
    # print("组合后的DDL ============================")          去掉打印
    # print(table_text_with_table_description)
    try:
        # 执行 LLM 输出的 DDL
        cursor.executescript(table_text_with_table_description)
        print("Schema executed successfully!")
        # 如果你想回滚，raise Exception 或直接 rollback
        conn.rollback()  
        print("Rollback executed, database unchanged.")
        return 'Success', table_text_with_table_description

    except Exception as e:
        print("Error executing schema:", e)
        print("原始 DDL 组合头1000字符:\n", table_text_with_table_description[:1000])
        # 可按需保存全DDL到文件调试
        with open('ddl_error_dump.sql', 'w', encoding='utf-8') as f:
            f.write(table_text_with_table_description)
        conn.rollback()
        return 'Fail', ""

    finally: # 会在return前面执行
        conn.close()
        # 删除临时数据库文件
        if os.path.exists(temp_db_path):
            os.remove(temp_db_path)
            print(f"Temporary database {temp_db_path} deleted.")


def format_table_column_name(name: str) -> str:
    '''格式化table和column的名称，有时候会少了`'''
    if not name or not isinstance(name, str):  # 这一行保证None/空/非字符串直接原样返回
        return name
    if (name.startswith('`') and name.endswith('`')) or (name.startswith('"') and name.endswith('"')):
        return name
    sqlite_keywords = {
        # "ABORT", "ACTION", "ADD", "AFTER", "ALL", "ALTER", "ANALYZE", "AND",
        # "AS", "ASC", "ATTACH", "AUTOINCREMENT",
        # "BEFORE", "BEGIN", "BETWEEN", "BY",
        # "CASCADE", "CASE", "CAST", "CHECK", "COLLATE", "COLUMN", "COMMIT",
        # "CONFLICT", "CONSTRAINT", "CREATE", "CROSS", "CURRENT_DATE",
        # "CURRENT_TIME", "CURRENT_TIMESTAMP",
        # "DATABASE", "DEFAULT", "DEFERRABLE", "DEFERRED", "DELETE", "DESC",
        # "DETACH", "DISTINCT", "DROP",
        # "EACH", "ELSE", "END", "ESCAPE", "EXCEPT", "EXCLUSIVE", "EXISTS",
        # "EXPLAIN",
        # "FAIL", "FOR", "FOREIGN", "FROM", "FULL",
        # "GLOB", "GROUP",
        # "HAVING",
        # "IF", "IGNORE", "IMMEDIATE", "IN", "INDEX", "INITIALLY", "INNER",
        # "INSERT", "INSTEAD", "INTERSECT", "INTO", "IS", "ISNULL",
        # "JOIN",
        # "KEY",
        # "LEFT", "LIKE", "LIMIT",
        # "MATCH",
        # "NATURAL", "NO", "NOT", "NOTNULL", "NULL",
        # "OF", "OFFSET", "ON", "OR", "ORDER", "OUTER",
        # "PLAN", "PRAGMA", "PRIMARY", "QUERY",
        # "RAISE", "RECURSIVE", "REFERENCES", "REGEXP", "REINDEX", "RELEASE",
        # "RENAME", "REPLACE", "RESTRICT", "RIGHT", "ROLLBACK", "ROW",
        # "SAVEPOINT", "SELECT", "SET",
        # "TABLE", "TEMP", "TEMPORARY", "THEN", "TO", "TRANSACTION", "TRIGGER",
        # "UNION", "UNIQUE", "UPDATE", "USING",
        # "VACUUM", "VALUES", "VIEW", "VIRTUAL",
        # "WHEN", "WHERE", "WITH", "WITHOUT",
        "ACCESS", "ADD", "AFTER", "ALL", "ALTER",
        "ANALYZE", "AND", "AS", "ASC", "AVG",
        "BEFORE", "BEGIN", "BETWEEN", "BIGINT", "BINARY",
        "BIT", "BLOB", "BOOLEAN", "BOTH", "BREAK",
        "BY", "CALL", "CASCADE", "CASE", "CAST",
        "CHAR", "CHARACTER", "CHECK", "CLOB", "COLLATE",
        "COLUMN", "COMMENT", "COMMIT", "CONDITION", "CONNECT",
        "CONSTRAINT", "CONTINUE", "CONVERT", "COUNT", "CREATE",
        "CROSS", "CURRENT", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
        "CURSOR", "DATABASE", "DATE", "DAY", "DEC",
        "DECIMAL", "DECLARE", "DEFAULT", "DELETE", "DESC",
        "DESCRIBE", "DISTINCT", "DO", "DOUBLE", "DROP",
        "EACH", "ELSE", "ELSEIF", "END", "ESCAPE",
        "EXCEPT", "EXEC", "EXECUTE", "EXISTS", "EXIT",
        "EXPLAIN", "EXTEND", "EXTERNAL", "FALSE", "FETCH",
        "FILTER", "FLOAT", "FOR", "FOREIGN", "FROM",
        "FULL", "FUNCTION", "GENERATED", "GLOBAL", "GRANT",
        "GROUP", "HAVING", "HOLD", "HOUR", "IDENTIFIED",
        "IF", "IGNORE", "ILIKE", "IN", "INDEX",
        "INNER", "INOUT", "INSENSITIVE", "INSERT", "INT",
        "INTEGER", "INTERSECT", "INTERVAL", "INTO", "IS",
        "ITERATE", "JOIN", "KEY", "LANGUAGE", "LEADING",
        "LEAVE", "LEFT", "LIKE", "LIMIT", "LOCAL",
        "LOCALTIME", "LOCALTIMESTAMP", "LOCK", "LOOP", "MATCH",
        "MAX", "MERGE", "MIN", "MINUTE", "MODIFIES",
        "MODULE", "MONTH", "NATURAL", "NCHAR", "NEW",
        "NO", "NOT", "NULL", "NUMERIC", "OF",
        "OFFSET", "OLD", "ON", "ONLY", "OPEN",
        "OR", "ORDER", "OUT", "OUTER", "OVER",
        "PARTITION", "PERCENT", "PLACING", "POSITION", "PRECISION",
        "PRIMARY", "PROCEDURE", "RANGE", "READS", "REAL",
        "RECURSIVE", "REFERENCES", "REGEXP", "RELEASE", "RENAME",
        "REPEAT", "REPLACE", "RESIGNAL", "RESTRICT", "RETURN",
        "REVOKE", "RIGHT", "RLIKE", "ROLE", "ROLLBACK",
        "ROW", "ROWNUM", "ROWS", "SAVEPOINT", "SCHEMA",
        "SELECT", "SENSITIVE", "SESSION", "SET", "SIGNAL",
        "SMALLINT", "SOME", "SPECIFIC", "SQL", "SQLEXCEPTION",
        "SQLSTATE", "SQLWARNING", "START", "STATIC", "SUBSTRING",
        "SUM", "SYSDATE", "TABLE", "TERMINATED", "THEN",
        "TIME", "TIMESTAMP", "TO", "TOP", "TRAILING",
        "TRIGGER", "TRUE", "UNDO", "UNION", "UNIQUE",
        "UNKNOWN", "UNLOCK", "UPDATE", "USAGE", "USER",
        "USING", "VALUE", "VALUES", "VARCHAR", "VARYING",
        "VIEW", "WHEN", "WHERE", "WHILE", "WITH",
        "WITHIN", "YEAR", "ZONE"
    }

    if name.upper() in sqlite_keywords or ' ' in name or '-' in name:
        return '`'+name+'`'
    else:
        return name 


import re

def fix_trailing_comma_in_create_table(ddl: str) -> str:
    lines = [line for line in ddl.strip().split('\n') if line.strip() != '']
    if len(lines) < 1:
        return ddl

    last_line = lines[-1]

    # 兼容,);
    # 用正则去掉 ); 前第一个逗号（只删括号前最后的逗号，不动前面字段之间逗号）
    import re
    if re.search(r',\s*\)\s*;', last_line):
        # 找到最后一个,再找到最近);，只删最近的那个
        idx_comma = last_line.rfind(',')
        idx_paren = last_line.find(')', idx_comma)
        if idx_comma != -1 and idx_paren != -1 and idx_paren > idx_comma:
            # 删掉逗号
            lines[-1] = last_line[:idx_comma] + last_line[idx_comma+1:]
            return '\n'.join(lines)

    # 否则走多行末行是);的情况
    if last_line.strip().replace(" ", "") == ');' and len(lines) >= 2:
        fld = lines[-2]
        if '--' in fld:
            field_part, comment_part = fld.split('--', 1)
            field_part = field_part.rstrip().rstrip(',')
            comment_part = '--' + comment_part
            lines[-2] = field_part + ' ' + comment_part
        else:
            lines[-2] = lines[-2].rstrip().rstrip(',')
    return '\n'.join(lines)

def extract_columns_from_schema(schema_text):
    """
    提取schema中的所有字段名（仅基于字段定义行，自动识别各类类型，无视约束行）
    """
    type_keywords = (
        'integer|int|bigint|smallint|tinyint|mediumint|'
        'real|double|float|numeric|decimal|boolean|'
        'text|char|varchar|clob|'
        'date|datetime|blob'
    )
    pattern = re.compile(
        rf'^\s*([`"\[]?[\w\s\(\)%-]+[`"\]]?)\s+({type_keywords})\b',
        re.IGNORECASE
    )
    cols = []
    for line in schema_text.splitlines():
        line = line.strip()
        if not line or line.upper().startswith(('CREATE TABLE', '--')): continue
        m = pattern.match(line)
        if m:
            col = m.group(1).strip()
            # 去除行尾,号/首尾多余符号
            col = col.rstrip(',').strip('`"[] ')
            # 跳过滤掉主键、约束虚拟名
            if col.upper() in {"PRIMARY", "TABLE", "CONSTRAINT"}: continue
            col = format_table_column_name(col)
            cols.append(col)
    return cols
def compare_schema_columns(single_table_schema_text, table_with_comments_reponse):
    """
    返回两个列表:
    1. [原始DDL有但LLM输出没有的字段名]  # missing
    2. [LLM真实存在的字段名]           # both
    """
    schema_cols = extract_columns_from_schema(single_table_schema_text)
    # 上面这行不要转set！保留顺序！

    if type(table_with_comments_reponse) == list:
        ddl_text = table_with_comments_reponse[-1]
    else:
        ddl_text = table_with_comments_reponse

    llm_cols_all = set(extract_columns_from_schema(ddl_text))
    print(llm_cols_all)
    # 1. 原始DDL有但LLM输出没有的字段
    missing = [col for col in schema_cols if col not in llm_cols_all]
    # 2. LLM真实存在的字段（LLM输出且DDL原始里也有）
    real_exist = [col for col in schema_cols if col in llm_cols_all]
    return missing, real_exist
def fallback_fix_table_columns(single_table_schema_text, table_with_comments_reponse):
    """
    用原DDL字段兜底修正LLM输出：
    1. 删除LLM输出错误（即其实不存在）的字段行
    2. 把原DDL missing的列行补过来，并把注释全部换成" -- format:"
    3. 保留主键和其他约束相关内容
    返回修正后的DDL字符串
    """
    # 提取哪些列需要修正
    missing_cols, ok_cols = compare_schema_columns(single_table_schema_text, table_with_comments_reponse)
    # 拿到LLM输出DDL文本
    if isinstance(table_with_comments_reponse, list):
        llm_ddl = table_with_comments_reponse[-1]
    else:
        llm_ddl = table_with_comments_reponse

    llm_lines = llm_ddl.splitlines()
    fixed_lines = []
    # 用于判断插入missing内容的位置
    inside_col_block = False
    first_insert_done = False

    # 找到TABLE字段区块("CREATE TABLE ... (")之后的下标
    for idx, line in enumerate(llm_lines):
        # 检查是否CREATE TABLE行
        if re.match(r'^\s*CREATE\s+TABLE', line, re.IGNORECASE):
            fixed_lines.append(line)
            save_next_bracket = True  # 触发：下一个读取的行可能是括号
            continue
        # 只保留单独一行的左括号
        if save_next_bracket:
            if line.strip() == '(':
                fixed_lines.append(line)
            save_next_bracket = False  # 只检查一次
            continue
            # 插在这里
            if not first_insert_done and missing_cols:
                # 提取原DDL里的对应行，并替换注释
                schema_lines = single_table_schema_text.splitlines()
                for sline in schema_lines:
                    col_match = re.match(r'\s*(`[^`]+`|\w+)\s+(integer|real|text|date|int|float|double|numeric|decimal|varchar|char|boolean|blob|datetime|TEXT|INTEGER|REAL|DATE|INT|FLOAT|DOUBLE|NUMERIC|DECIMAL|VARCHAR|CHAR|BOOLEAN|BLOB|DATETIME)\b(?:\s+[^,\-\-]+)*\s*,?\s*--\s*(.+)', sline,re.IGNORECASE)
                    if col_match:
                        scol_raw = format_table_column_name(col_match.group(1))
                        if scol_raw in missing_cols:
                            # 去除原注释，加"-- format:"
                            field_type = col_match.group(2).lower()                   ##1
                            code_part = sline.split('--')[0].rstrip().rstrip(',')
                            code_part = code_part.rstrip(',')
                            type_anno = code_part if code_part.endswith(',') else code_part + ','
                            fixed_lines.append(f'{type_anno} -- Format: {field_type}')
                first_insert_done = True
            continue

        # 检查当前行是不是字段定义，决定是否保留
        col_match = re.match(r'\s*(`[^`]+`|\w+)\s+\w+', line)
        if col_match:
            col_raw = format_table_column_name(col_match.group(1))
            print(f"{col_raw}\n")
            if col_raw not in ok_cols:
                continue  # 跳过错误字段行
        # 其它行（主键、约束、结束括号等），全部保留
        fixed_lines.append(line)

    # 补救极端情况，如果CREATE TABLE单行带(，没有独立的左括号，则需要插入miss字段在下一个字段行之前
    if not first_insert_done and missing_cols:
        # 寻找第一次字段定义的那一行前，插入missing字段
        found_insert = False
        output = []
        schema_lines = single_table_schema_text.splitlines()
        insert_lines = []
        for sline in schema_lines:
            col_match = re.match(r'\s*(`[^`]+`|\w+)\s+(integer|real|text|date|int|float|double|numeric|decimal|varchar|char|boolean|blob|datetime|TEXT|INTEGER|REAL|DATE|INT|FLOAT|DOUBLE|NUMERIC|DECIMAL|VARCHAR|CHAR|BOOLEAN|BLOB|DATETIME)\b(?:\s+[^,\-\-]+)*\s*,?\s*--\s*(.+)', sline,re.IGNORECASE)
            if col_match:
                scol_raw = format_table_column_name(col_match.group(1))
                if scol_raw in missing_cols:
                    field_type = col_match.group(2).lower()
                    code_part = sline.split('--')[0].rstrip().rstrip(',')
                    code_part = code_part.rstrip(',')
                    type_anno = code_part if code_part.endswith(',') else code_part + ','
                    insert_lines.append(f'{type_anno} -- Format: {field_type}')
        for idx, line in enumerate(fixed_lines):
            col_match = re.match(r'\s*(`[^`]+`|\w+)\s+\w+', line)
            if not found_insert and col_match:
                output.extend(insert_lines)
                found_insert = True
            output.append(line)
        fixed_lines = output

    return '\n'.join(fixed_lines)
    
def enhance_schema_with_column_description(schema_text:str, db_path: str) -> str:
    conn = sqlite3.connect(db_path) 
    cursor = conn.cursor()

    query = "SELECT name FROM sqlite_master WHERE type='table' AND name!='sqlite_sequence';"
    tables = pd.read_sql_query(query, conn)
    tables = tables['name'].tolist()                #把查询的所有表名转为python列表
    print("数据库中的表:", tables)
    table_schema_text_list = split_ddl(schema_text)       #把schema_text转为数组，["creat",]
    print(len(table_schema_text_list), len(tables))
    assert len(table_schema_text_list) == len(tables)

    table_schema_text_dict = {}
    for table in tables:                       #tables是之前的列表
        table = format_table_column_name(table)
        for table_schema_t in table_schema_text_list:
            if f'CREATE TABLE {table} ' in table_schema_t:
                table_schema_text_dict[table] = table_schema_t         #建立好字典序列
                break
    print(len(table_schema_text_dict), len(tables))
    assert len(table_schema_text_dict) == len(tables)

    new_schema_text = ""
    for table in tables:
        print(f"正在处理表{table} --------------------")
        table = format_table_column_name(table)
        single_table_schema_text = table_schema_text_dict[table]     #schema_text根据table单拿出来的
        sql = f"SELECT * FROM {table} ORDER BY RANDOM() LIMIT 1;"
        print(db_path)
        #print(f'SQL执行：{sql}')                去掉打印
        cursor.execute(sql)
        results = cursor.fetchall()  
        columns = [description[0] for description in cursor.description]  # 取列名

        columns_text = ""
        for column in columns:
            column = format_table_column_name(column)

            sql = f"SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL AND LENGTH({column}) <= 80 ORDER BY RANDOM() LIMIT 10;"
           #print(f'SQL执行：{sql}')                      去掉打印
            cursor.execute(sql)
            results = cursor.fetchall() 

            columns_text += f"Randomly select 10 values from column {column} in table {table}: {[row[0] for row in results]} \n"
        
        prompt = f"""You are a database expert. For each table column and its metadata, according to the given column data, generate a detailed description including Data composition / format (how the value is constructed).

# Rules:
1. Output only the final schema wrapped in ```sql  ``` with the comments included.
2. Do not output any explanations besides the schema.

Follow the format below:

column_name column_type,  -- Format: description of how value is constructed, without list examples.

# Output:
```sql
DDL with column format here
```

# Example

Input Schema:
CREATE TABLE student (
    student_id CHAR(10) PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    birth_date DATE
);

Input Data:
Randomly select 10 values from column student_id in table student: ["2023001001", "2023001002", "2023001003", "2023001004", "2023001005", "2023001006", ...]

Randomly select 10 values from column name in table student: ["John Smith", "Alice Johnson", "Michael Brown", "Emily Davis", "David Wilson", ...]

Randomly select 10 values from column birth_date in table student: ["2004-05-12", "2003-11-23", "2005-07-19", "2002-09-30", "2004-01-08", "2003-06-25", ...]


Expected output:
```sql
CREATE TABLE student (
    student_id CHAR(10) PRIMARY KEY,  -- Format: first 4 digits = admission year, last 6 digits = serial number
    name VARCHAR(50) NOT NULL,  -- Format: Each name part should use Title Case (first letter uppercase, remaining letters lowercase)
    birth_date DATE  -- Format: ISO 8601 date 'YYYY-MM-DD' where the first 4 digits are the year (e.g., 2005), the middle 2 digits are the month (01-12), and the last 2 digits are the day (01-31).
);
```

Let's start!
Input Schema:
{single_table_schema_text}

Input Data:
{columns_text}

Output:
"""     
        # if(table == "posts" or table == "foreign_data"):
        #     print(f'LLM处理{table}输入: -------------')
        #     print(prompt)

        MAX_TRY_TIME = 3
        status = 'Fail'
        while MAX_TRY_TIME>0 and status=='Fail':
            table_with_comments_reponse = call_llm(prompt)
            fix_count = 2
            while fix_count > 0:
                missing_cols, ok_cols = compare_schema_columns(single_table_schema_text, table_with_comments_reponse)
                # 打印结果
                print("LLM缺少列（在LLM输出中没有但原DDL有）:", missing_cols)
                print("LLM真实列（在LLM输出中有且原DDL中也有）:", ok_cols)
                if len(missing_cols) != 0:
                    print("检测到LLM输出缺失字段，重新生成...")
                    table_with_comments_reponse = call_llm(prompt)
                    fix_count -= 1
                else :
                    break
            missing_cols, ok_cols = compare_schema_columns(single_table_schema_text, table_with_comments_reponse)
            if len(missing_cols) != 0:
                table_with_comments_reponse=fallback_fix_table_columns(single_table_schema_text, table_with_comments_reponse)
                print(f"现在有一个进行兜底策略")
            # if(table == "posts" or table == "foreign_data"):
            #     print(f'LLM处理{table}输出: -------------')             
            #     print(table_with_comments_reponse)                   
            table_with_comments = extract_sql_from_text(table_with_comments_reponse)
            # if(table == "posts" or table == "foreign_data"):
            #     print(f'LLM{table}输出提取: -------------')             
            #     print(table_with_comments) 
            # 执行一下，再回滚
            try:
                # 执行 LLM 输出的 DDL
                temp_db_path = "./temp.sqlite"
                temp_conn = sqlite3.connect(temp_db_path) 
                temp_cursor = temp_conn.cursor()
                ddl_to_exec = fix_trailing_comma_in_create_table(table_with_comments[-1])
                temp_cursor.execute(ddl_to_exec)
                print("Schema executed successfully!")
                # 如果你想回滚，raise Exception 或直接 rollback
                temp_conn.rollback()  
                print("Rollback executed, database unchanged.")
                status = 'Success'

            except Exception as e:
                print("Error executing schema:", e)
                temp_conn.rollback()
                MAX_TRY_TIME -= 1

            finally: # 会在return前面执行
                temp_conn.close()
                # 删除临时数据库文件
                if os.path.exists(temp_db_path):
                    os.remove(temp_db_path)
                    print(f"Temporary database {temp_db_path} deleted.")   
         
        if MAX_TRY_TIME == 0:
            print("-------- 原始DDL ----------")
            print(table_with_comments[-1])
            print("-------- 修正后DDL ----------")
            print(ddl_to_exec)
            raise ValueError(f"经过3次尝试，{table} 表还是不能生成正常的column描述。")
        
        #new_schema_text += table_with_comments[-1] + '\n\n'
        new_schema_text += ddl_to_exec + '\n\n'

    conn.close()
        
    return new_schema_text
      
def split_database_ratio_maps(database_ratio_maps, max_pairs_per_group = 80):
    """
    将database_ratio_maps按pair数量分组，每个分组按总pair计数（1:1+N:1），如遇到单表某区域pair过多会局部切片。
    返回groups，每组为{table:{'1:1': [...], 'N:1': [...]}}结构，不丢失任何pair。
    """
    import copy

    # 统计总
    all_count = sum(len(v["1:1"]) + len(v["N:1"]) for v in database_ratio_maps.values())
    print(f"all_count = {all_count}")
    if(all_count > 400):          #这里考虑DDL的，如果表多，说明输入多，就可以给分组小点
        max_pairs_per_group = 70
    if all_count < 100 and max_pairs_per_group == 80:
        print("Pair count < 100，无需切分，直接全体处理。")
        return [copy.deepcopy(database_ratio_maps)]

    tables = list(database_ratio_maps.keys())
    groups = []
    cur_group = {}
    cur_count = 0
    table_pos = 0
    area_pos = {"1:1": 0, "N:1": 0}
    active_table = None
    active_area = None
    total_pairs_this_group = 0

    # 所有表遍历
    while table_pos < len(tables):
        tab = tables[table_pos]
        tabmap = database_ratio_maps[tab]
        # 对每个区域：先1:1，再N:1
        for area in ["1:1", "N:1"]:
            pairs = tabmap[area]
            start = 0 if area_pos.get(f"{tab}_{area}") is None else area_pos[f"{tab}_{area}"]
            while start < len(pairs):
                remain = max_pairs_per_group - cur_count
                remain_pairs = len(pairs) - start
                taking = min(remain, remain_pairs)
                # 新的一组直接加
                if tab not in cur_group:
                    cur_group[tab] = {"1:1":[], "N:1":[]}
                # 注意：同一个表、同一个区域续加
                cur_group[tab][area].extend(pairs[start:start+taking])
                cur_count += taking
                start += taking
                # 分完一组就break（如果组满了/区域没完要续，留start，下次从此位置继续）
                if cur_count == max_pairs_per_group:
                    # 留下断点，记录到table_pos/area/下标
                    # 下次进入此tab:area时从start断点继续
                    area_pos[f"{tab}_{area}"] = start
                    # 统计pair数
                    group_pair_count = sum(len(x["1:1"]) + len(x["N:1"]) for x in cur_group.values())
                    print(f"分组 {len(groups)+1}: 表数={len(cur_group)}, pair数={group_pair_count}    断点: {tab} - {area} - {start}/{len(pairs)}")
                    groups.append(copy.deepcopy(cur_group))
                    cur_group = {}
                    cur_count = 0
                elif start == len(pairs):
                    # 区域走完，断点清空
                    if f"{tab}_{area}" in area_pos:
                        del area_pos[f"{tab}_{area}"]
            # 区域更新完
        table_pos += 1

    # 收尾：有剩余未成组
    if cur_count > 0:
        group_pair_count = sum(len(x["1:1"]) + len(x["N:1"]) for x in cur_group.values())
        print(f"分组 {len(groups)+1}: 表数={len(cur_group)}, pair数={group_pair_count}    (结尾组)")
        groups.append(cur_group)
    return groups


def get_db_fd_list_and_column_info(table_desc_dict, opt): #TODO 没有容错处理
    # 处理数据库
    for i, sqlite_dataset_name in enumerate(table_desc_dict):
        print("================"*5)        
        print(f'现在处理第{i}/{len(table_desc_dict)}个数据库: {sqlite_dataset_name}')
        if 'fd_list' in table_desc_dict[sqlite_dataset_name]:
            print('fd list 已经被处理，跳过')
            continue
        if sqlite_dataset_name not in table_desc_dict:
            print(f'{sqlite_dataset_name} not in table_desc_dict, 跳过')
            continue
        
        db_path = os.path.join(opt.db_path, sqlite_dataset_name, sqlite_dataset_name+'.sqlite')
        db_model = Database(db_path)
        tables = db_model.list_tables()
    
        database_ratio_maps = {}
        null_column_list = []
        #遍历所有表
        for table in tables:
            table_check_null = db_model.check_null_values(table)
            null_column_list.extend([f'{table}.{col}' for col in table_check_null['columns_with_nulls']])

            # 1.1 先对数据库中的每个表分析函数依赖关系
            try:
                # fd_list shape [[[Left],'Right'], [[Left], 'Right'], ...]
                fd_list = db_model.analyze_specific_table(table, max_lhs_size=1)  # 这个可以根据primary key的个数来定
                print(f"\n表 {table} 的分析完成\n" + "="*50)
            except Exception as e:
                fd_list = []
                print(f"分析表 {table} 时出错: {e}")
            # 1.2 这里的函数依赖太多了，需要过滤一些，首先是column中只有一个值的，这样的column谁都能确定，它一直被位于右值。
            table_distributions = db_model.analyze_column_distribution(table)
            filted_fd_list = []
            for fd in fd_list:
                left_attr = fd[0][0]  # 后面是[0]，因为之前设置的依赖size是1
                right_attr = fd[1]
                # 左值是pk的就不要了吧，毕竟这个函数依赖关系确定了
                # if table_schema_dict[left_attr]['pk'] == False and table_distributions[right_attr]['unique_count'] > 1: 
                if table_distributions[right_attr]['unique_count'] > 1:  # 保留主键
                    filted_fd_list.append([left_attr, right_attr])  # 这里去掉left的[]了
            
            # 2. 处理属性之间的比例关系。
            fd_set = set(tuple(fd) for fd in filted_fd_list)
            visited = set()
            ratio_map = {"1:1":[], "N:1":[]}

            for fd in filted_fd_list:
                left, right = fd
                if (left, right) in visited or (right, left) in visited:
                    continue 

                reverse_fd = (right, left)
                if reverse_fd in fd_set:
                    ratio_map["1:1"].append([left, right])
                    visited.add((left, right))
                    visited.add((right, left))
                else:
                    ratio_map["N:1"].append([left, right])
                    visited.add((left, right))

            database_ratio_maps[table] = ratio_map
    
        print(f"数据库属性比例关系：\n {database_ratio_maps}")
        #database_ratio_maps={'sqlite_sequence': {'1:1': [['seq', 'name']], 'N:1': []}, 'cards': {'1:1': [['mtgjsonV4Id', 'id'], ['uuid', 'id'], ['uuid', 'mtgjsonV4Id']], 'N:1': [['id', 'artist'], ['id', 'asciiName'], ['id', 'availability'], ['id', 'borderColor'], ['id', 'cardKingdomFoilId'], ['id', 'cardKingdomId'], ['id', 'colorIdentity'], ['id', 'colorIndicator'], ['id', 'colors'], ['id', 'convertedManaCost'], ['id', 'duelDeck'], ['id', 'edhrecRank'], ['id', 'faceConvertedManaCost'], ['id', 'faceName'], ['id', 'flavorName'], ['id', 'flavorText'], ['id', 'frameEffects'], ['id', 'frameVersion'], ['id', 'hand'], ['id', 'hasAlternativeDeckLimit'], ['id', 'hasContentWarning'], ['id', 'hasFoil'], ['id', 'hasNonFoil'], ['id', 'isAlternative'], ['id', 'isFullArt'], ['id', 'isOnlineOnly'], ['id', 'isOversized'], ['id', 'isPromo'], ['id', 'isReprint'], ['id', 'isReserved'], ['id', 'isStarter'], ['id', 'isStorySpotlight'], ['id', 'isTextless'], ['id', 'isTimeshifted'], ['id', 'keywords'], ['id', 'layout'], ['id', 'leadershipSkills'], ['id', 'life'], ['id', 'loyalty'], ['id', 'manaCost'], ['id', 'mcmId'], ['id', 'mcmMetaId'], ['id', 'mtgArenaId'], ['id', 'mtgoFoilId'], ['id', 'mtgoId'], ['id', 'multiverseId'], ['id', 'name'], ['id', 'number'], ['id', 'originalReleaseDate'], ['id', 'originalText'], ['id', 'originalType'], ['id', 'otherFaceIds'], ['id', 'power'], ['id', 'printings'], ['id', 'promoTypes'], ['id', 'purchaseUrls'], ['id', 'rarity'], ['id', 'scryfallId'], ['id', 'scryfallIllustrationId'], ['id', 'scryfallOracleId'], ['id', 'setCode'], ['id', 'side'], ['id', 'subtypes'], ['id', 'supertypes'], ['id', 'tcgplayerProductId'], ['id', 'text'], ['id', 'toughness'], ['id', 'type'], ['id', 'types'], ['id', 'variations'], ['id', 'watermark'], ['mtgjsonV4Id', 'artist'], ['uuid', 'artist'], ['mtgjsonV4Id', 'asciiName'], ['name', 'asciiName'], ['scryfallId', 'asciiName'], ['scryfallOracleId', 'asciiName'], ['uuid', 'asciiName'], ['availability', 'isOnlineOnly'], ['mtgjsonV4Id', 'availability'], ['scryfallId', 'availability'], ['uuid', 'availability'], ['mtgjsonV4Id', 'borderColor'], ['scryfallId', 'borderColor'], ['uuid', 'borderColor'], ['mtgjsonV4Id', 'cardKingdomFoilId'], ['purchaseUrls', 'cardKingdomFoilId'], ['uuid', 'cardKingdomFoilId'], ['mtgjsonV4Id', 'cardKingdomId'], ['purchaseUrls', 'cardKingdomId'], ['uuid', 'cardKingdomId'], ['mtgjsonV4Id', 'colorIdentity'], ['name', 'colorIdentity'], ['scryfallId', 'colorIdentity'], ['scryfallOracleId', 'colorIdentity'], ['uuid', 'colorIdentity'], ['mtgjsonV4Id', 'colorIndicator'], ['scryfallIllustrationId', 'colorIndicator'], ['uuid', 'colorIndicator'], ['mtgjsonV4Id', 'colors'], ['uuid', 'colors'], ['mtgjsonV4Id', 'convertedManaCost'], ['scryfallId', 'convertedManaCost'], ['scryfallOracleId', 'convertedManaCost'], ['uuid', 'convertedManaCost'], ['mtgjsonV4Id', 'duelDeck'], ['multiverseId', 'duelDeck'], ['purchaseUrls', 'duelDeck'], ['scryfallId', 'duelDeck'], ['uuid', 'duelDeck'], ['edhrecRank', 'hasAlternativeDeckLimit'], ['edhrecRank', 'hasContentWarning'], ['mtgjsonV4Id', 'edhrecRank'], ['name', 'edhrecRank'], ['scryfallId', 'edhrecRank'], ['scryfallOracleId', 'edhrecRank'], ['uuid', 'edhrecRank'], ['faceName', 'faceConvertedManaCost'], ['mtgjsonV4Id', 'faceConvertedManaCost'], ['otherFaceIds', 'faceConvertedManaCost'], ['uuid', 'faceConvertedManaCost'], ['mtgjsonV4Id', 'faceName'], ['uuid', 'faceName'], ['mtgjsonV4Id', 'flavorName'], ['purchaseUrls', 'flavorName'], ['scryfallId', 'flavorName'], ['uuid', 'flavorName'], ['mtgjsonV4Id', 'flavorText'], ['uuid', 'flavorText'], ['mtgjsonV4Id', 'frameEffects'], ['scryfallId', 'frameEffects'], ['uuid', 'frameEffects'], ['mtgjsonV4Id', 'frameVersion'], ['scryfallId', 'frameVersion'], ['uuid', 'frameVersion'], ['mtgjsonV4Id', 'hand'], ['name', 'hand'], ['scryfallId', 'hand'], ['scryfallIllustrationId', 'hand'], ['scryfallOracleId', 'hand'], ['uuid', 'hand'], ['mtgjsonV4Id', 'hasAlternativeDeckLimit'], ['name', 'hasAlternativeDeckLimit'], ['scryfallId', 'hasAlternativeDeckLimit'], ['scryfallIllustrationId', 'hasAlternativeDeckLimit'], ['scryfallOracleId', 'hasAlternativeDeckLimit'], ['text', 'hasAlternativeDeckLimit'], ['uuid', 'hasAlternativeDeckLimit'], ['mtgjsonV4Id', 'hasContentWarning'], ['name', 'hasContentWarning'], ['scryfallId', 'hasContentWarning'], ['scryfallIllustrationId', 'hasContentWarning'], ['scryfallOracleId', 'hasContentWarning'], ['uuid', 'hasContentWarning'], ['mtgjsonV4Id', 'hasFoil'], ['scryfallId', 'hasFoil'], ['uuid', 'hasFoil'], ['mtgjsonV4Id', 'hasNonFoil'], ['scryfallId', 'hasNonFoil'], ['uuid', 'hasNonFoil'], ['mtgjsonV4Id', 'isAlternative'], ['purchaseUrls', 'isAlternative'], ['scryfallId', 'isAlternative'], ['uuid', 'isAlternative'], ['variations', 'isAlternative'], ['mtgjsonV4Id', 'isFullArt'], ['scryfallId', 'isFullArt'], ['uuid', 'isFullArt'], ['mtgjsonV4Id', 'isOnlineOnly'], ['scryfallId', 'isOnlineOnly'], ['setCode', 'isOnlineOnly'], ['uuid', 'isOnlineOnly'], ['mtgjsonV4Id', 'isOversized'], ['scryfallId', 'isOversized'], ['uuid', 'isOversized'], ['mtgjsonV4Id', 'isPromo'], ['scryfallId', 'isPromo'], ['uuid', 'isPromo'], ['mtgjsonV4Id', 'isReprint'], ['scryfallId', 'isReprint'], ['uuid', 'isReprint'], ['mtgjsonV4Id', 'isReserved'], ['name', 'isReserved'], ['scryfallId', 'isReserved'], ['scryfallOracleId', 'isReserved'], ['uuid', 'isReserved'], ['mtgjsonV4Id', 'isStarter'], ['scryfallId', 'isStarter'], ['uuid', 'isStarter'], ['mcmId', 'isStorySpotlight'], ['mtgjsonV4Id', 'isStorySpotlight'], ['purchaseUrls', 'isStorySpotlight'], ['scryfallId', 'isStorySpotlight'], ['tcgplayerProductId', 'isStorySpotlight'], ['uuid', 'isStorySpotlight'], ['mtgjsonV4Id', 'isTextless'], ['scryfallId', 'isTextless'], ['uuid', 'isTextless'], ['mtgjsonV4Id', 'isTimeshifted'], ['scryfallId', 'isTimeshifted'], ['uuid', 'isTimeshifted'], ['mtgjsonV4Id', 'keywords'], ['uuid', 'keywords'], ['mtgjsonV4Id', 'layout'], ['name', 'layout'], ['scryfallId', 'layout'], ['scryfallIllustrationId', 'layout'], ['scryfallOracleId', 'layout'], ['uuid', 'layout'], ['mtgjsonV4Id', 'leadershipSkills'], ['uuid', 'leadershipSkills'], ['mtgjsonV4Id', 'life'], ['name', 'life'], ['scryfallId', 'life'], ['scryfallIllustrationId', 'life'], ['scryfallOracleId', 'life'], ['uuid', 'life'], ['mtgjsonV4Id', 'loyalty'], ['text', 'loyalty'], ['uuid', 'loyalty'], ['mtgjsonV4Id', 'manaCost'], ['uuid', 'manaCost'], ['mtgjsonV4Id', 'mcmId'], ['uuid', 'mcmId'], ['mtgjsonV4Id', 'mcmMetaId'], ['purchaseUrls', 'mcmMetaId'], ['uuid', 'mcmMetaId'], ['mtgjsonV4Id', 'mtgArenaId'], ['scryfallId', 'mtgArenaId'], ['uuid', 'mtgArenaId'], ['mtgjsonV4Id', 'mtgoFoilId'], ['mtgjsonV4Id', 'mtgoId'], ['mtgjsonV4Id', 'multiverseId'], ['mtgjsonV4Id', 'name'], ['mtgjsonV4Id', 'number'], ['mtgjsonV4Id', 'originalReleaseDate'], ['mtgjsonV4Id', 'originalText'], ['mtgjsonV4Id', 'originalType'], ['mtgjsonV4Id', 'otherFaceIds'], ['mtgjsonV4Id', 'power'], ['mtgjsonV4Id', 'printings'], ['mtgjsonV4Id', 'promoTypes'], ['mtgjsonV4Id', 'purchaseUrls'], ['mtgjsonV4Id', 'rarity'], ['mtgjsonV4Id', 'scryfallId'], ['mtgjsonV4Id', 'scryfallIllustrationId'], ['mtgjsonV4Id', 'scryfallOracleId'], ['mtgjsonV4Id', 'setCode'], ['mtgjsonV4Id', 'side'], ['mtgjsonV4Id', 'subtypes'], ['mtgjsonV4Id', 'supertypes'], ['mtgjsonV4Id', 'tcgplayerProductId'], ['mtgjsonV4Id', 'text'], ['mtgjsonV4Id', 'toughness'], ['mtgjsonV4Id', 'type'], ['mtgjsonV4Id', 'types'], ['mtgjsonV4Id', 'variations'], ['mtgjsonV4Id', 'watermark'], ['scryfallId', 'mtgoFoilId'], ['uuid', 'mtgoFoilId'], ['scryfallId', 'mtgoId'], ['uuid', 'mtgoId'], ['uuid', 'multiverseId'], ['name', 'printings'], ['scryfallId', 'name'], ['scryfallOracleId', 'name'], ['uuid', 'name'], ['scryfallId', 'number'], ['uuid', 'number'], ['uuid', 'originalReleaseDate'], ['uuid', 'originalText'], ['uuid', 'originalType'], ['otherFaceIds', 'side'], ['uuid', 'otherFaceIds'], ['uuid', 'power'], ['scryfallId', 'printings'], ['scryfallOracleId', 'printings'], ['uuid', 'printings'], ['uuid', 'promoTypes'], ['purchaseUrls', 'tcgplayerProductId'], ['uuid', 'purchaseUrls'], ['scryfallId', 'rarity'], ['uuid', 'rarity'], ['scryfallId', 'scryfallOracleId'], ['scryfallId', 'setCode'], ['scryfallId', 'tcgplayerProductId'], ['uuid', 'scryfallId'], ['scryfallId', 'watermark'], ['uuid', 'scryfallIllustrationId'], ['uuid', 'scryfallOracleId'], ['uuid', 'setCode'], ['uuid', 'side'], ['type', 'subtypes'], ['uuid', 'subtypes'], ['type', 'supertypes'], ['uuid', 'supertypes'], ['uuid', 'tcgplayerProductId'], ['uuid', 'text'], ['uuid', 'toughness'], ['type', 'types'], ['uuid', 'type'], ['uuid', 'types'], ['uuid', 'variations'], ['uuid', 'watermark']]}, 'foreign_data': {'1:1': [], 'N:1': [['id', 'flavorText'], ['id', 'language'], ['id', 'multiverseid'], ['id', 'name'], ['id', 'text'], ['id', 'type'], ['id', 'uuid']]}, 'legalities': {'1:1': [], 'N:1': [['id', 'format'], ['id', 'status'], ['id', 'uuid']]}, 'sets': {'1:1': [['code', 'id'], ['name', 'id'], ['name', 'code'], ['mcmName', 'mcmId']], 'N:1': [['id', 'baseSetSize'], ['id', 'block'], ['id', 'booster'], ['id', 'isFoilOnly'], ['id', 'isForeignOnly'], ['id', 'isNonFoilOnly'], ['id', 'isOnlineOnly'], ['id', 'isPartialPreview'], ['id', 'keyruneCode'], ['id', 'mcmId'], ['id', 'mcmIdExtras'], ['id', 'mcmName'], ['id', 'mtgoCode'], ['id', 'parentCode'], ['id', 'releaseDate'], ['id', 'tcgplayerGroupId'], ['id', 'totalSetSize'], ['id', 'type'], ['code', 'baseSetSize'], ['name', 'baseSetSize'], ['code', 'block'], ['name', 'block'], ['code', 'booster'], ['booster', 'mcmIdExtras'], ['name', 'booster'], ['code', 'isFoilOnly'], ['code', 'isForeignOnly'], ['code', 'isNonFoilOnly'], ['code', 'isOnlineOnly'], ['code', 'isPartialPreview'], ['code', 'keyruneCode'], ['code', 'mcmId'], ['code', 'mcmIdExtras'], ['code', 'mcmName'], ['code', 'mtgoCode'], ['code', 'parentCode'], ['code', 'releaseDate'], ['code', 'tcgplayerGroupId'], ['code', 'totalSetSize'], ['code', 'type'], ['name', 'isFoilOnly'], ['name', 'isForeignOnly'], ['name', 'isNonFoilOnly'], ['name', 'isOnlineOnly'], ['keyruneCode', 'isPartialPreview'], ['name', 'isPartialPreview'], ['releaseDate', 'isPartialPreview'], ['tcgplayerGroupId', 'isPartialPreview'], ['name', 'keyruneCode'], ['mcmId', 'mcmIdExtras'], ['name', 'mcmId'], ['mcmName', 'mcmIdExtras'], ['mtgoCode', 'mcmIdExtras'], ['name', 'mcmIdExtras'], ['tcgplayerGroupId', 'mcmIdExtras'], ['name', 'mcmName'], ['name', 'mtgoCode'], ['name', 'parentCode'], ['name', 'releaseDate'], ['name', 'tcgplayerGroupId'], ['name', 'totalSetSize'], ['name', 'type']]}, 'set_translations': {'1:1': [], 'N:1': [['id', 'language'], ['id', 'setCode'], ['id', 'translation']]}, 'rulings': {'1:1': [], 'N:1': [['id', 'date'], ['id', 'text'], ['id', 'uuid']]}}
        DDL = db_model.get_database_ddls()
        groups = split_database_ratio_maps(database_ratio_maps, max_pairs_per_group=80)
        database_ratio_text = ""
        for idx, group in enumerate(groups):
            # 用语言描述是会好点
            prompt = f'''Task Overview:
Below, you are provided with a database schema and the proportional relationship between attributes in json format. Please describe the proportional relationship in natural language format. 
For example, "the relationship from satscores.sname to satcores.rttype is N:1, indicating that multiple schools with the same name may belong to the same reporting type." Each relationship requires line breaks, and there is no need to output additional analysis content beyond that.

Output example:
The relationship from frpm.`Charter School Number` to frpm.`Charter Funding Type` is N:1, indicating that multiple charter school numbers may belong to the same charter funding type.
the relationship from satscores.sname to satcores.rtype is N:1, indicating that multiple schools with the same name may belong to the same reporting type.

Database Schema:
{DDL}

Please keep the necessary '`' symbol in column name and table name to ensure the correct SQL.

Proportional Relationship between Attributes:
{group}
'''
            # print(f"prompt长度: {len(prompt)}")
            # print(f"使用的model: {opt.model_name}")
            client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'), base_url="https://www.dmxapi.com/v1/")
            response = client.chat.completions.create(
                    model=opt.model_name,
                    messages=[
                        {"role": "system", "content": "You are a data science expert."},
                        {"role": "user", "content": prompt}
                    ],
                    stream=False
                )
            group_ratio_text = response.choices[0].message.content
            group_ratio_text = '\n'.join([line for line in group_ratio_text.split('\n') if line.strip()])  # 去空行
            group_len=len(group_ratio_text.split('\n'))
            group_count = sum(len(v["1:1"]) + len(v["N:1"]) for v in group.values())
            group_retry=3
            while(group_len != group_count and group_retry!=0):          #如果返回长度达不到要求就分组重试。
                print(f"分组{idx}长度达不到要求：\n{group_len}：{group_count}\n")
                group_ratio_text=""
                arrs = split_database_ratio_maps(group,group_count//2)
                print(f"因为分组{idx}长度达不到要求，划分成{group_count/2}的{len(arrs)}个组")
                for idxx, arr in enumerate(arrs):
                    prompt = f'''Task Overview:
Below, you are provided with a database schema and the proportional relationship between attributes in json format. Please describe the proportional relationship in natural language format. 
For example, "the relationship from satscores.sname to satcores.rttype is N:1, indicating that multiple schools with the same name may belong to the same reporting type." Each relationship requires line breaks, and there is no need to output additional analysis content beyond that.

Output example:
The relationship from frpm.`Charter School Number` to frpm.`Charter Funding Type` is N:1, indicating that multiple charter school numbers may belong to the same charter funding type.
the relationship from satscores.sname to satcores.rtype is N:1, indicating that multiple schools with the same name may belong to the same reporting type.

Database Schema:
{DDL}

Please keep the necessary '`' symbol in column name and table name to ensure the correct SQL.

Proportional Relationship between Attributes:
{arr}
'''
                    response = client.chat.completions.create(
                            model=opt.model_name,
                            messages=[
                                {"role": "system", "content": "You are a data science expert."},
                                {"role": "user", "content": prompt}
                            ],
                            stream=False
                        )
                    arr_ratio_text = ""
                    arr_ratio_text = response.choices[0].message.content
                    arr_ratio_text = '\n'.join([line for line in arr_ratio_text.split('\n') if line.strip()])  # 去空行
                    arr_len = len(arr_ratio_text.split('\n'))
                    print(f"------------------------{idx}分组{idxx}的长度：{arr_len}\n")
                    group_ratio_text += "\n"+arr_ratio_text + "\n\n"
                group_ratio_text = '\n'.join([line for line in group_ratio_text.split('\n') if line.strip()])  # 去空行
                group_len=len(group_ratio_text.split('\n'))
                group_retry -= 1

            print(f"****************************************分组{idx}LLM输出总行数:{group_len}\n")
            database_ratio_text += group_ratio_text + "\n\n"
        database_ratio_text = '\n'.join([line for line in database_ratio_text.split('\n') if line.strip()])  #去掉空行
        # print(f"LLM 输入：\n{prompt}")                    
        # print(f"LLM 输出text：\n{database_ratio_text}")
        
        database_ratio_text_list = database_ratio_text.split('\n')
        print(f"输出总行数：{len(database_ratio_text_list)}")
        # 3. 处理冗余情况，和数据不一致情况，数据库设计规范的问题，这个很难从数值上确定是不是同一列，可能需要大模型，当然数值上也可以缩小范围
        prompt = f'''Task Overview:
Below, you are provided with a database schema. Your task is to understand the schema and identify redundant columns.

Database Schema:
{DDL}

Output Format:
In your answer, please enclose the generated SQL query in a code block:
```json
[["table1.column1", "table2.column2", "table1.key1", "table2.key2"], ...]
```
Where ["table1. column1", "table2. column2", "table1.key1", "table2.key2"] means that "table1.column1" and "table2.column2" have the same meaning and may be redundant. 'table1.key1' represents the Join key for column 'table1.column1', and 'table2.key2' represents the Join key for column 'table2.column2'. The core function of association keys is to serve as the basis for data matching between different tables. By connecting rows with the same value, it enables us to cross the boundaries of the table and integrate scattered stored information into a meaningful and coherent data view. Here, you can determine whether there is data inconsistency between columns by using association keys.

if '`' symbol exists in the column name and table name of Database Schema, keep it.

Take a deep breath and think step by step to find the identify redundant columns.
'''
        client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'), base_url="https://www.dmxapi.com/v1/")
        response = client.chat.completions.create(
                model=opt.model_name,
                messages=[
                    {"role": "system", "content": "You are a data science expert."},
                    {"role": "user", "content": prompt}
                ],
                stream=False
            )
        result = response.choices[0].message.content                                    
        # result = '\n'.join([line for line in result.split('\n') if line.strip()])
        # print(f"LLM 输入：\n{prompt}")
        # print(f"LLM 输出text：\n{result}")
        # with open("log.txt", "a", encoding="utf-8") as f:
        #     f.write(f"开始对{sqlite_dataset_name}处理-----------------------------------")
            #f.write(f"LLM 输入：\n{prompt}\n")
            #f.write(f"LLM 输出text：\n{result}\n\n")
        match = re.search(r"```json\s*(.*?)\s*```", result, re.DOTALL)            
        redundant_columns_list = []
        if match:
            json_str = match.group(1).strip()
            try:
                redundant_columns_list = json.loads(json_str)
            except Exception as e:
                print(e)
        #redundant_columns_list = [['client.district_id', 'account.district_id', 'client.client_id', 'disp.client_id'], ['account.district_id', 'district.district_id', 'account.district_id', 'district.district_id'], ['account.account_id', 'loan.account_id', 'account.account_id', 'loan.account_id'], ['account.account_id', 'order.account_id', 'account.account_id', 'order.account_id'], ['account.account_id', 'trans.account_id', 'account.account_id', 'trans.account_id'], ['disp.account_id', 'account.account_id', 'disp.account_id', 'account.account_id'], ['disp.client_id', 'client.client_id', 'disp.client_id', 'client.client_id']]
        # with open("log.txt", "a", encoding="utf-8") as f:
        #     f.write(f"\nLLM输出可能冗余列（{len(redundant_columns_list)}个）：\n")
        #     f.write(f"\nLLM 输出json：\n{redundant_columns_list}")
        consistency_redundant_columns_list = []
        inconsistency_redundant_columns_list = []
        if redundant_columns_list:
            for redundant_item in redundant_columns_list:
                table1, column1 = redundant_item[0].split('.')
                table2, column2 = redundant_item[1].split('.')
                # key1 = redundant_item[2].split('.')[1]
                # key2 = redundant_item[3].split('.')[1]
                key1 = redundant_item[2].split('.')[1] if redundant_item[2] and '.' in redundant_item[2] else None
                key2 = redundant_item[3].split('.')[1] if redundant_item[3] and '.' in redundant_item[3] else None
                # table1 = format_table_column_name(table1)
                # table2 = format_table_column_name(table2)
                # column1 = format_table_column_name(column1)
                # column2 = format_table_column_name(column2)
                # key1 = format_table_column_name(key1)
                # key2 = format_table_column_name(key2)

                
                try:
                    if redundant_item[2] == redundant_item[3] and table1 == table2:  # 同一个表中的冗余
                        sql_statement = f'''SELECT
        {table1}.{key1},
        {table1}.{column1},
        {table2}.{key2},
        {table2}.{column2}
        FROM
            {table1} 
        WHERE
            {table1}.{column1} <> {table2}.{column2}
        '''
                    else:
                        sql_statement = f'''SELECT
        {table1}.{key1},
        {table1}.{column1},
        {table2}.{key2},
        {table2}.{column2}
        FROM
            {table1} 
        JOIN
            {table2} ON {table1}.{key1} = {table2}.{key2}
        WHERE
            {table1}.{column1} <> {table2}.{column2}
        '''
                    execute_result = db_model.execute_sql(sql_statement)
                    if execute_result.empty: 
                        consistency_redundant_columns_list.append(redundant_item)
                    else:
                        inconsistency_redundant_columns_list.append(redundant_item)
                except Exception as e:
                    print(f'fail to check {redundant_item}')
                    print(e)
    
        # print(f"一致性列：\n{consistency_redundant_columns_list}")
        # print(f"不一致性列：\n{inconsistency_redundant_columns_list}")
        # with open("log.txt", "a", encoding="utf-8") as f:
        #     f.write(f"\n一致性列（{len(consistency_redundant_columns_list)}个，数据完全一致）：\n")
        #     f.write(f"一致性列：\n{consistency_redundant_columns_list}")
        #     f.write(f"\n不一致性列（{len(inconsistency_redundant_columns_list)}个，数据存在差异）：\n")
        #     f.write(f"不一致性列：\n{inconsistency_redundant_columns_list}")
        #     f.write(f"\n********************************************结束一个************************\n")


        # 4. 最后，组合这些信息 
        # input schema 和 question 描述直接在之前的arctic上面加
        # v1 这是直接加json格式的函数依赖关系
        database_information = "The following are the cardinality relationships between attributes. A many-to-one (N:1) relationship exists between attribute A and attribute B ([A, B]) if A functionally determines B (i.e., A → B), but B does not functionally determine A. In this case, each value of A can be associated with multiple values of B, but each value of B corresponds to exactly one value of A. A one-to-one (1:1) relationship exists between attribute A and attribute B if both A → B and B → A hold. This means each value of A corresponds to exactly one value of B, and vice versa. Any attribute pairs not mentioned are assumed to have a many-to-many (N:N) relationship. For example, the ratio between student ID and age is N:1, as there may be multiple students with the same age. \n" 
        for table in database_ratio_maps:
            database_information += f"In table `{table}`: \n"
            for key, value in database_ratio_maps[table].items():
                database_information += f"{key}: {value} \n"
        database_information += "Not all many-to-to (N:1) relationships are listed here. Whenever A is a primary key and B is any other attribute, the relationship between A and B is considered to be many-to-one by default.\nThese cardinality relationships will influence how you generate SQL queries. \n"

        database_information += "In addition, **there are some redundant columns here, but their stored data is consistent. You can use one of them freely.**\n"
        for consistency_redundant_columns in consistency_redundant_columns_list:
            database_information += f"{consistency_redundant_columns[0]} and {consistency_redundant_columns[1]}\n"
        
        database_information += "**There are also some redundant columns, but the data they store is inconsistent. When querying involving these columns, you need to carefully consider which column to use.**\n"
        for inconsistency_redundant_columns in inconsistency_redundant_columns_list:
            database_information += f"{inconsistency_redundant_columns[0]} and {inconsistency_redundant_columns[1]}\n"

        table_desc_dict[sqlite_dataset_name]['fd_list'] = database_ratio_text_list
        table_desc_dict[sqlite_dataset_name]['consistency_redundant_columns'] = consistency_redundant_columns_list
        table_desc_dict[sqlite_dataset_name]['inconsistency_redundant_columns'] = inconsistency_redundant_columns_list
        table_desc_dict[sqlite_dataset_name]['null_column'] = null_column_list

        # 保存table desc文件
        with open(opt.table_desc_file, 'w', encoding='utf-8') as f:
            json.dump(table_desc_dict, f, indent=2, ensure_ascii=False)
        


def get_similar_column_by_score(meaning_file,table_desc_file):
    file_path = meaning_file
    save_path = table_desc_file
    if not os.path.exists(meaning_file):                           #meaning file跳过逻辑
        print(f'meaning_file 不存在：{meaning_file}，跳过该数据处理')
        return  # 直接跳过后续处理
    threshold = 0.8
    model_path = "../model/sentence-transformers/all-MiniLM-L6-v2"
    #model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
    model = SentenceTransformer(model_path)
    print('Success load model ------')
    print("开始计算相似列...")
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    with open(save_path, 'r', encoding='utf-8') as f:
        save_datas = json.load(f)

    for dataset_name, tables in save_datas.items():
        print(f'开始处理{dataset_name} ============')
        sentences = []
        keys = []
        for key, sent in data.items():
            if dataset_name == key.split('|')[0]:
                sentences.append(f"{key}:{sent}")
                keys.append(key)
        print(f'共有 {len(sentences)} 个句子需要比较')
        # 编码所有句子
        print('开始编码句子...')
        embeddings = model.encode(sentences, convert_to_tensor=True)
        print('编码完成')
    
        # 计算余弦相似度矩阵
        print('计算相似度矩阵...')
        cosine_scores = util.cos_sim(embeddings, embeddings)
    
        # 找出相似度大于阈值的句子对
        similar_pairs = []
        n = len(sentences)
    
        for i in range(n):
            for j in range(i + 1, n):  # 只比较上三角,避免重复
                score = cosine_scores[i][j].item()
                if score >= threshold and keys[i].split('|')[:-1] != keys[j].split('|')[:-1]:
                    similar_pairs.append([
                        '.'.join(keys[i].split('|')[1:]),
                        '.'.join(keys[j].split('|')[1:]),
                        round(score, 4)  # 保留4位小数
                    ])
    
        print(f'找到 {len(similar_pairs)} 对相似句子(阈值>={threshold})')

        save_datas[dataset_name]['similar_pairs'] = similar_pairs

 
    # 保存结果
    with open(save_path, 'w', encoding='utf-8') as f:
        json.dump(save_datas, f, ensure_ascii=False, indent=2)

    print(f'结果已保存到: {save_path}')


