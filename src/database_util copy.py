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
    print(enhance_schema_json)
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

def enhance_schema_with_column_description(schema_text:str, db_path: str) -> str:
    conn = sqlite3.connect(db_path) 
    cursor = conn.cursor()

    query = "SELECT name FROM sqlite_master WHERE type='table' AND name!='sqlite_sequence';"
    tables = pd.read_sql_query(query, conn)
    tables = tables['name'].tolist()
    print("数据库中的表:", tables)
    table_schema_text_list = split_ddl(schema_text)
    print(len(table_schema_text_list), len(tables))
    assert len(table_schema_text_list) == len(tables)

    table_schema_text_dict = {}
    for table in tables:
        table = format_table_column_name(table)
        for table_schema_t in table_schema_text_list:
            if f'CREATE TABLE {table} ' in table_schema_t:
                table_schema_text_dict[table] = table_schema_t
                break
    print(len(table_schema_text_dict), len(tables))
    assert len(table_schema_text_dict) == len(tables)

    new_schema_text = ""
    for table in tables:
        print(f"正在处理表{table} --------------------")
        table = format_table_column_name(table)
        single_table_schema_text = table_schema_text_dict[table]
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
        if(table == "posts" or table == "foreign_data"):
            print(f'LLM处理{table}输入: -------------')
            print(prompt)

        MAX_TRY_TIME = 3
        status = 'Fail'
        while MAX_TRY_TIME>0 and status=='Fail':
            table_with_comments_reponse = call_llm(prompt)
            if(table == "posts" or table == "foreign_data"):
                print(f'LLM处理{table}输出: -------------')             
                print(table_with_comments_reponse)                   
            table_with_comments = extract_sql_from_text(table_with_comments_reponse)
            if(table == "posts" or table == "foreign_data"):
                print(f'LLM{table}输出提取: -------------')             
                print(table_with_comments) 
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
        # 遍历所有表
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
        
        DDL = db_model.get_database_ddls()

        # 用语言描述是会好点
        prompt = f'''Task Overview:
Below, you are provided with a database schema and the proportional relationship between attributes in json format. Please describe the proportional relationship in natural language format. 
For example, "the relationship from satscores.sname to satcores.rttype is N:1, indicating that multiple schools with the same name may belong to the same reporting type." Each relationship requires line breaks, and there is no need to output additional analysis content beyond that.

Database Schema:
{DDL}

Please keep the necessary '`' symbol in column name and table name to ensure the correct SQL.

Proportional Relationship between Attributes:
{database_ratio_maps}
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
        database_ratio_text = response.choices[0].message.content
        # print(f"LLM 输入：\n{prompt}")                    去掉打印
        # print(f"LLM 输出text：\n{database_ratio_text}")

        database_ratio_text_list = database_ratio_text.split('\n')

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
        print(f"LLM 输入：\n{prompt}")
        print(f"LLM 输出text：\n{result}")
        match = re.search(r"```json\s*(.*?)\s*```", result, re.DOTALL)
        redundant_columns_list = []
        if match:
            json_str = match.group(1).strip()
            try:
                redundant_columns_list = json.loads(json_str)
            except Exception as e:
                print(e)
        # redundant_columns_list = [["bond.molecule_id", "atom.molecule_id", "bond.bond_id", "atom.atom_id"]]

        print(f"LLM 输出json：\n{redundant_columns_list}")
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
    
        print(f"一致性列：\n{consistency_redundant_columns_list}")
        print(f"不一致性列：\n{inconsistency_redundant_columns_list}")


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
        


def get_similar_column_by_score():
    file_path = '../output/bird/dev/column_meaning.json'
    save_path = '../output/bird/dev/table_desc.json'
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


