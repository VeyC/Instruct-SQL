import re
import pandas as pd
import warnings
warnings.filterwarnings('ignore')
from itertools import combinations
import snowflake.connector
import sqlite3
def format_table_column_name(name: str) -> str:
    '''格式化table和column的名称，有时候会少了`'''
    if not name or not isinstance(name, str):  # 这一行保证None/空/非字符串直接原样返回
        return name
    if (name.startswith('`') and name.endswith('`')) or (name.startswith('"') and name.endswith('"')):
        return name
    sqlite_keywords = {
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

class Database:
    def __init__(self, db_path, table_name=None):
        """
        初始化DATABASE算法类
        Args:
            db_path: SQLite数据库路径
            table_name: 要分析的表名（如果为None，会列出所有表让用户选择）
        """
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.table_name = table_name
        self.data = None
        
        if table_name is None:
            self.list_tables()
        else:
            self.load_data()
    
    def list_tables(self):
        """列出数据库中的所有表"""
        query = "SELECT name FROM sqlite_master WHERE type='table';"
        tables = pd.read_sql_query(query, self.conn)
        print("数据库中的表:")
        for i, table in enumerate(tables['name']):
            print(f"{i+1}. {table}")
        return tables['name'].tolist()
    
    def get_database_ddls(self) -> str:
        """得到数据库的DDL语句"""
        query = "SELECT sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        # 仅获取用户表（排除 sqlite 内部表）
        ddl = pd.read_sql_query(query, self.conn)
        full_ddl = "\n\n".join(ddl['sql'].tolist())
        return full_ddl
    
    def execute_sql(self, query):
        res = pd.read_sql_query(query, self.conn)
        return res
    
    def set_table(self, table_name):
        """设置要分析的表"""
        self.table_name = table_name
        self.load_data()
        
        if table_name == 'connected':
            self.remove_symmetric_duplicates_from_data()

        
    def remove_symmetric_duplicates_from_data(self):
        """在内存中删除connected表数据的对称重复记录"""
        try:
            print("开始在内存中删除对称重复记录...")
            
            original_shape = self.data.shape
            print(f"去重前数据形状: {original_shape}")
            
            # 创建一个标识列来标记要保留的记录
            # 对于每对对称记录(A,B)和(B,A)，只保留字典序较小的那个
            mask = self.data['atom_id'] <= self.data['atom_id2']
            
            # 应用过滤
            self.data = self.data[mask].copy()
            
            new_shape = self.data.shape
            removed_count = original_shape[0] - new_shape[0]
            
            print(f"去重后数据形状: {new_shape}")
            print(f"删除了 {removed_count} 条对称重复记录")
            print(f"去重后前5行数据:")
            print(self.data.head())

        except Exception as e:
            print(f"删除对称重复记录时出错: {e}")

    
    def load_data(self):
        """从SQLite数据库加载数据"""
        if self.table_name is None:
            print("请先设置表名")
            return
        
        try:
            # 加载数据
            self.table_name = format_table_column_name(self.table_name)
            query = f"SELECT * FROM {self.table_name}"
            self.data = pd.read_sql_query(query, self.conn)
            print(f"成功加载表 '{self.table_name}'，数据形状: {self.data.shape}")
            print(f"列名: {list(self.data.columns)}")
            print(f"前5行数据:")
            print(self.data.head())
        except Exception as e:
            print(f"加载数据时出错: {e}")
    
    def get_table_schema(self):
        """获取表的结构信息"""
        if self.table_name is None:
            print("请先设置表名")
            return
        
        query = f"PRAGMA table_info({self.table_name})"
        schema = pd.read_sql_query(query, self.conn)
        print(f"表 '{self.table_name}' 的结构:")
        print(schema)
        return schema
    
    def get_table_attrs(self, table_name):
        query = f"PRAGMA table_info({table_name})"
        schema = pd.read_sql_query(query, self.conn)
        # 转换为字典结构
        schema_dict = {}
        for _, row in schema.iterrows():
            column_name = row['name']
            schema_dict[column_name] = {
                'type': row['type'],
                'notnull': bool(row['notnull']),
                'dflt_value': row['dflt_value'],
                'pk': bool(row['pk'])
            }
        return schema_dict
    
    def compute_entropy(self, LHS, RHS):
        """
        计算给定LHS和RHS的熵
        
        Args:
            LHS: 左侧属性列表
            RHS: 右侧属性
        
        Returns:
            熵值
        """
        if self.data is None:
            print("数据未加载")
            return float('inf')
        
        try:
            # 将数据转换为字符串以处理不同数据类型
            grouped_data = self.data.copy()
            for col in LHS + [RHS]:
                grouped_data[col] = grouped_data[col].astype(str)
            
            tmp = grouped_data.groupby(LHS)[RHS].nunique()
            entropy = (tmp > 1).sum()
            return entropy
        except Exception as e:
            print(f"计算熵时出错: {e}")
            return float('inf')
    
    def find_functional_dependencies(self, max_lhs_size=None):
        """
        使用TANE算法贪婪地找到函数依赖关系
        
        Args:
            max_lhs_size: LHS的最大大小限制（None表示无限制）
        
        Returns:
            函数依赖列表
        """
        if self.data is None:
            print("数据未加载，无法分析函数依赖")
            return []
        
        FD_list = []
        columns = list(self.data.columns)
        
        # 如果没有指定最大LHS大小，则使用列数
        if max_lhs_size is None:
            max_lhs_size = len(columns)
        
        print(f"开始分析函数依赖，最大LHS大小: {max_lhs_size}")
        
        # 从大小为2的组合开始，逐步增加到max_lhs_size+1
        for r in range(2, min(max_lhs_size + 2, len(columns) + 1)):
            print(f"正在分析大小为 {r} 的属性组合...")
            
            for comb in combinations(columns, r):
                for RHS in comb:
                    LHS = [col for col in comb if col != RHS]
                    
                    # 条件1: 检查是否已有更小的LHS能推导出相同的RHS
                    cond_1 = [r_t == RHS and len(set(LHS).intersection(set(l_t))) == len(l_t) 
                             for l_t, r_t in FD_list]
                    
                    # 条件2: 检查当前LHS是否包含已存在FD的所有属性
                    cond_2 = [set(l_t + [r_t]).intersection(set(LHS)) == set(l_t + [r_t]) 
                             for l_t, r_t in FD_list]
                    
                    if sum(cond_1) == 0 and sum(cond_2) == 0:
                        entropy = self.compute_entropy(LHS, RHS)
                        if entropy == 0:
                            FD_list.append([LHS, RHS])
                            print(f"发现函数依赖: {' '.join(LHS)} -> {RHS}")
        
        return FD_list
    
    def format_functional_dependencies(self, FD_list):
        """
        格式化函数依赖输出
        
        Args:
            FD_list: 函数依赖列表
        
        Returns:
            格式化的函数依赖字符串列表
        """
        formatted_fds = []
        for lhs, rhs in FD_list:
            lhs_str = ''.join(lhs) if isinstance(lhs, list) else lhs
            formatted_fds.append(f"{lhs_str} -> {rhs}")
        return formatted_fds
    
    def analyze_specific_table(self, table_name, max_lhs_size=3):
        """
        分析指定表的函数依赖
        
        Args:
            table_name: 表名
            max_lhs_size: LHS最大大小
        """
        print(f"\n=== 分析表: {table_name} ===")
        self.set_table(table_name)
        
        if self.data is not None:
            # 显示表结构
            print(f"\n表结构信息:")
            self.get_table_schema()
            
            # 寻找函数依赖
            print(f"\n开始寻找函数依赖...")
            fd_list = self.find_functional_dependencies(max_lhs_size)
            
            # 输出结果
            if fd_list:
                print(f"\n发现的函数依赖关系:")
                formatted_fds = self.format_functional_dependencies(fd_list)
                for i, fd in enumerate(formatted_fds, 1):
                    print(f"{i}. {fd}")
                print(f"\n总共发现 {len(fd_list)} 个函数依赖关系")
            else:
                print("\n未发现函数依赖关系")
            
            return fd_list
    

    def analyze_column_distribution(self, table_name=None):
        """
        分析表中所有列的值分布情况
        
        Args:
            table_name: 表名（如果为None，使用当前设置的表）
        
        Returns:
            dict: 每列的分布统计信息
                - unique_count: 不同值的个数
                - top_5_values: 出现次数最高的5个值及其频次
                - total_count: 总记录数
                - null_count: 空值数量
        """
        if table_name:
            temp_table = self.table_name
            self.set_table(table_name)
        
        if self.data is None:
            print("数据未加载，无法分析分布")
            return {}
        
        distribution_stats = {}
        
        print(f"\n=== 分析表 '{self.table_name}' 的列分布情况 ===")
        
        for column in self.data.columns:
            try:
                # 计算基本统计信息
                total_count = len(self.data)
                null_count = self.data[column].isnull().sum()
                non_null_data = self.data[column].dropna()
                
                # 计算不同值的个数
                unique_count = non_null_data.nunique()
                
                # 获取值的频次统计
                value_counts = non_null_data.value_counts()
                
                # 获取出现次数最高的5个值
                top_5_values = value_counts.head(5).to_dict()
                top_5_list = [(value, count) for value, count in top_5_values.items()]
                                
                # 存储统计信息
                distribution_stats[column] = {
                    'unique_count': unique_count,
                    # 'top_5_values': top_5_list,
                    'null_count': null_count.item(),
                    'null_ratio': null_count.item()/total_count
                }
               
                
            except Exception as e:
                print(f"分析列 '{column}' 时出错: {e}")
                distribution_stats[column] = {
                    'error': str(e),
                    'unique_count': 0,
                    'top_5_values': [],
                    'total_count': 0,
                    'null_count': 0
                }
        
        # 恢复原来的表设置
        if table_name and temp_table:
            self.table_name = temp_table
            self.load_data()
        
        return distribution_stats



    def check_null_values(self, table_name=None):
        """
        检查表中哪些列包含空值，哪些列不包含空值
        
        Args:
            table_name: 表名（如果为None，使用当前设置的表）
        
        Returns:
            dict: {
                'columns_with_nulls': [列名列表],
                'columns_without_nulls': [列名列表]
            }
        """
        if table_name:
            temp_table = self.table_name
            self.set_table(table_name)
        
        if self.data is None:
            print("数据未加载，无法检查空值")
            return {
                'columns_with_nulls': [],
                'columns_without_nulls': [],
                'null_details': {}
            }
        
        columns_with_nulls = []
        columns_without_nulls = []
        
        
        print(f"\n=== 检查表 '{self.table_name}' 的空值情况 ===")
        
        for column in self.data.columns:
            try:
                # 计算空值数量
                null_count = self.data[column].isnull().sum()
                
                # 分类列
                if null_count > 0:
                    columns_with_nulls.append(column)
                else:
                    columns_without_nulls.append(column)
                print(f"column: {column}, null_count:{null_count/len(self.data)}")
            except Exception as e:
                print(f"检查列 '{column}' 时出错: {e}")

        
        # 打印汇总信息
        print("-" * 70)
        print(f"包含空值的列数: {columns_with_nulls}")
        print(f"不包含空值的列数: {columns_without_nulls}")
        
        
        # 恢复原来的表设置
        if table_name and temp_table:
            self.table_name = temp_table
            self.load_data()
        
        result = {
            'columns_with_nulls': columns_with_nulls,
            'columns_without_nulls': columns_without_nulls
        }
        
        return result


    def close(self):
        """关闭数据库连接"""
        self.conn.close()
 





class SnowflakeDatabase(Database):
    def __init__(self, connection_params, database_name=None, database_ddl=None, table_name=None):
        """
        初始化Snowflake DATABASE算法类
        Args:
            database_name: Snowflake数据库名称
            table_name: 完全限定的表名，格式为 DATABASE.SCHEMA.TABLE（如果为None，会列出所有表）
            **connection_params: Snowflake连接参数
                - user: 用户名
                - password: 密码
                - account: 账户标识符
                - warehouse: 仓库名称
                - role: 角色（可选）
        """
        self.database_name = database_name
        self.database_ddl = database_ddl
        self.table_name = table_name
        self.data = None
        
        # 建立Snowflake连接
        try:
            self.conn = snowflake.connector.connect(
                **connection_params
            )
            print(f"成功连接到 Snowflake 数据库")
        except Exception as e:
            print(f"连接Snowflake时出错: {e}")
            raise
    
    def set_dataset(self, dataset_name, DDL):
        self.database_name = dataset_name
        self.database_ddl = DDL 
        print(f'成功连接数据库: {self.database_name}')
    
    def list_tables(self):
        """从DDL字符串中提取所有表名（返回完全限定名称）"""
        # 使用正则表达式匹配 CREATE TABLE 后的表名
        # 匹配格式: CREATE TABLE database.schema.table_name
        pattern = r'CREATE TABLE\s+([\w]+\.[\w]+\.[\w]+)\s*\('
        
        matches = re.findall(pattern, self.database_ddl, re.IGNORECASE)
        
        if matches:
            print(f"数据库 {self.database_name} 中的表:")
            for i, table in enumerate(matches):
                print(f"{i+1}. {table}")
            return matches
        else:
            print(f"在DDL中未找到任何表定义")
            return []
    
    def execute_sql(self, query):
        """执行SQL查询并返回结果"""
        try:
            res = pd.read_sql(query, self.conn)
            return res
        except Exception as e:
            print(f"执行SQL时出错: {e}")
            return pd.DataFrame()
    
    def set_table(self, table_name):
        """
        设置要分析的表
        Args:
            table_name: 完全限定的表名，格式为 DATABASE.SCHEMA.TABLE
        """
        self.table_name = table_name
        self.load_data()
        
    
    def load_data(self):
        """从Snowflake数据库加载数据"""
        if self.table_name is None:
            print("请先设置表名")
            return
        
        try:
            # 直接使用完全限定的表名
            self.table_name = format_table_column_name(self.table_name)
            query = f'SELECT * FROM {self.table_name} ORDER BY RANDOM() LIMIT 5000'
            self.data = pd.read_sql(query, self.conn)
            print(f"成功加载表 '{self.table_name}'，数据形状: {self.data.shape}")
            print(f"列名: {list(self.data.columns)}")

            # 随机抽取200行（固定随机种子）
            sample_size = min(200, len(self.data))
            self.data = self.data.sample(n=sample_size, random_state=42).reset_index(drop=True)
            print(f"抽样后数据形状: {self.data.shape}")

            print(f"前5行数据:")
            print(self.data.head())
        except Exception as e:
            print(f"加载数据时出错: {e}")
            self.data = None
    

    def get_table_attrs(self, table_name):
        """
        获取表的属性信息（以字典形式返回）
        Args:
            table_name: 完全限定的表名，格式为 DATABASE.SCHEMA.TABLE
        """
        # 解析完全限定表名
        parts = table_name.split('.')
        if len(parts) != 3:
            print(f"表名格式错误，应为 DATABASE.SCHEMA.TABLE，当前为: {table_name}")
            return {}
        
        db_name, schema_name, tbl_name = parts
        
        query = f"""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT
        FROM {db_name}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_CATALOG = '{db_name}'
        AND TABLE_SCHEMA = '{schema_name}'
        AND TABLE_NAME = '{tbl_name}'
        ORDER BY ORDINAL_POSITION
        """
        try:
            schema = pd.read_sql(query, self.conn)
            # 转换为字典结构
            schema_dict = {}
            for _, row in schema.iterrows():
                column_name = row['COLUMN_NAME']
                schema_dict[column_name] = {
                    'type': row['DATA_TYPE'],
                    'nullable': row['IS_NULLABLE'] == 'YES',
                    'default': row['COLUMN_DEFAULT']
                }
            return schema_dict
        except Exception as e:
            print(f"获取表属性时出错: {e}")
            return {}
    
    def analyze_specific_table(self, table_name, max_lhs_size=3):
        """
        分析指定表的函数依赖
        
        Args:
            table_name: 完全限定的表名，格式为 DATABASE.SCHEMA.TABLE
            max_lhs_size: LHS最大大小
        """
        print(f"\n=== 分析表: {table_name} ===")
        self.set_table(table_name)
        
        if self.data is not None:

            # 寻找函数依赖
            print(f"\n开始寻找函数依赖...")
            fd_list = self.find_functional_dependencies(max_lhs_size)
            
            # 输出结果
            if fd_list:
                print(f"\n发现的函数依赖关系:")
                formatted_fds = self.format_functional_dependencies(fd_list)
                for i, fd in enumerate(formatted_fds, 1):
                    print(f"{i}. {fd}")
                print(f"\n总共发现 {len(fd_list)} 个函数依赖关系")
            else:
                print("\n未发现函数依赖关系")
            
            return fd_list
        return []
    
    def analyze_column_distribution(self, table_name=None):
        """
        分析表中所有列的值分布情况
        
        Args:
            table_name: 完全限定的表名（如果为None，使用当前设置的表）
        
        Returns:
            dict: 每列的分布统计信息
                - unique_count: 不同值的个数
                - null_count: 空值数量
                - null_ratio: 空值比例
        """
        if table_name:
            temp_table = self.table_name
            self.set_table(table_name)
        
        if self.data is None:
            print("数据未加载，无法分析分布")
            return {}
        
        distribution_stats = {}
        
        print(f"\n=== 分析表 '{self.table_name}' 的列分布情况 ===")
        
        for column in self.data.columns:
            try:
                # 计算基本统计信息
                total_count = len(self.data)
                null_count = self.data[column].isnull().sum()
                non_null_data = self.data[column].dropna()
                
                # 计算不同值的个数
                unique_count = non_null_data.nunique()
                
                # 存储统计信息
                distribution_stats[column] = {
                    'unique_count': unique_count,
                    'null_count': int(null_count),
                    'null_ratio': float(null_count) / total_count if total_count > 0 else 0
                }
                
            except Exception as e:
                print(f"分析列 '{column}' 时出错: {e}")
                distribution_stats[column] = {
                    'error': str(e),
                    'unique_count': 0,
                    'null_count': 0,
                    'null_ratio': 0
                }
        
        # 恢复原来的表设置
        if table_name and temp_table:
            self.table_name = temp_table
            self.load_data()
        
        return distribution_stats
    
    def check_null_values(self, table_name=None):
        """
        检查表中哪些列包含空值，哪些列不包含空值
        
        Args:
            table_name: 完全限定的表名（如果为None，使用当前设置的表）
        
        Returns:
            dict: {
                'columns_with_nulls': [列名列表],
                'columns_without_nulls': [列名列表]
            }
        """
        if table_name:
            temp_table = self.table_name
            self.set_table(table_name)
        
        if self.data is None:
            print("数据未加载，无法检查空值")
            return {
                'columns_with_nulls': [],
                'columns_without_nulls': []
            }
        
        columns_with_nulls = []
        columns_without_nulls = []
        
        print(f"\n=== 检查表 '{self.table_name}' 的空值情况 ===")
        
        for column in self.data.columns:
            try:
                # 计算空值数量
                null_count = self.data[column].isnull().sum()
                null_ratio = null_count / len(self.data) if len(self.data) > 0 else 0
                
                # 分类列
                if null_count > 0:
                    columns_with_nulls.append(column)
                else:
                    columns_without_nulls.append(column)
                print(f"column: {column}, null_ratio: {null_ratio:.4f}")
            except Exception as e:
                print(f"检查列 '{column}' 时出错: {e}")
        
        # 打印汇总信息
        print("-" * 70)
        print(f"包含空值的列: {columns_with_nulls}")
        print(f"不包含空值的列: {columns_without_nulls}")
        
        # 恢复原来的表设置
        if table_name and temp_table:
            self.table_name = temp_table
            self.load_data()
        
        result = {
            'columns_with_nulls': columns_with_nulls,
            'columns_without_nulls': columns_without_nulls
        }
        
        return result
    
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
            print("Snowflake连接已关闭")

