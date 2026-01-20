
# 1. Question Analysis: Follow all rules mentioned in the question (such as format conversions, pattern matching, and value transformations, usually indicated by "refer to") when analysis the question.

def get_filter_ddl_agent_prompt(db_desc, question):
    FILTER_DDL_AGNET_PROMPT = f"""Follow the STEP to answer the question.
# STEP:
1. Question Analysis: When analyzing the question, you must strictly follow all the rules mentioned in the question (such as format conversions and value transformations, which are usually indicated by keywords like “refer to”). These rules represent expert knowledge and must be applied unless they clearly conflict with the database schema or question.

2. SQL Generation: Before generating the final SQL query, please think through the steps of how to write the query.

# Database Engine:
SQLite

# Database Schema:
{db_desc}
This schema describes the database's structure, including tables, columns, primary keys, foreign keys, and any relevant relationships or constraints.

# Instructions:
- **Keep the '`' symbols of column name and table name if they are in the Database Schema.** This is very important as it will directly lead to SQL execution failure. 
- Make sure you only output the information that is asked in the question. If the question asks for a specific column, make sure to only include that column in the SELECT clause, nothing more.
- The generated query should return all of the information asked in the question without any missing or extra information.
- **Do NOT use aliases unless absolutely necessary.**

Output format:
Please provide a detailed chain-of-thought reasoning process and include your thought process within `<think>` tags. Your final answer should be enclosed within `<answer>` tags.

Ensure that your SQL query follows the correct syntax and is formatted as follows:

```sql
-- Your SQL query here
```

Example format:
<think> Step-by-step reasoning, following the instructions, including self-reflection and corrections if necessary. [Limited by 4K tokens] </think>

<answer> Summary of the thought process leading to the final SQL query. **It should be made clear that the data may not be perfect, but you MUST generate an SQL query for the user (perhaps suboptimal).** [Limited by 1K tokens]

```sql
Correct SQL query here
```
</answer>

Question:
{question}
"""
    return FILTER_DDL_AGNET_PROMPT

# 这是bird的，spider2的结构不同
# # Database Schema:
# {item["db_table_column_desc"]}
# This schema describes the database's structure, including tables, columns, primary keys, foreign keys, and any relevant relationships or constraints.


def get_generate_sql_agent_prompt(filtered_ddl, question, sql, examples):
    GENERATE_SQL_AGENT_PROMPT = f"""# Goal: Follow the STEP, refine the given draft SQL or rewrite a executable SQL so it fully satisfies the user’s requirement. Strictly check all constraints before outputting the final query.

# STEP:
STEP 1. From the Draft SQL, first explain literally what this SQL query is intended to do.

STEP 2. Determine whether it is suitable to answer the question. Answer only "YES" or "NO" after careful thinking.

STEP 3.1. If your answer is "NO", then think step by step and rewrite an executable SQL query so that it correctly answers the question. 

STEP 3.2. If your answer is "YES", execute the sql, carefully check and correct any potential errors that may arise due to unclear column cardinality relationships in Draft SQL.

STEP 4. The SQL query you generate will be passed to another model for style transfer. Please draft a concise and precise set of instructions for this style transfer model, strictly warning it not to modify, add, or remove any critical conditions that you identified during the action and observation process. Please use the following format:
(1) The condition [XXX] is critical; do not modify it, otherwise the query will return empty results.
(2) The table [XXX] is essential; do not change it, otherwise it will result in a logic error.
(3) Do not add the [XXX] filter condition. It's not mentioned in the question.
...

STEP 5. Before return the final answer, always check these points explicitly:
- 1. **Keep the '`' symbols of column name and table name if they are in the Database Schema.** This is very important as it will directly lead to SQL execution failure.

- 2. **Prohibit value checks (including the returned column), such as `column is NOT NULL` and `column > 0`, unless the user explicitly requests them in the Question. If such checks appear in the SQL, remove them.** 

- 3. Follow all the rules (functions) mentioned in the question (such as format conversions and value transformations, which are usually indicated by keywords like “refer to”). These rules represent expert knowledge and must be applied unless they clearly conflict with the database schema, question or execution results.

- 4. **Aggregation (MAX/MIN):** Always perform JOINs before using MAX() or MIN().

- 5. Use the `DISTINCT` keyword in the following three primary scenarios:

  (1) To Fulfill User Requirements for Uniqueness (Explicit Need): The query must return a list of entities that are explicitly required to be unique and non-repeating.
  
  (2) To Eliminate JOIN-Induced Duplicates (Structural Necessity): To prevent records from the primary table from being repeated (exploded) due to multiple matches in a multi-table JOIN (e.g., in one-to-many relationships).

  (3) For Accurate Aggregation of Unique Values: To ensure aggregate functions (like COUNT) perform calculations based only on the non-duplicate values within a column.


# Database Engine:
SQLite

# Database Schema:
{filtered_ddl}
This schema describes the database's structure, including tables, columns, primary keys, foreign keys, and any relevant relationships or constraints.
**Every field included here is critical, as it has been extracted and filtered as a partial DDL from the Draft SQL produced in the preceding stage.**


# Examples for SQL writing:
SQL supports multiple equivalent syntaxes. Generating the SQL formatting according to the style reflected in the following examples.
I think you should prioritize **clean, join-first, flat** SQL structures. Avoid unnecessary nested subqueries.
{examples}


# Notice:
- Since you have limited knowledge of the actual stored value, when you are refining or rewrite, you can generate some exploratory SQL queries (not the final SQL given to the user) to examine your answer.
- You can only use the actions provided in the **Action space** to solve the task.

# Action space: 
You can use the following tools:

## Action: 
- TOOL_NAME: execute_sql
- Function: Execute the given SQL query statement and return the execution result of the database.   
- ActionInput:
```sql
-- SQL query
```
- Action Example:
Action: execute_sql
ActionInput:
```sql
-- SQL query
```


Example format:
<answer> Summary of the thought process leading to the final SQL query. **It should be made clear that the data may not be perfect, but you MUST generate an SQL query for the user (perhaps suboptimal).** [Limited by 1K tokens]

```text
Instructions for the style transfer model
```

```sql
Correct Single SQL query here
```
</answer>

**VERY IMPORTANT: After writing ActionInput, STOP generating. Wait for the system to provide the Observation. DO NOT generate the Observation yourself.**

Get started!
Question: 
{question}

Draft SQL: 
```sql
{sql}
```

"""
    return GENERATE_SQL_AGENT_PROMPT



def get_style_sql_agent_prompt(question, sql, rules):
    REFINE_SQL_AGENT = f"""# Goal: Your task is to perform a preference check on the given SQL statement. You must strictly follow both the given rules and check rules bellow, and convert the given SQL into a compliant, executable SQL statement.

# Given Rules:
{rules}

The Given rules contain instructions from the previous model that generated the given SQL, which you must follow **unless it violates the following Check rules, such as `IS NOT NULL` check in the return column. According to Check rule 1 bellow, it should remove.** 

# Check Rules:

- 1. Value Check Rule: **Prohibit value checks (such as IS NOT NULL check in the return column) unless the user explicitly requests them in the Question or it satisfies the Rule 7 (Add NULL Check Rule) bellow. If such checks appear in the SQL, remove them.** 

- 2. Function Check Rule: **Replace `SELECT ... value = (SELECT MAX/MIN(column)...)` with `ORDER BY ... LIMIT 1`**

- 3. Date Check Rule: Year extraction and age calculation: STRFTIME ('%Y', time_now) - STRFTIME ('%Y', Birthday). ONLY year.

- 4. Percentage Check Rule: When generating SQL for percentage-related questions,  ensure the SELECT statement explicitly includes "* 100" in the **numerator** unless the user explicitly don't request them in the Question..

- 5. Division Check Rule: When it comes to division, always cast denominator to FLOAT or REAL.

- 6. Format Check Rule: NEVER use `||`, `GROUP_CONCAT` or `CONCAT` in SQL queries. Individual columns in the SELECT clause should be returned as separate fields without combining them.

- 7. Add NULL Check Rule: MUST Add IS NOT NULL for columns used in `ORDER BY ... ASC`. 

- 8. Entity Rule: Prohibit using `SELECT *`. When the user does not specify the fields to return, default to returning the entity identifier to represent entity.

- 9. **Logic Preservation Rule (CRITICAL):**Except for SQL segments explicitly modified by the above rules, STRICTLY PROHIBIT changing logical operators or values, even though they have errors.

# Output format:
<think> Step-by-step reasoning, following the Check Rules, check and correct the given SQL. [Limited by 4K tokens] </think>
<answer> Summary of the thought process leading to the final SQL query. **It should be made clear that the data may not be perfect, but you MUST generate an SQL query for the user (perhaps suboptimal).** [Limited by 1K tokens]

```sql
Correct SQL query here
```
</answer>

Example Output:
<think>  
- User question: "What is the maximum age of the students?"  
- Given SQL: `SELECT MAX(age) FROM student WHERE age > 0 and age IS NOT NULL;` 

Now, I need follow the STEP to check and correct the given SQL.  

Step 1: Value Check Rule
- remove `age > 0` and `age IS NOT NULL`, cause it doesn't explicitly required in the User question.

...

Step 6: Format Check  
- No use of `||`. Safe.  

Step 7: Add NULL Check  
- No ORDER BY ASC is in the SQL. Safe

...

Therefore, the corrected SQL should be:  
`SELECT MAX(age) FROM student;`  
</think>

<answer>  
```sql
SELECT MAX(age) FROM student;
```
</answer>

# Database Engine: 
SQLite

# Given SQL:
{sql}

# Question:
{question}

"""
    return REFINE_SQL_AGENT

# - 0. Based on the conversation, explicitly summarize the constraints that the given SQL are required to keep. These rules must be treated as immutable global constraints and strictly enforced in all subsequent steps.


def get_output_sql_agent_prompt(question, sql):  
    OUTPUT_SQL_AGENT = f"""# Goal: Follow the STEP, your task is to perform a column check on the given SQL statement.

# STEP:

- 1. Extract the explicit content that the user needs to return as the **minimum** requirement in the question. Not having an identifier is completely acceptable.

- 2. Modify the SELECT clause in the SQL statement to return only the requested content.

# Important Note:
- **You can only delete return columns, add new return columns, adjust the order of return columns. Other operations are strictly prohibited, even if the logic in the SQL might be incorrect.**

# Column rules:
- 1. Column Selection Check Rule:
    - Example 1: 
        ++ User question: "What is the maximum age of the students?"  
        ++ ✅ Correct SQL: SELECT MAX(age) FROM student;  
        ++ ❌ Incorrect SQL: SELECT id, MAX(age) FROM student; 
        ++ Explanation: Incorrect SQL includes the id column, which was not asked for in the question, resulting in extra information.
    - Example 2:
        ++ User question: "Which top 4 student had the most games?"
        ++ ✅ Correct SQL: SELECT id FROM League ...;
        ++ ❌ Incorrect SQL: SELECT id, COUNT(game.id) FROM student ...;
        ++ Explanation: Incorrect SQL returns both id and the count of games, whereas the user only asked for the student identifiers.
    - Example 3:
        ++ User question: "List all students older than 18."  
        ++ ✅ Correct SQL: SELECT id FROM student WHERE age > 18;  
        ++ ❌ Incorrect SQL: SELECT * FROM student WHERE age > 18;
        ++ Explanation: Incorrect SQL returns all columns of the student table, just need the identifiers requested in the question.
    - Example 4:
        ++ User question: "What is the rule of playing card \"Benalish Knight\"?"  
        ++ ✅ Correct SQL: SELECT T2.format FROM cards AS T1 INNER JOIN legalities AS T2 ON T1.uuid = T2.uuid WHERE T1.name = 'Benalish Knight';  
        ++ ❌ Incorrect SQL: "SELECT T2.format, T2.status FROM cards AS T1 INNER JOIN legalities AS T2 ON T1.uuid = T2.uuid WHERE T1.name = 'Benalish Knight'";
    - Example 5:
        ++ User question: "Which user created post ID 1 and what is the reputation of this user?"  
        ++ ✅ Correct SQL: SELECT T2.Id, T2.Reputation FROM comments AS T1 INNER JOIN users AS T2 ON T1.UserId = T2.Id WHERE T1.PostId = 1;  
        ++ ❌ Incorrect SQL: SELECT T2.DisplayName, T2.Reputation FROM comments AS T1 INNER JOIN users AS T2 ON T1.UserId = T2.Id WHERE T1.PostId = 1;
    - Example 6:
        ++ User question: "What time did the the 2010's Formula_1 race took place on the Abu Dhabi Circuit?"  
        ++ ✅ Correct SQL: SELECT T2.date, T2.time FROM circuits AS T1 INNER JOIN races AS T2 ON T2.circuitID = T1.circuitId WHERE T2.year = 2010 AND T2.name = 'Abu Dhabi Grand Prix';  
        ++ ❌ Incorrect SQL: SELECT T2.time FROM circuits AS T1 INNER JOIN races AS T2 ON T2.circuitID = T1.circuitId WHERE T2.year = 2010 AND T2.name = 'Abu Dhabi Grand Prix';
    - Example 7:
        ++ User question: "What is the eligible free or reduced price meal rate for the top 5 schools in grades 1-12 with the highest free or reduced price meal count of the schools with the ownership code 66?"  
        ++ ✅ Correct SQL: SELECT CAST(T1.`FRPM Count (K-12)` AS REAL) / T1.`Enrollment (K-12)` FROM frpm AS T1 INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode WHERE T2.SOC = 66 ORDER BY T1.`FRPM Count (K-12)` DESC LIMIT 5;  
        ++ ❌ Incorrect SQL: SELECT CAST(T1.`FRPM Count (K-12)` AS REAL) / T1.`Enrollment (K-12)`, frpm.`School Name` FROM frpm AS T1 INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode WHERE T2.SOC = 66 ORDER BY T1.`FRPM Count (K-12)` DESC LIMIT 5;
    
- 2. Column Order Check Rule:
    - Example 1:  
        ++ User question: "What are the student id and name?"  
        ++ ✅ Correct SQL: SELECT id, name FROM student;  
        ++ ❌ Incorrect SQL: SELECT name, id FROM student;
        ++ Explanation: Incorrect SQL reverses the order of the columns (name first, id second), which does not match the order in the user question.
    - Example 2:
        ++ User question: "How old is the youngest Japanese driver? What is his name?"
        ++ ✅ Correct SQL: SELECT age, name FROM drivers ...;
        ++ ❌ Incorrect SQL: SELECT name, age FROM drivers ...;
        ++ Explanation: Incorrect SQL places name before age, which does not match the order specified in the user question.
    - Example 3:
        ++ User question: "In which city can you find the school in the state of California with the lowest latitude coordinates and what is its lowest grade? Indicate the school name."
        ++ ✅ Correct SQL: SELECT T2.City, T1.`Low Grade`, T1.`School Name` FROM frpm AS T1 INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode WHERE T2.State = 'CA' ORDER BY T2.Latitude ASC LIMIT 1;
        ++ ❌ Incorrect SQL: SELECT T2.City, T1.`School Name`, T1.`Low Grade` FROM frpm AS T1 INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode WHERE T2.State = 'CA' ORDER BY T2.Latitude ASC LIMIT 1;    

- 3. RANK Check Rule: For questions that include the phrase "Rank ...", add `RANK(xxx)` at the end of all return columns.

- 4. Format Check Rule: NEVER use `||`, `GROUP_CONCAT` or `CONCAT` in SQL queries. Individual columns in the SELECT clause should be returned as separate fields without combining them.

# Output Format:
<think>
Step-by-step reasoning on how the given SQL is analyzed and rewritten according to the above rules.  
</think>

<answer>
Summarize the reasoning and show the final, rewritten SQL query that follows the preferred SQL style.
Remember you can only delete return columns, add new return columns, adjust the order of return columns. Other operations are strictly prohibited, even if the logic in the SQL might be incorrect.

```sql
-- Corrected SQL query here
```
</answer>

# Database Engine: 
SQLite

# User question:
{question}

# Given SQL:
{sql}
"""

    return OUTPUT_SQL_AGENT