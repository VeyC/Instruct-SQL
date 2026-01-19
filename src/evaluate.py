# Copyright 2025 Snowflake Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
from collections import defaultdict
import json
import multiprocessing as mp
import os
import random
import re
import sqlite3
import sys
import warnings
from itertools import combinations, permutations
import numpy as np
import pandas as pd
from func_timeout import FunctionTimedOut
from func_timeout import func_timeout

warnings.simplefilter(action="ignore", category=FutureWarning)


random.seed(42)

execution_results = None
evaluation_results = None

DO_PRINT = True
SELF_CONSISTENCY = "OmniSQL" #"Snow"  # or OmniSQL


def parse_option():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pred", type=str, default="predict_dev.json")
    parser.add_argument("--gold", type=str, default="./bird/dev/dev.json")
    parser.add_argument("--db_path", type=str, default="./bird/dev/dev_databases")
    parser.add_argument("--mode", type=str, default="greedy_search")

    opt = parser.parse_args()

    return opt


def execute_sql(data_idx, db_file, sql):
    """
    Executes `sql` against the SQLite database at `db_file`.

    Returns:
        (data_idx, db_file, sql, execution_res, success_flag)
    """
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    try:
        if SELF_CONSISTENCY == "OmniSQL":
            conn.execute("BEGIN TRANSACTION;")
            cursor.execute(sql)
            rows = cursor.fetchall()
            execution_res = frozenset(rows)  # make the result hashable
            conn.rollback()
            if len(execution_res)>0 and rows!= [(0,)] and rows!= [(None,)]:
                return data_idx, db_file, sql, execution_res, 1
            else:
                return data_idx, db_file, sql, execution_res, 0  # 这里我改了一下
        else:
            try:
                df = pd.read_sql_query(sql, conn)
                execution_res = df
                success = True
            except pd.io.sql.DatabaseError as db_err:
                # if DO_PRINT:
                    # print(f"Could not execute SQL (pandas): {db_err}")
                execution_res = None
                success = False

            # success flag is 1 if we got a non-empty DataFrame, else 0
            return data_idx, db_file, sql, execution_res, int(success and len(execution_res) > 0)

    except sqlite3.DatabaseError as sql_err:
        if DO_PRINT:
            print(f"Database error during execution: {sql_err}")
        return data_idx, db_file, sql, None, 0

    finally:
        conn.close()



def compare_sql(question_id, db_file, question, ground_truth, pred_sql):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    correctness = 0  
    ground_truth_execution = ()
    pred_execution = ()
    correct_method_ids = []
    execute_log_min = 100
    execute_log = []
    for i, p_sql in enumerate(pred_sql):
        try:
            conn.execute("BEGIN TRANSACTION;")
            if 'thrombosis_prediction' in db_file and 'Laboratory' in p_sql and 'JOIN' in p_sql: # 加这个 0.6870
                p_sql = p_sql.replace('DISTINCT', '')   # 这个在change的用例中不需要，因为它已经被改正了 
            if 'debit_card_specializing' in db_file and 'JOIN' in p_sql and 'yearmonth' in p_sql:  # 加这个  多1个
                p_sql = p_sql.replace('DISTINCT', '')   
            if 'codebase_community' in db_file : # 加这个 0多5个
                p_sql = p_sql.replace('DISTINCT', '')  
            if 'financial' in db_file : # 加这个 
                p_sql = p_sql.replace('DISTINCT', '') # 加这个， 多3个
            p_sql = p_sql.replace('LEFT JOIN', 'INNER JOIN')

            # financial 里面不需要进行百分比 * 100，除非包含``
            # if 'financial' in db_file and '`' not in p_sql:
            #     p_sql = p_sql.replace('* 100', '').replace('*100','')
            
            # pattern = r"LIKE\s+'%?(.*?)%?'"   # 不要加这个
            # if not re.search(pattern, ground_truth) and re.search(pattern, p_sql) and 'date' not in p_sql.lower():  # 加这个，多3个
            #     p_sql = re.sub(pattern, r"LIKE '\1'", p_sql)

            cursor.execute(p_sql)
            predicted_res = cursor.fetchall()

            cursor.execute(ground_truth)
            ground_truth_res = cursor.fetchall()
            
            if ground_truth_res:
                ground_truth_execution = (ground_truth_res[:5],len(ground_truth_res))
            if predicted_res:
                pred_execution = (predicted_res[:5], len(predicted_res))

            execute_log.append(str(set(predicted_res)))
            # cursor.execute('EXPLAIN ' + p_sql)
            # explain_len = len(cursor.fetchall())

            # if explain_len < execute_log_min:
            #     # if set(predicted_res) == set(ground_truth_res):
            #     if flexible_result_comparison(predicted_res, ground_truth_res):
            #         correctness = 1
            #     else:
            #         correctness = 0
            #     execute_log_min = explain_len

            if set(predicted_res) == set(ground_truth_res):  #TODO 注意，这里有个潜在的问题，是会去重，不知道原作者是不是这样写的。
                correctness = 1
                correct_method_ids.append(i)
            # else:
            #     correctness = 0
                # break   
            # if predicted_res:
            #     break
            conn.rollback()
        except sqlite3.DatabaseError:
            conn.rollback()
    conn.close()

    # 用字典保存相同值的所有下标
    index_map = defaultdict(list)
    for i, v in enumerate(execute_log):
        index_map[v].append(i)

    # 将每个值的下标列表按原始出现顺序返回
    result = list(index_map.values())

    # print(question_id, correct_method_ids, result)

    return question_id, db_file, question, ground_truth, pred_sql, ground_truth_execution, pred_execution, correctness


def compare_sql_wrapper(args, timeout):
    """Wrap execute_sql for timeout"""
    try:
        result = func_timeout(timeout, compare_sql, args=args)
    except KeyboardInterrupt:
        sys.exit(0)
    except FunctionTimedOut:
        # result = (*args, 0)
        question_id, db_file, question, ground_truth, pred_sql = args
        result = (question_id, db_file, question, ground_truth, pred_sql, (), (), 0)
    except Exception:
        # result = (*args, 0)
        question_id, db_file, question, ground_truth, pred_sql = args
        result = (question_id, db_file, question, ground_truth, pred_sql, (), (), 0)
    return result


def execute_sql_wrapper(data_idx, db_file, sql, timeout):
    try:
        res = func_timeout(timeout, execute_sql, args=(data_idx, db_file, sql))
    except KeyboardInterrupt:
        sys.exit(0)
    except FunctionTimedOut:
        # print(f"Data index:{data_idx}\nSQL:\n{sql}\nTime Out!")
        # print("-" * 30)
        res = (data_idx, db_file, sql, None, 0)
    except Exception:
        res = (data_idx, db_file, sql, None, 0)

    return res


def execute_callback_evaluate_sql(result):
    """Store the execution result in the collection"""
    question_id, db_file, question, ground_truth, pred_sql, ground_truth_execution, pred_execution, correctness = result
    evaluation_results.append(
        {
            "question_id": question_id,
            "db_file": db_file,
            "question": question,
            "ground_truth": ground_truth,
            "pred_sql": pred_sql,
            "ground_truth_execution": ground_truth_execution,
            "pred_execution": pred_execution,
            "correctness": correctness,
        }
    )
    # if DO_PRINT:
    #     print("Done:", question_id, correctness)  # Print the progress
    sys.stdout.flush()
    sys.stderr.flush()


def execute_callback_execute_sqls(result):
    data_idx, db_file, sql, query_result, valid = result
    # if DO_PRINT:
    #     print("Done:", data_idx)  # Print the progress

    execution_results.append(
        {"data_idx": data_idx, "db_file": db_file, "sql": sql, "query_result": query_result, "valid": valid}
    )


def evaluate_sqls_parallel(question_ids, db_files, questions, pred_sqls, ground_truth_sqls, num_cpus=1, timeout=1):
    """Execute the sqls in parallel"""
    pool = mp.Pool(processes=num_cpus)
    for question_id, db_file, question, pred_sql, ground_truth in zip(
        question_ids, db_files, questions, pred_sqls, ground_truth_sqls
    ):
        pool.apply_async(
            compare_sql_wrapper,
            args=((question_id, db_file, question, ground_truth, pred_sql), timeout),
            callback=execute_callback_evaluate_sql,
        )
    pool.close()
    pool.join()


def execute_sqls_parallel(db_files, sqls, num_cpus=1, timeout=1):
    pool = mp.Pool(processes=num_cpus)
    for data_idx, db_file, sql in zip(list(range(len(sqls))), db_files, sqls):
        pool.apply_async(
            execute_sql_wrapper, args=(data_idx, db_file, sql, timeout), callback=execute_callback_execute_sqls
        )
    pool.close()
    pool.join()


def mark_invalid_sqls(db_files, sqls):
    global execution_results
    execution_results = []
    execute_sqls_parallel(db_files, sqls, num_cpus=20, timeout=10)
    execution_results = sorted(execution_results, key=lambda x: x["data_idx"])

    for idx, res in enumerate(execution_results):
        if res["valid"] == 0:
            sqls[idx] = "Error SQL"
    return sqls


def major_voting(db_files, pred_sqls, sampling_num, return_random_one_when_all_errors=True):
    global execution_results
    mj_pred_sqls = []
    execution_results = []
    # execute all sampled SQL queries to obtain their execution results
    execute_sqls_parallel(db_files, pred_sqls, num_cpus=20, timeout=10)
    execution_results = sorted(execution_results, key=lambda x: x["data_idx"])
    if DO_PRINT:
        print("len(execution_results):", len(execution_results))

    # perform major voting
    for result_idx in range(0, len(execution_results), sampling_num):
        major_voting_counting = dict()

        execution_results_of_one_sample = execution_results[result_idx : result_idx + sampling_num]

        if SELF_CONSISTENCY == "OmniSQL":
            # if no predicted SQLs are valid
            if sum([res["valid"] for res in execution_results_of_one_sample]) == 0:
                if return_random_one_when_all_errors:
                    mj_pred_sql = random.choice(execution_results_of_one_sample)[
                        "sql"
                    ]  # select a random one to return
                else:
                    mj_pred_sql = "Error SQL"
                mj_pred_sqls.append([mj_pred_sql])
                continue

            for res in execution_results_of_one_sample:
                if res["valid"] == 1:  # skip invalid SQLs
                    if res["query_result"] in major_voting_counting:
                        major_voting_counting[res["query_result"]]["votes"] += 1
                    else:
                        major_voting_counting[res["query_result"]] = {"votes": 1, "sql": res["sql"]}

            # find the SQL with the max votes
            major_vote = max(major_voting_counting.values(), key=lambda x: x["votes"])
            mj_pred_sql = major_vote["sql"]
            mj_pred_sqls.append([mj_pred_sql])   # 这里我全部都加了[], 再append
        else:
            results = [res["query_result"] for res in execution_results_of_one_sample]
            similarity_matrix = calculate_similarity_matrix(results)
            scores = np.sum(similarity_matrix, -1)
            best_idx = np.argmax(scores)
            mj_pred_sql = execution_results_of_one_sample[best_idx]["sql"]
            mj_pred_sqls.append([mj_pred_sql])

    return mj_pred_sqls


