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
from typing import Any, Hashable
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


def efficient_soft_df_similarity(df1: pd.DataFrame, df2: pd.DataFrame) -> float:
    """
    Computes the "soft denotation" similarity between two DataFrames:
      - For each column, get the frequency counts of each distinct value in df1 and df2
      - Align them by the union of distinct values
      - Accumulate 'real agreement' vs. 'possible agreement'
      - Return total_real_agreement / total_possible_agreement

    Args:
        df1, df2 (pd.DataFrame): DataFrames to compare

    Returns:
        float: Similarity score in [0, 1].
    """
    # If either DataFrame is empty in rows or columns, similarity = 0
    if df1 is None or df2 is None or len(df1) == 0 or len(df2) == 0:
        return 0.0

    df1 = df1.copy()  # Precaution
    df2 = df2.copy()

    def _ensure_hashable_values(value: Any) -> Hashable:
        if isinstance(value, Hashable):
            return value
        return repr(value)

    # Use applymap for DataFrames
    df1 = df1.applymap(_ensure_hashable_values)
    df2 = df2.applymap(_ensure_hashable_values)

    # For rare cases where df has two columns with the same name (sic!)
    def _select_1d(df, col):
        c = df[col]
        if len(c.shape) == 2:
            return pd.DataFrame(c.stack().values)
        return c

    # Precompute value_counts for each column
    df1_counts = {col: _select_1d(df1, col).value_counts(dropna=False) for col in df1.columns}
    df2_counts = {col: _select_1d(df2, col).value_counts(dropna=False) for col in df2.columns}

    total_real_agreement = 0.0
    total_possible_agreement = 0.0

    # Union of all columns
    all_columns = df1.columns.union(df2.columns)

    for col in all_columns:
        vc1 = df1_counts.get(col)
        vc2 = df2_counts.get(col)

        if vc1 is None or vc1.empty:
            total_possible_agreement += vc2.to_numpy().sum()
            continue

        if vc2 is None or vc2.empty:
            total_possible_agreement += vc1.to_numpy().sum()
            continue

        # 1) Get union of distinct values in that column
        union_idx = pd.Index(pd.concat([vc1.index.to_frame(), vc2.index.to_frame()], axis=0).iloc[:, 0].unique())
        if union_idx.dtype != "object":
            union_idx = union_idx.astype(object)

        # 2) Reindex both frequency series to that union, fill missing with 0
        freq1 = vc1.reindex(union_idx, fill_value=0).values
        freq2 = vc2.reindex(union_idx, fill_value=0).values

        if np.nan in union_idx:
            freq1[union_idx.isnull()] += freq2[union_idx.isnull()]
            freq2[union_idx.isnull()] = 0

        # 3) Vectorized computations (avoiding DataFrame overhead)
        possible_agreement = np.maximum(freq1, freq2).sum()
        accumulated_difference = np.abs(freq1 - freq2).sum()
        real_agreement = possible_agreement - accumulated_difference

        # Accumulate column-wise
        total_real_agreement += real_agreement
        total_possible_agreement += possible_agreement

    # Avoid division by zero if possible_agreement == 0
    if total_possible_agreement == 0:
        return 0.0

    return total_real_agreement / total_possible_agreement


def calculate_similarity_matrix(
    candidate_sqls,
) -> np.ndarray:
    sql_len = len(candidate_sqls)
    similarity_matrix = np.zeros((sql_len, sql_len))
    for idx1 in range(sql_len):
        df1 = candidate_sqls[idx1]
        if df1 is not None:
            similarity_matrix[idx1, idx1] += 1
        for idx2 in range(idx1 + 1, sql_len):
            df2 = candidate_sqls[idx2]
            similarity = efficient_soft_df_similarity(df1, df2)
            similarity_matrix[idx1, idx2] += similarity
            similarity_matrix[idx2, idx1] += similarity
    return similarity_matrix


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
            rows = list(set(rows))    # 这里我改了一下
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
    execute_sqls_parallel(db_files, pred_sqls, num_cpus=20, timeout=5)
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


