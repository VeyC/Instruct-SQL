import json
import argparse
from dataset_process_for_submit import get_db_fd_list_and_column_info, get_similar_column_by_score
# 替换 your_module 为你定义这些函数所在的实际模块名。如果就在同一文件，可省略。

parser = argparse.ArgumentParser()
parser.add_argument("--mode", type=str, choices=['dev', 'test'], default="dev")
parser.add_argument("--input_file", type=str, default='/media/hnu/hnu2024/wangqin/python_work/Text2SQL/output/bird/dev.json')
parser.add_argument("--intermediate_file", type=str)
parser.add_argument("--output_file", type=str, default='../output/bird/dev/dev_bird_metadata.json')
parser.add_argument("--meaning_file", type=str, default="/media/hnu/hnu2024/wangqin/python_work/Text2SQL/output/bird/TA-SQL/column_meaning.json")
parser.add_argument("--table_desc_file", type=str, default='../output/bird/dev/table_desc.json')
parser.add_argument("--db_path", type=str, default='/media/hnu/hnu2024/wangqin/python_work/Text2SQL/datasets/bird/dev/dev_databases')
parser.add_argument("--model_name", type=str, default='gpt-4o')
opt = parser.parse_args()

# 用参数传递的table_desc_file
with open(opt.table_desc_file, 'r', encoding='utf-8') as f:
    table_desc_dict = json.load(f)

get_db_fd_list_and_column_info(table_desc_dict, opt)
get_similar_column_by_score()