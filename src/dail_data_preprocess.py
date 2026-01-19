import argparse
import json
import os
import shutil
from tqdm import tqdm

from dail_utils.linking_process import SpiderEncoderV2Preproc
from dail_utils.pretrained_embeddings import GloVe
from dail_utils.datasets.spider import load_tables

def schema_linking_producer(infer_file, train_file, table, db, dataset_dir, infer_section='dev', compute_cv_link=True):
    # load data
    infer_data = json.load(open(os.path.join(dataset_dir, infer_file)))
    train_data = json.load(open(os.path.join(dataset_dir, train_file)))

    # load schemas
    schemas, _ = load_tables([os.path.join(dataset_dir, table)])

    # Backup in-memory copies of all the DBs and create the live connections
    import sqlite3
    from pathlib import Path
    for db_id, schema in tqdm(schemas.items(), desc="DB connections"):
        sqlite_path = Path(dataset_dir) / db / db_id / f"{db_id}.sqlite"
        source: sqlite3.Connection
        with sqlite3.connect(str(sqlite_path)) as source:
            dest = sqlite3.connect(':memory:')
            dest.row_factory = sqlite3.Row
            source.backup(dest)
        schema.connection = dest
    print("开始加载")
    word_emb = GloVe(kind='42B', lemmatize=True)
    print("GloVe embedding loaded!")
    linking_processor = SpiderEncoderV2Preproc(dataset_dir,
            min_freq=4,
            max_count=5000,
            include_table_name_in_column=False,
            word_emb=word_emb,
            fix_issue_16_primary_keys=True,
            compute_sc_link=True,
            compute_cv_link=compute_cv_link)

    # build schema-linking
    # process infer (dev/test) and train for schema linking
    for data, section in zip([infer_data, train_data], [infer_section, 'train']):
        for item in tqdm(data, desc=f"{section} section linking"):
            db_id = item["db_id"]
            schema = schemas[db_id]
            to_add, validation_info = linking_processor.validate_item(item, schema, section)
            if to_add:
                linking_processor.add_item(item, schema, section, validation_info)

    # save
    linking_processor.save()

def bird_pre_process(bird_dir, mode="dev", with_evidence=False):
    new_db_path = os.path.join(bird_dir, "database")
     # === 清空 database 文件夹 ===
    if os.path.exists(new_db_path):
        shutil.rmtree(new_db_path)
    os.makedirs(new_db_path)
    # 合并 train 和 {mode} 下的所有 db 到 database
    print(f"---------------Copying files to database-----------------\n")
    db_source_dirs = [
        os.path.join(bird_dir, "train/train_databases"),
        os.path.join(bird_dir, f"{mode}/{mode}_databases"),
    ]
    for src_dir in db_source_dirs:
        if os.path.exists(src_dir):
            # 合并所有db_id目录
            for db_id in os.listdir(src_dir):
                src_db_path = os.path.join(src_dir, db_id)
                dest_db_path = os.path.join(new_db_path, db_id)
                if os.path.isdir(src_db_path):
                    os.system(f"cp -r '{src_db_path}' '{dest_db_path}'")

    def json_preprocess(data_jsons):
        new_datas = []
        for data_json in data_jsons:
            ### Append the evidence to the question
            if with_evidence and len(data_json.get("evidence", "")) > 0:
                data_json['question'] = (data_json['question'] + " " + data_json["evidence"]).strip()
            question = data_json['question']
            tokens = []
            for token in question.split(' '):
                if len(token) == 0:
                    continue
                if token[-1] in ['?', '.', ':', ';', ','] and len(token) > 1:
                    tokens.extend([token[:-1], token[-1:]])
                else:
                    tokens.append(token)
            data_json['question_toks'] = tokens
            # test集未必有SQL字段，保险加判断
            if "SQL" in data_json:
                data_json['query'] = data_json['SQL']
            new_datas.append(data_json)
        return new_datas
    # 处理 train.json
    input_train_json = os.path.join(bird_dir, 'train/train.json')
    output_train_json = os.path.join(bird_dir, 'train.json')
    if os.path.exists(input_train_json):
        with open(input_train_json) as f:
            data_jsons = json.load(f)
            with open(output_train_json, 'w') as wf:
                json.dump(json_preprocess(data_jsons), wf, indent=4)

    # 处理 dev/test，输出 dev.json/test.json 到 bird_dir
    input_json = os.path.join(bird_dir, f'{mode}/{mode}.json')
    output_json = os.path.join(bird_dir, f'{mode}.json')
    with open(input_json) as f:
        data_jsons = json.load(f)
        with open(output_json, 'w') as wf:
            json.dump(json_preprocess(data_jsons), wf, indent=4)
    # 拷贝SQL（test集通常没有金标SQL，可以加个判断）
    sql_file = os.path.join(bird_dir, f'{mode}/{mode}.sql')
    if os.path.exists(sql_file):
        os.system(f"cp {sql_file} {bird_dir}")
    # 合并table（总是dev_tables和train_tables，test部分一般也共享）  
    tables = []
    if mode == "dev":
        for part in ['dev', 'train']:
            tables_json = os.path.join(bird_dir, f'{part}/{part}_tables.json')
            if os.path.exists(tables_json):
                with open(tables_json) as f:
                    tables.extend(json.load(f))
    elif mode == "test":
        for part in ['test', 'train']:
            tables_json = os.path.join(bird_dir, f'{part}/{part}_tables.json')
            if os.path.exists(tables_json):
                with open(tables_json) as f:
                    tables.extend(json.load(f))
    with open(os.path.join(bird_dir, 'tables.json'), 'w') as f:
        json.dump(tables, f, indent=4)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_dir", type=str, required=True, help="Root directory for the bird dataset")
    parser.add_argument("--mode", type=str, choices=["dev", "test"], default="dev",
                        help="Which split to preprocess (dev or test)")
    args = parser.parse_args()

    bird_dir = args.data_dir
    mode = args.mode

    bird_pre_process(bird_dir, mode=mode, with_evidence=True)
    infer_file = f"{mode}.json"
    train_file = "train.json"
    bird_table = "tables.json"
    bird_db = "database"
    ccvl = False
    schema_linking_producer(infer_file, train_file, bird_table, bird_db, bird_dir, infer_section=mode, compute_cv_link=ccvl)