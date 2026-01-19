import json
from database_util import format_table_column_name

def normalize_column_keys_by_format(desc_json_path):
    with open(desc_json_path, 'r', encoding='utf-8') as f:
        desc_datas = json.load(f)

    changed = False
    for db_name, tables in desc_datas.items():
        for table_name, table_info in tables.items():
            if 'column_desc' not in table_info:
                continue
            col_items = list(table_info['column_desc'].items())
            new_col_desc = {}

            for col_name, value in col_items:
                # 统一去除反引号/引号再格式化
                norm_name = format_table_column_name(col_name.replace('`','').replace('"',''))
                # 如果冲突覆盖，只保留标准化后的key
                new_col_desc[norm_name] = value
            # 如果key发生变化才算changed
            if set(table_info['column_desc'].keys()) != set(new_col_desc.keys()):
                changed = True
            table_info['column_desc'] = new_col_desc  # 全量覆盖

    if changed:
        with open(desc_json_path, 'w', encoding='utf-8') as f:
            json.dump(desc_datas, f, ensure_ascii=False, indent=2)
        print('所有字段已归一化（只保留format_table_column_name输出的字段名key）并覆盖保存。')
    else:
        print('没有需要归一化的字段，数据未变更。')

if __name__ == '__main__':
    normalize_column_keys_by_format('../output/bird/dev/table_desc.json')