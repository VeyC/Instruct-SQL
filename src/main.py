# 使用llm直接生成output
import argparse
from datetime import datetime
import json

import torch
from run_manager import RunManager


def main(opt):
    with open(opt.input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
        data = sorted(data, key=lambda x: x['question_id'])
     
    print(f"读取到 {len(data)} 条记录")

    run_manager = RunManager(opt)
    run_manager.initialize_tasks(data)
    run_manager.run_tasks()
    run_manager.generate_sql_files()




if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, choices=['dev', 'test'], default="dev")
    parser.add_argument("--input_file", type=str, help="samples的文件位置")
    # parser.add_argument("--output_file", type=str, help="用来生成处理后的samples的位置")
    parser.add_argument("--db_root_path", type=str, help="存放数据库的位置")
    parser.add_argument("--pipeline_setup", type=str, help="workflow设置")
    parser.add_argument("--pipeline_nodes", type=str, help="workflow的node")
    parser.add_argument("--seed", type=int, default=42, help="随机种子一定程度上控制生成的效果")
    parser.add_argument("--type", type=str, choices=['greedy','major'], help="选择使用greedy或者是major")
    parser.add_argument("--model_name", type=str, help="默认的模型名称")
    parser.add_argument('--log_level', type=str, default='warning', help="Logging level.")
    parser.add_argument('--pretrained_model_name_or_path', type=str, default='warning', help="Arctic model path.")
    parser.add_argument("--temperature", type=float, default=0.0, help="温度越高越随机")
    parser.add_argument("--n", type=int, default=1, help="arctic的生成个数")

    # 获取可用的GPU数量
    tensor_parallel_size = torch.cuda.device_count()
    print(f"Available GPUs: {tensor_parallel_size}")
    
    parser.add_argument("--tensor_parallel_size", type=int, default=1, help="gpu的个数")


    opt = parser.parse_args()
    opt.run_start_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    main(opt)