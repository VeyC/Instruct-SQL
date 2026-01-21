# 使用llm直接生成output
import argparse
import math
from datetime import datetime
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import threading

import torch
from run_manager import RunManager
from arctic_manager import ArcticManager


def process_batch(batch_data, batch_index, opt, progress_counter, total_batches):
    """处理单个批次数据的函数"""
    thread_name = threading.current_thread().name
    print(f"[{thread_name}] 开始处理批次 {batch_index + 1}/{total_batches}，包含 {len(batch_data)} 条记录")

    # 为每个批次创建独立的 RunManager 实例，但模型已经在全局预加载
    run_manager = RunManager(opt, batch_index)
    run_manager.initialize_tasks(batch_data)
    run_manager.run_tasks()
    # 生成最终的 SQL 文件
    result_directory = run_manager.generate_sql_files()

    with progress_counter['lock']:
        progress_counter['completed'] += 1
        completed = progress_counter['completed']
        print(f"[{thread_name}] 批次 {batch_index + 1} 处理完成 ({completed}/{total_batches})")

    return batch_index, len(batch_data), result_directory

def main(opt):
    with open(opt.input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
        data = sorted(data, key=lambda x: x['question_id'])

    print(f"读取到 {len(data)} 条记录")

    # 计算批次
    num_batches = 5
    batch_size = math.ceil(len(data) / num_batches)
    print(f"将数据分成 {num_batches} 个批次，每批最多 {batch_size} 条记录")

    # 分割数据为批次
    batches = []
    for i in range(num_batches):
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, len(data))
        batch_data = data[start_idx:end_idx]
        batches.append((batch_data, i))

    # 使用5个线程并行处理批次
    max_workers = 5  # 用户要求5个线程并行

    # 创建进度计数器（线程安全）
    progress_counter = {'completed': 0, 'lock': threading.Lock()}

    # 预加载共享的 Manager 实例，避免在每个 worker 中重复初始化
    print("预加载模型和管理器...")
    arctic_manager = ArcticManager(
        opt.pretrained_model_name_or_path,
        opt.tensor_parallel_size,
        opt.temperature,
        opt.n
    )

    print("开始并行处理批次...")
    result_directorys = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 立即提交所有任务，避免闭包问题
        future_to_batch = []
        for batch_data, batch_idx in batches:
            print(f"提交批次 {batch_idx}，包含 {len(batch_data)} 条数据")
            print(f"  第一条数据 question_id: {batch_data[0]['question_id']}")
            
            future = executor.submit(
                process_batch, 
                batch_data,      # 立即绑定
                batch_idx,       # 立即绑定
                opt, 
                progress_counter, 
                num_batches
            )
            future_to_batch.append(future)


        # 等待所有批次完成
        for future in as_completed(future_to_batch):
            try:
                batch_idx, batch_size_processed, result_directory = future.result()
                result_directorys.append(result_directory)
            except Exception as exc:
                print(f'批次 {batch_idx + 1} 处理时发生异常: {exc}')
                raise

    print("所有批次处理完成，开始生成最终的SQL文件...")
    value_dict = {}
    for result_directory in result_directorys:
         with open(os.path.join(result_directory, "-sql_selection.json"), 'r') as f:
            pred = json.load(f)
            value_dict.update(pred)
    
    with open(opt.output_file, 'w', encoding='utf-8') as f:
        json.dump(value_dict, f, indent=2, ensure_ascii=False)

    print("处理完成！")
    print(f'文件成功保存至{opt.output_file}')




if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, choices=['dev', 'test'], default="dev")
    parser.add_argument("--input_file", type=str, help="samples的文件位置")
    parser.add_argument("--output_file", type=str, help="用来生成处理后的samples的位置")
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
    parser.add_argument("--tensor_parallel_size", type=int, default=1, help="gpu的个数")
    # 获取可用的GPU数量
    tensor_parallel_size = torch.cuda.device_count()
    print(f"Available GPUs: {tensor_parallel_size}")

    opt = parser.parse_args()
    opt.tensor_parallel_size = tensor_parallel_size
    opt.run_start_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    main(opt)