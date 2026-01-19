#!/bin/bash
set -e

MODE=$1     # "dail-sql" 或 "llama_factory"
SPLIT=$2    # "dev" 或 "test"

if [ "$MODE" = "dail-sql" ]; then
    # 激活环境
    source /opt/conda/bin/activate dail-sql

    # 启动 CoreNLP 服务（后台，不堵前台）
    nohup java -mx4g -cp "/workspace/model/third_party/stanford-corenlp-full-2018-10-05/*" edu.stanford.nlp.pipeline.StanfordCoreNLPServer &
    sleep 5


    # 运行主脚本（切分数据集）
    cd /workspace/src
    python nltk_downloader.py
    bash run_for_bird.sh "$SPLIT"
    conda deactivate

elif [ "$MODE" = "llama_factory" ]; then
    source /opt/conda/bin/activate llama_factory
    cd /workspace/src
    bash run.sh "$SPLIT"
    conda deactivate

else
    echo "Unknown mode: $MODE"
    exit 1
fi