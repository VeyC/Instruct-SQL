FROM continuumio/miniconda3

WORKDIR /workspace

COPY dail_requirements.txt /workspace/dail_requirements.txt
COPY llama_requirements.txt /workspace/llama_requirements.txt
RUN apt-get update && apt-get install -y wget unzip
# ------ 创建 dail-sql 环境（Python3.8 + openjdk + requirements） ------
RUN conda create -n dail-sql -c conda-forge python=3.8 openjdk=17 && \
    /opt/conda/envs/dail-sql/bin/pip install -r /workspace/dail_requirements.txt

# ------ 创建 llama_factory 环境（Python3.10 + openjdk + CUDA包 + requirements） ------
RUN conda create -n llama_factory -c conda-forge python=3.10 openjdk=21.0.6 && \
    /opt/conda/envs/llama_factory/bin/pip install torch==2.2.0+cu121 torchvision==0.17.0+cu121 torchaudio==2.2.0+cu121 --index-url https://download.pytorch.org/whl/cu121 && \
    /opt/conda/envs/llama_factory/bin/pip install -r /workspace/llama_requirements.txt

COPY . /workspace
# 下载并解压 Stanford CoreNLP
RUN mkdir -p /workspace/model/third_party
RUN cd /workspace/model/third_party && \
    wget http://nlp.stanford.edu/software/stanford-corenlp-full-2018-10-05.zip && \
    unzip stanford-corenlp-full-2018-10-05.zip && \
    rm stanford-corenlp-full-2018-10-05.zip
# expose 常用端口 (如 CoreNLP 默认 9000)
EXPOSE 9000

COPY custom_entry.sh /workspace/custom_entry.sh
RUN chmod +x /workspace/custom_entry.sh

ENTRYPOINT ["/workspace/custom_entry.sh"]
CMD ["dail-sql", "test"]