# Preliminary Note
### 1. We need the **`column_meaning.json`** files.

Place the **`column_meaning.json`** of the **dev** dataset under:

```
$project/output/bird/dev/
```

Place the **`column_meaning.json`** of the **test** dataset under:

```
$project/output/bird/test/
```

### 2. All files under **`$project/datasets/bird`** are sourced from the official website. You may replace them as needed, but please keep the directory structure unchanged.


### 3. Our code takes approximately **80 hours** to run on the **dev** dataset and requires at least **96 GB of GPU memory** (e.g., **2 × A40 48 GB GPUs**). GPU resources will be utilized throughout the entire pipeline.

### 4. The results we have already completed on the **dev** dataset are saved at:
```
$project/output/bird/dev/final_prediction_reference.json
```



# Evaluation
Our output files for the **dev** dataset will be located at:
```
$project/output/bird/dev/final_prediction.json
```
Our output files for the **test** dataset will be located at:
```
$project/output/bird/test/final_prediction.json
```

# Install
1.You can run the code according to the following method. The code below is for performing the few-shot step
step1:
conda create -n dail-sql python=3.8
conda activate dail-sql

step2:
To set up the environment, you should download the stanford-cornlp（http://nlp.stanford.edu/software/stanford-corenlp-full-2018-10-05.zip） and unzip it to the folder ./model/third_party. Next, you need to launch the coreNLP server:
conda install -c conda-forge openjdk
cd model/third_party/stanford-corenlp-full-2018-10-05
nohup java -mx4g -cp "*" edu.stanford.nlp.pipeline.StanfordCoreNLPServer &
cd ../../../

step3:
cd src
pip install -r dail_requirements.txt

step4:
python nltk_downloader.py
sh run_for_bird.sh  test   #If you want to test the test set, please change "dev" to "test" in the sh file
conda deactivate
cd ..

#It takes 2-3 hours to run here
 
step1:
conda create -n llama_factory -c conda-forge python=3.10 openjdk=21.0.6
conda activate llama_factory
pip install torch==2.2.0+cu121 torchvision==0.17.0+cu121 torchaudio==2.2.0+cu121 --index-url https://download.pytorch.org/whl/cu121
#conda install -c conda-forge openjdk=21.0.6 -y
cd src
pip install -r llama_requirements.txt

step2:
sh run.sh dev #If you want to test the test set, please change "dev" to "test" in the commend



#docker--run
docker run -it --rm text2sql-multi                     # 等价于跑 dail-sql test
mkdir -p ./output/bird/test
# Step 1: 运行 dail-sql，挂载本地 output 目录
docker run --rm -it \
  -v "$PWD/output":/workspace/output \
  text2sql-multi dail-sql test

# Step 2: 运行 llama_factory，同样挂载本地 output 目录
docker run -it --rm \
  -e OPENAI_API_KEY=$OPENAI_API_KEY \
  -v "$PWD/output":/workspace/output \
  text2sql-multi llama_factory test