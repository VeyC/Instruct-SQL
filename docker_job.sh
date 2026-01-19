#docker build -t text2sql-multi .           这一步是建立docker
cd temp
docker run --rm -it \
  -v "$PWD/output":/workspace/output \
  text2sql-multi dail-sql test
#在进行这一步前吧colunmn_meaning.json放到test目录下面
docker run -it --rm \
  -e OPENAI_API_KEY=$OPENAI_API_KEY \
  -v "$PWD/output":/workspace/output \
  text2sql-multi llama_factory test
