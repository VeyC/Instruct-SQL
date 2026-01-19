import os
import nltk

nltk_data_dir = "../model/nltk"
nltk.data.path.append(nltk_data_dir)

def resource_exists(resource):
    # resource: 'tokenizers/punkt' 或 'corpora/stopwords'
    path = os.path.join(nltk_data_dir, resource.replace('/', os.sep))
    return os.path.isdir(path) and bool(os.listdir(path))

if not resource_exists('tokenizers/punkt'):
    nltk.download('punkt', download_dir=nltk_data_dir)
else:
    print("punkt 已存在，跳过下载。")

if not resource_exists('corpora/stopwords'):
    nltk.download('stopwords', download_dir=nltk_data_dir)
else:
    print("stopwords 已存在，跳过下载。")