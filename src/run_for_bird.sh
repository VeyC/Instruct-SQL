#!/usr/bin/env bash
BASE="$(cd "$(dirname "$0")" && pwd)"
DATA_ROOT="$BASE/../datasets/bird"
set -euo pipefail

MODE="${1:-dev}"
if [[ "$MODE" != "dev" && "$MODE" != "test" ]]; then
  echo "Usage: $0 [dev|test]"
  exit 2
fi
python dail_data_preprocess.py --data_dir "$DATA_ROOT" --mode "$MODE"   # 处理dev

python dail_generate_question.py --data_type bird \
--split test --tokenizer gpt-3.5-turbo --prompt_repr SQL \
--selector_type EUCDISQUESTIONMASK --max_seq_len 4096 --k_shot 9 --example_type QA --mode "$MODE" 