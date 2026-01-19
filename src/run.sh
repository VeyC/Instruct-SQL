#!/usr/bin/env bash
# run.sh - run the full pipeline for dev or test
# Usage: ./run.sh [dev|test]
set -euo pipefail

MODE="${1:-dev}"
if [[ "$MODE" != "dev" && "$MODE" != "test" ]]; then
  echo "Usage: $0 [dev|test]"
  exit 2
fi

# ---------- Configuration (adjust these paths if needed) ----------
# Java (set JAVA_HOME before running if you want a custom one)
: "${JAVA_HOME:=/usr/lib/jvm/jdk-11.0.22+7}"   # default, replace if needed
export JAVA_HOME
export PATH="$JAVA_HOME/bin:$PATH"

# Base folders
BASE="$(cd "$(dirname "$0")" && pwd)"
DATA_ROOT="$BASE/../datasets/bird"
OUTPUT_ROOT="$BASE/../output/bird"

# pipeline_nodes='schema_linking+schema_linking_info+sql_generation+sql_style_refinement+sql_output_refinement+sql_selection'
pipeline_nodes='schema_linking+schema_linking_info+sql_generation+sql_style_refinement+sql_output_refinement+sql_selection'
# pipeline_nodes='schema_linking+schema_linking_info'

# Per-mode paths
if [[ "$MODE" == "dev" ]]; then
  DB_ROOT="$DATA_ROOT/"
  INPUT_JSON="$DATA_ROOT/dev/dev.json"
  TABLES_JSON="$DATA_ROOT/dev/dev_tables.json"
  INDEX_ROOT="$OUTPUT_ROOT/dev/db_contents_index"
  TEMP_DIR="$OUTPUT_ROOT/dev/temp"
  INTERMEDIATE_JSON="$OUTPUT_ROOT/dev/dev_bird.json"
  METADATA_OUTPUT="$OUTPUT_ROOT/dev/dev_bird_metadata.json"
  MEANING_FILE="$OUTPUT_ROOT/dev/column_meaning.json"  
  TABLE_DESC_FILE="$OUTPUT_ROOT/dev/table_desc.json"
  DB_PATH="$DATA_ROOT/dev/dev_databases"
  MODEL_NAME="claude-sonnet-4-20250514"
  N_RUNS=4
else
  DB_ROOT="$DATA_ROOT/"
  INPUT_JSON="$DATA_ROOT/test/test.json"
  TABLES_JSON="$DATA_ROOT/test/test_tables.json"
  INDEX_ROOT="$OUTPUT_ROOT/test/db_contents_index"
  TEMP_DIR="$OUTPUT_ROOT/test/temp"
  INTERMEDIATE_JSON="$OUTPUT_ROOT/test/test_bird.json"
  METADATA_OUTPUT="$OUTPUT_ROOT/test/test_bird_metadata.json"
  MEANING_FILE="$OUTPUT_ROOT/test/column_meaning.json"
  TABLE_DESC_FILE="$OUTPUT_ROOT/test/table_desc.json"
  DB_PATH="$DATA_ROOT/test/test_databases"
  MODEL_NAME="claude-sonnet-4-20250514"
  N_RUNS=1
fi


# Parallel threads for index building
THREADS=16

# Engines for pipeline (can override by setting env vars before running)
engine1="qwen3-coder-plus" #"gpt-4o"
engine2="claude-sonnet-4-20250514"
engine3="gpt-5"

# Create required output dirs
#mkdir -p "$INDEX_ROOT" "$TEMP_DIR" "$(dirname "$INTERMEDIATE_JSON")" "$(dirname "$METADATA_OUTPUT")"

echo "=== Running pipeline for mode: $MODE ==="
echo "JAVA_HOME=$JAVA_HOME"
echo "DB_ROOT=$DB_ROOT"
echo "INDEX_ROOT=$INDEX_ROOT"
echo "INPUT_JSON=$INPUT_JSON"
echo "INTERMEDIATE_JSON=$INTERMEDIATE_JSON"
echo "METADATA_OUTPUT=$METADATA_OUTPUT"

# ---------- Step 1: build contents index (requires a JVM) ----------
echo
echo ">>> Step 1: Build DB contents index (this step requires JVM)"
# Example: ensure build_contents_index.py supports these args; adjust if needed
python build_contents_index.py \
  --db-root "$DB_ROOT" \
  --index-root "$INDEX_ROOT" \
  --temp-dir "$TEMP_DIR" \
  --threads "$THREADS" \
  --mode  "$MODE"
echo "Step 1 done. Index stored at: $INDEX_ROOT"

# ---------- Step 2: process dataset (vector retrieval -> examples) ----------
echo
echo ">>> Step 2: Process dataset to get examples for each column"
bash process_dataset.sh \
  -i "$INPUT_JSON" \
  -o "$INTERMEDIATE_JSON" \
  -d "$DB_ROOT" \
  -t "$TABLES_JSON" \
  -s bird \
  -m "$MODE" \
  -v 2 \
  -c "$INDEX_ROOT"

echo "Step 2 done. Intermediate JSON: $INTERMEDIATE_JSON"

# python fd.py \
#   --table_desc_file "$TABLE_DESC_FILE" \
#   --db_path "/media/hnu/hnu2024/wangqin/python_work/Text2SQL_submit copy/datasets/bird/dev/dev_databases" \
#   --meaning_file "$MEANING_FILE" \
#   --mode "dev"\
#   --model_name "claude-sonnet-4-20250514"

# ---------- Step 3: derive table distributions & metadata ----------
echo
echo ">>> Step 3: Dataset distribution -> metadata & table descriptions"
# Note: your original script had a typo 'olumn_meaning.json' -> I assume 'column_meaning.json'
# If your file is named differently, change MEANING_FILE accordingly.
python dataset_process_for_submit.py \
  --mode "$MODE" \
  --input_file "$INPUT_JSON" \
  --intermediate_file "$INTERMEDIATE_JSON" \
  --output_file "$METADATA_OUTPUT" \
  --meaning_file "$MEANING_FILE" \
  --table_desc_file "$TABLE_DESC_FILE" \
  --db_path "$DB_PATH"

echo "Step 3 done. Metadata at: $METADATA_OUTPUT ; Table desc at: $TABLE_DESC_FILE"

# ---------- Step 4: run main.py to generate outputs ----------
echo
echo ">>> Step 4: Run main pipeline (greedy / single-run default)"

pipeline_setup='{
    "schema_linking": {
        "engine": "'${engine1}'",
        "n": 5,
        "temperature": [1.0, 0.4, 0.1]
    },
     "schema_linking_info": {
        "engine": "'${engine1}'",
        "n": 4,
        "temperature": [1.0, 0.4, 0.1]
    },
    "sql_generation": {
        "engine": "'${engine1}'"
    },
    "sql_style_refinement": {
        "engine": "'${engine1}'"
    },
    "sql_output_refinement": {
        "engine": "'${engine1}'"
    },
    "sql_finetuned_output_refine": {
        "finetuned_model": "/media/hnu/LLM/bge-m3"
    },
    "sql_correction": {
        "engine": "'${engine1}'"
    },
    "sql_selection": {
        "engine": "'${engine3}'"
    }
}'

# 可以每种方案生成4个候选答案，即n=4

python -u ./main.py \
  --mode "$MODE" \
  --type major \
  --input_file "$METADATA_OUTPUT" \
  --model_name "$MODEL_NAME" \
  --pipeline_setup "$pipeline_setup" \
  --pipeline_nodes ${pipeline_nodes} \
  --db_root_path "$DB_ROOT" \

echo "Step 4 done. Review output under $OUTPUT_ROOT/$MODE"

# ---------- Done ----------
echo
echo "=== Pipeline finished for mode: $MODE ==="









