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

pipeline_nodes='schema_linking+schema_linking_info+sql_generation+sql_style_refinement+sql_output_refinement+sql_selection'
MODEL_NAME="Snowflake/Arctic-Text2SQL-R1-7B"
# ARCTIC_PATH="$BASE/../model"   # TODO 后续改回来
ARCTIC_PATH='/media/hnu/LLM/Arctic-Text2SQL-R1-7B'
ARCTIC_TEMPERATURE='0.8'
ARCTIC_N='8'



# Per-mode paths
DB_ROOT="$DATA_ROOT/"
INPUT_JSON="$DATA_ROOT/$MODE/$MODE.json"
TABLES_JSON="$DATA_ROOT/$MODE/${MODE}_tables.json"
INDEX_ROOT="$OUTPUT_ROOT/$MODE/db_contents_index"
TEMP_DIR="$OUTPUT_ROOT/$MODE/temp"
INTERMEDIATE_JSON="$OUTPUT_ROOT/$MODE/${MODE}_bird.json"
METADATA_OUTPUT="$OUTPUT_ROOT/$MODE/${MODE}_bird_metadata.json"
MEANING_FILE="$OUTPUT_ROOT/$MODE/column_meaning.json"
TABLE_DESC_FILE="$OUTPUT_ROOT/$MODE/table_desc.json"
DB_PATH="$DATA_ROOT/$MODE/${MODE}_databases"
MODEL_NAME="claude-sonnet-4-20250514"
PREDICT_OUTPUT="$OUTPUT_ROOT/$MODE/final_prediction.json"

# Parallel threads for index building
THREADS=16

# Engines for pipeline (can override by setting env vars before running)
engine1="qwen3-coder-plus"
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




# ---------- Step 0: optional dev tied-append processing ----------
if [[ "$MODE" == "dev" ]]; then
  echo
  echo ">>> Step 0: Apply tied append overrides for dev"
  APPEND_JSON="$DATA_ROOT/$MODE/${MODE}_tied_append.json"
  python "$BASE/apply_tied_append.py" \
    --input-json "$INPUT_JSON" \
    --append-json "$APPEND_JSON"
  echo "Step 0 done."
fi

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


# ---------- Step 3: download arctic model ----------
echo
echo ">>> Step 3: download arctic model"

echo "Check if the model already exists: $ARCTIC_PATH"

if [ -d "$ARCTIC_PATH" ] && [ "$(ls -A "$ARCTIC_PATH")" ]; then
  echo "The model already exists, skip downloading"
else
  echo "The model does not exist, start downloading ..."

  mkdir -p "$ARCTIC_PATH"

  huggingface-cli download \
    "$MODEL_NAME" \
    --local-dir "$ARCTIC_PATH" \
    --local-dir-use-symlinks False

  echo "Model download completed!"
fi
echo "Step 3 done. Model downloaded at: $ARCTIC_PATH"


# ---------- Step 4: derive table distributions & metadata ----------
echo
echo ">>> Step 4: Dataset distribution -> metadata & table descriptions"
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

echo "Step 4 done. Metadata at: $METADATA_OUTPUT ; Table desc at: $TABLE_DESC_FILE"



# ---------- Step 5: run main.py to generate outputs ----------
echo
echo ">>> Step 5: Run main pipeline (greedy / single-run default)"

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
    "sql_correction": {
        "engine": "'${engine1}'"
    },
    "sql_selection": {
        "engine": "'${engine1}'"
    }
}'


python -u ./main.py \
  --mode "$MODE" \
  --type major \
  --input_file "$METADATA_OUTPUT" \
  --output_file "$PREDICT_OUTPUT" \
  --model_name "$MODEL_NAME" \
  --pipeline_setup "$pipeline_setup" \
  --pipeline_nodes ${pipeline_nodes} \
  --db_root_path "$DB_ROOT" \
  --pretrained_model_name_or_path "$ARCTIC_PATH" \
  --temperature "$ARCTIC_TEMPERATURE" \
  --n "$ARCTIC_N"

echo "Step 5 done. Review output under $OUTPUT_ROOT/$MODE"


# ---------- Done ----------
echo
echo "=== Pipeline finished for mode: $MODE ==="




