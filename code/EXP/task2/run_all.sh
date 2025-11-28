#!/bin/bash
# Complete automation script for Task 2
# This script runs the entire experiment pipeline

set -e

echo "============================================================"
echo "Task 2: Data Scalability Testing"
echo "Complete Automation Script"
echo "============================================================"

# Navigate to task directory
cd /root/Exp-hadoop/EXP/task2

echo ""
echo "Step 1: Setting up Python environment"
echo "------------------------------------------------------------"
# Use virtual environment from main directory
PROJECT_ROOT="/root/Exp-hadoop"
if [ ! -d "$PROJECT_ROOT/.venv" ]; then
    echo "Creating virtual environment in main directory with uv..."
    cd "$PROJECT_ROOT"
    uv venv
    source .venv/bin/activate
    echo "Installing dependencies with Aliyun mirror..."
    if [ -f "$PROJECT_ROOT/requirements.txt" ]; then
        uv pip install -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/
    else
        uv pip install -i https://mirrors.aliyun.com/pypi/simple/ pandas matplotlib numpy
    fi
    cd /root/Exp-hadoop/EXP/task2
else
    echo "Virtual environment exists in main directory, activating..."
    source "$PROJECT_ROOT/.venv/bin/activate"
fi

echo ""
echo "Step 2: Generating test data (500MB, 1GB, 2GB)"
echo "------------------------------------------------------------"
echo "This step will generate multiple data files."
echo "If files already exist, you'll be prompted to regenerate."
echo ""

# Check which files are missing
need_generation=false
for size in "500mb" "1gb" "2gb"; do
    if [ ! -f "data/input_${size}.txt" ]; then
        echo "Missing: input_${size}.txt"
        need_generation=true
    else
        echo "Exists: input_${size}.txt ($(ls -lh data/input_${size}.txt | awk '{print $5}'))"
    fi
done

if [ "$need_generation" = true ]; then
    echo ""
    echo "Some data files are missing. Generating..."
    python3 scripts/generate_data.py
else
    echo ""
    echo "All data files exist."
    read -p "Regenerate data files? (y/N): " regenerate
    if [[ "$regenerate" =~ ^[Yy]$ ]]; then
        python3 scripts/generate_data.py
    fi
fi

echo ""
echo "Step 3: Compiling WordCount program"
echo "------------------------------------------------------------"
if [ ! -f "wordcount.jar" ]; then
    echo "Compiling WordCount.java..."
    chmod +x compile.sh
    ./compile.sh
else
    echo "WordCount JAR already exists, recompiling..."
    chmod +x compile.sh
    ./compile.sh
fi

echo ""
echo "Step 4: Running experiments"
echo "------------------------------------------------------------"
echo "This will run 27 experiments:"
echo "  - 3 data sizes (500MB, 1GB, 2GB)"
echo "  - 3 slowstart values (0.05, 0.50, 1.00)"
echo "  - 3 runs per configuration"
echo ""
echo "Estimated time: 60-120 minutes depending on cluster performance"
echo ""
read -p "Press Enter to start experiments, or Ctrl+C to cancel..."

# Ensure virtual environment is activated
source "$PROJECT_ROOT/.venv/bin/activate"

python3 scripts/run_experiment.py

echo ""
echo "============================================================"
echo "✓ Task 2 Complete!"
echo "============================================================"
echo ""
echo "Results location: /root/Exp-hadoop/EXP/task2/results/"
echo ""
echo "Generated files:"
echo "  - Raw data: results/raw_results.json"
echo "  - Individual runs: results/results.csv"
echo "  - Summary table (Table 3): results/table3_summary.csv"
echo ""
echo "Next steps:"
echo "  1. Review the results: cat results/table3_summary.csv"
echo "  2. Analyze which slowstart value performs best for each data size"
echo "  3. Check if optimal slowstart changes with data scale"
echo "  4. Create visualizations using the CSV data"
echo "  5. Proceed to Task 3 (different workload types)"
echo ""
echo "============================================================"

