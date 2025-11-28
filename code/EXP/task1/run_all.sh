#!/bin/bash
# Complete automation script for Task 1
# This script runs the entire experiment pipeline

set -e

echo "============================================================"
echo "Task 1: Slowstart Parameter Sensitivity Analysis"
echo "Complete Automation Script"
echo "============================================================"

# Navigate to task directory
cd /root/Exp-hadoop/EXP/task1

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
    cd /root/Exp-hadoop/EXP/task1
else
    echo "Virtual environment exists in main directory, activating..."
    source "$PROJECT_ROOT/.venv/bin/activate"
fi

echo ""
echo "Step 2: Generating test data (1GB)"
echo "------------------------------------------------------------"
if [ ! -f "data/input_1gb.txt" ]; then
    echo "Generating 1GB test data (this may take a few minutes)..."
    python3 scripts/generate_data.py
else
    echo "Test data already exists at data/input_1gb.txt"
    ls -lh data/input_1gb.txt
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
echo "This will run 15 experiments (5 slowstart values × 3 runs each)"
echo "Estimated time: 30-60 minutes depending on cluster performance"
echo ""
read -p "Press Enter to start experiments, or Ctrl+C to cancel..."

# Ensure virtual environment is activated
source "$PROJECT_ROOT/.venv/bin/activate"

python3 scripts/run_experiment.py

echo ""
echo "Step 5: Analyzing results and generating visualizations"
echo "------------------------------------------------------------"
python3 scripts/analyze_results.py

echo ""
echo "============================================================"
echo "✓ Task 1 Complete!"
echo "============================================================"
echo ""
echo "Results location: /root/Exp-hadoop/EXP/task1/results/"
echo ""
echo "Generated files:"
echo "  - Table 1: results/table1_individual_runs.csv"
echo "  - Table 2: results/table2_summary.csv"
echo "  - Figure 1: results/figure1_sensitivity_analysis.png"
echo "  - Analysis Report: results/analysis_report.txt"
echo ""
echo "You can view the results or proceed to Task 2."
echo "============================================================"


