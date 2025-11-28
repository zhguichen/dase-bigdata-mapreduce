#!/bin/bash
# Complete automation script for Task 4
# This script runs the entire experiment pipeline

set -e

echo "============================================================"
echo "Task 4: Data Skew Testing"
echo "Complete Automation Script"
echo "============================================================"

# Navigate to task directory
cd /root/Exp-hadoop/EXP/task4

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
    cd /root/Exp-hadoop/EXP/task4
else
    echo "Virtual environment exists in main directory, activating..."
    source "$PROJECT_ROOT/.venv/bin/activate"
fi

echo ""
echo "Step 2: Generating test data (Skewed + Uniform, 1GB each)"
echo "------------------------------------------------------------"
echo "This will generate two datasets:"
echo "  1. Skewed data: 60% hotkey concentration"
echo "  2. Uniform data: evenly distributed (control group)"
echo ""

# Check which files are missing
need_generation=false
for dtype in "skewed" "uniform"; do
    if [ ! -f "data/input_${dtype}_1gb.txt" ]; then
        echo "Missing: input_${dtype}_1gb.txt"
        need_generation=true
    else
        echo "Exists: input_${dtype}_1gb.txt ($(ls -lh data/input_${dtype}_1gb.txt | awk '{print $5}'))"
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
echo "This will run 18 experiments:"
echo "  - 2 data types (skewed, uniform)"
echo "  - 3 slowstart values (0.05, 0.50, 1.00)"
echo "  - 3 runs per configuration"
echo ""
echo "Estimated time: 40-90 minutes depending on cluster performance"
echo ""
echo "IMPORTANT: After experiments complete, you will need to:"
echo "  1. Access JobHistory Web UI (http://47.116.112.198:19888)"
echo "  2. Record individual Reduce task times for each job"
echo "  3. Fill in the reduce_tasks_template.csv file"
echo ""
read -p "Press Enter to start experiments, or Ctrl+C to cancel..."

# Ensure virtual environment is activated
source "$PROJECT_ROOT/.venv/bin/activate"

python3 scripts/run_experiment.py

echo ""
echo "============================================================"
echo "✓ Task 4 Experiments Complete!"
echo "============================================================"
echo ""
echo "Results location: /root/Exp-hadoop/EXP/task4/results/"
echo ""
echo "Generated files:"
echo "  - Raw data: results/raw_results.json"
echo "  - Individual runs: results/results.csv"
echo "  - Basic summary: results/summary_basic.csv"
echo "  - Reduce task template: results/reduce_tasks_template.csv"
echo ""
echo "============================================================"
echo "NEXT STEPS - Manual Analysis Required"
echo "============================================================"
echo ""
echo "1. Access JobHistory Web UI:"
echo "   http://47.116.112.198:19888"
echo ""
echo "2. For each experiment job:"
echo "   - Click on the job to view details"
echo "   - Go to 'Tasks' -> 'Reduce' tasks"
echo "   - Record the finish time of each Reduce task"
echo "   - Calculate: min, avg, max, stddev of Reduce completion times"
echo ""
echo "3. Fill in results/reduce_tasks_template.csv with:"
echo "   - min_reduce_time: Earliest Reduce finish time"
echo "   - avg_reduce_time: Average of all Reduce finish times"
echo "   - max_reduce_time: Latest Reduce finish time (straggler)"
echo "   - reduce_stddev: Standard deviation (indicates skew severity)"
echo ""
echo "4. Analyze results by comparing:"
echo "   - Skewed vs Uniform data"
echo "   - Effect of different slowstart values on long-tail tasks"
echo "   - Whether early slowstart helps mitigate data skew"
echo ""
echo "5. Key metrics to focus on:"
echo "   - Time difference between fastest and slowest Reduce"
echo "   - Standard deviation of Reduce completion times"
echo "   - Percentage of time spent waiting for straggler"
echo ""
echo "Commands to retrieve job logs:"
echo "  mapred job -list"
echo "  mapred job -status <job_id>"
echo "  yarn logs -applicationId <application_id> | less"
echo ""
echo "============================================================"

