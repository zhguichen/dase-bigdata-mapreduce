#!/bin/bash
# Complete automation script for Task 3: Different Workload Comparison

set -e

TASK_DIR="/root/Exp-hadoop/EXP/task3"
PROJECT_ROOT="/root/Exp-hadoop"

echo "============================================================"
echo "Task 3: Different Workload Comparison - Complete Workflow"
echo "============================================================"
echo ""
echo "This script will:"
echo "  1. Activate Python virtual environment"
echo "  2. Generate WordCount test data (1GB)"
echo "  3. Compile WordCount.java"
echo "  4. Run all experiments (WordCount + TeraSort)"
echo "  5. Save results"
echo ""
echo "Estimated total time: 40-80 minutes"
echo "============================================================"
echo ""

read -p "Press Enter to continue or Ctrl+C to cancel..."

# Step 1: Activate virtual environment
echo ""
echo "============================================================"
echo "Step 1: Activating Python Virtual Environment"
echo "============================================================"
cd $PROJECT_ROOT

if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    uv venv
fi

source .venv/bin/activate
echo "✓ Virtual environment activated"

# Check if required packages are installed
echo "Checking Python dependencies..."
if ! python3 -c "import pandas" 2>/dev/null; then
    echo "Installing dependencies..."
    uv pip install -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/
fi
echo "✓ Python dependencies OK"

# Step 2: Generate WordCount data
echo ""
echo "============================================================"
echo "Step 2: Generating WordCount Test Data"
echo "============================================================"
cd $TASK_DIR

if [ -f "data/input_wordcount_1gb.txt" ]; then
    size=$(du -h data/input_wordcount_1gb.txt | cut -f1)
    echo "WordCount data already exists (size: $size)"
    read -p "Regenerate? (y/N): " regenerate
    if [ "$regenerate" = "y" ] || [ "$regenerate" = "Y" ]; then
        python3 scripts/generate_data.py
    else
        echo "Using existing data"
    fi
else
    python3 scripts/generate_data.py
fi

echo "✓ Data generation complete"

# Step 3: Compile WordCount
echo ""
echo "============================================================"
echo "Step 3: Compiling WordCount.java"
echo "============================================================"
cd $TASK_DIR

chmod +x compile.sh
./compile.sh

echo "✓ Compilation complete"

# Step 4: Check Hadoop cluster status
echo ""
echo "============================================================"
echo "Step 4: Checking Hadoop Cluster Status"
echo "============================================================"

echo "Checking HDFS..."
if hdfs dfs -ls / >/dev/null 2>&1; then
    echo "✓ HDFS is accessible"
else
    echo "✗ HDFS is not accessible"
    echo "Please ensure Hadoop cluster is running"
    exit 1
fi

echo ""
echo "Checking YARN..."
if yarn node -list 2>/dev/null | grep -q "Total Nodes"; then
    echo "✓ YARN is running"
    yarn node -list | grep "Total Nodes"
else
    echo "⚠ Could not verify YARN status"
fi

# Step 5: Run experiments
echo ""
echo "============================================================"
echo "Step 5: Running Experiments"
echo "============================================================"
echo ""
echo "This will run:"
echo "  - WordCount with slowstart = 0.05, 0.50, 1.00 (3 runs each)"
echo "  - TeraSort with slowstart = 0.05, 0.50, 1.00 (3 runs each)"
echo "  Total: 18 experiments"
echo ""
echo "Note: TeraSort data will be generated automatically using TeraGen"
echo ""

read -p "Press Enter to start experiments..."

cd $TASK_DIR
python3 scripts/run_experiment.py

# Step 6: Display results
echo ""
echo "============================================================"
echo "Step 6: Experiment Results"
echo "============================================================"
echo ""

if [ -f "results/table4_terasort_summary.csv" ]; then
    echo "Table 4: TeraSort (IO-intensive) Summary"
    echo "─────────────────────────────────────────────────────────"
    cat results/table4_terasort_summary.csv
    echo ""
fi

if [ -f "results/table5_wordcount_summary.csv" ]; then
    echo "Table 5: WordCount (CPU-intensive) Summary"
    echo "─────────────────────────────────────────────────────────"
    cat results/table5_wordcount_summary.csv
    echo ""
fi

echo "============================================================"
echo "✓ Task 3 Complete!"
echo "============================================================"
echo ""
echo "Results saved in: $TASK_DIR/results/"
echo "  - raw_results.json"
echo "  - results.csv"
echo "  - table4_terasort_summary.csv"
echo "  - table5_wordcount_summary.csv"
echo ""
echo "Next steps:"
echo "  1. Review the summary tables above"
echo "  2. Compare TeraSort vs WordCount performance"
echo "  3. Analyze which workload is more sensitive to slowstart"
echo "  4. Create visualizations for your report"
echo ""
echo "Web UIs:"
echo "  - ResourceManager: http://47.116.119.3:8088"
echo "  - JobHistory: http://47.116.119.3:19888"
echo ""

