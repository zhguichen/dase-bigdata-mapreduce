#!/usr/bin/env python3
"""
Automated experiment runner for Task 2: Data Scalability Testing.
Tests different data sizes (500MB, 1GB, 2GB) with different slowstart values.
"""

import subprocess
import json
import time
import os
import re
from datetime import datetime
import sys

# Configuration
HADOOP_HOME = os.environ.get('HADOOP_HOME', '/opt/hadoop')
HDFS_BASE_DIR = '/user/root/task2'
LOCAL_DATA_DIR = '/root/Exp-hadoop/EXP/task2/data'
WORDCOUNT_JAR = '/root/Exp-hadoop/EXP/task2/wordcount.jar'
TERASORT_JAR = f'{HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar'
NUM_REDUCERS = 4

# Experiment task type: 'wordcount' or 'terasort'
TASK_TYPE = 'terasort'  # Options: 'wordcount', 'terasort'

# TeraGen configuration (100-byte records)
# Calculate records: size_bytes / 100 bytes per record
TERAGEN_RECORDS = {
    '500MB': 5368709,    # 500MB = 524,288,000 bytes / 100 ≈ 5,368,709 records
    '1GB': 10737418,     # 1GB = 1,073,741,824 bytes / 100 ≈ 10,737,418 records
    '1500MB': 16106127,  # 1500MB = 1,572,864,000 bytes / 100 ≈ 15,706,624 records
    '2GB': 21474836,     # 2GB = 2,147,483,648 bytes / 100 ≈ 21,474,836 records
}

# Test configurations
DATA_SIZES = [
    ('500MB', 'input_500mb.txt'),
    ('1GB', 'input_1gb.txt'),
    # ('2GB', 'input_2gb.txt'),
    ('1500MB', 'input_1500mb.txt'),
]

SLOWSTART_VALUES = [0.05, 0.10, 0.20, 0.30, 0.50, 0.70, 0.80, 0.90, 1.00]
RUNS_PER_CONFIG = 3
# SLOWSTART_VALUES = [0.5]
# RUNS_PER_CONFIG = 1

class ExperimentRunner:
    def __init__(self):
        self.results = []
        self.experiment_start_time = datetime.now()
        
    def run_command(self, command, shell=True):
        """Execute shell command and return output."""
        try:
            result = subprocess.run(
                command,
                shell=shell,
                capture_output=True,
                text=True,
                check=True
            )
            return result.stdout, result.stderr, 0
        except subprocess.CalledProcessError as e:
            return e.stdout, e.stderr, e.returncode
    
    def get_terasort_jar(self):
        """Find the TeraSort example JAR file."""
        # Use glob to find the JAR
        stdout, stderr, code = self.run_command(f"ls {TERASORT_JAR}")
        if code == 0:
            jar_files = stdout.strip().split('\n')
            if jar_files:
                return jar_files[0]
        return None
    
    def upload_wordcount_data(self, data_label, local_file):
        """Upload WordCount data file to HDFS."""
        task_prefix = TASK_TYPE.lower()
        hdfs_input_dir = f"{HDFS_BASE_DIR}/input_{task_prefix}_{data_label}"
        
        print(f"\n  Uploading WordCount {data_label} data to HDFS...")
        print(f"  Local file: {local_file}")
        print(f"  HDFS path: {hdfs_input_dir}")
        
        # Remove existing directory
        self.run_command(f"hdfs dfs -rm -r -f {hdfs_input_dir}")
        
        # Create input directory
        self.run_command(f"hdfs dfs -mkdir -p {hdfs_input_dir}")
        
        # Upload file
        start_time = time.time()
        stdout, stderr, code = self.run_command(
            f"hdfs dfs -put {local_file} {hdfs_input_dir}/"
        )
        upload_time = time.time() - start_time
        
        if code == 0:
            print(f"  ✓ Upload completed in {upload_time:.2f} seconds")
            return hdfs_input_dir
        else:
            print(f"  ✗ Upload failed: {stderr}")
            sys.exit(1)
    
    def generate_terasort_data(self, data_label):
        """Generate TeraSort input data using TeraGen."""
        task_prefix = TASK_TYPE.lower()
        hdfs_input_dir = f"{HDFS_BASE_DIR}/input_{task_prefix}_{data_label}"
        
        # Get number of records for this data size
        num_records = TERAGEN_RECORDS.get(data_label)
        if not num_records:
            # Calculate based on label if not in dictionary
            if '500' in data_label:
                num_records = TERAGEN_RECORDS['500MB']
            elif '1500' in data_label or '1.5' in data_label:
                num_records = TERAGEN_RECORDS['1500MB']
            elif '2' in data_label and 'GB' in data_label:
                num_records = TERAGEN_RECORDS['2GB']
            else:
                num_records = TERAGEN_RECORDS['1GB']  # Default to 1GB
        
        print(f"\n  Generating TeraSort {data_label} data using TeraGen...")
        print(f"  Records: {num_records:,} (~{data_label})")
        print(f"  HDFS path: {hdfs_input_dir}")
        
        # Remove existing directory
        self.run_command(f"hdfs dfs -rm -r -f {hdfs_input_dir}")
        
        # Find TeraSort JAR
        terasort_jar = self.get_terasort_jar()
        if not terasort_jar:
            print("  ✗ Error: TeraSort JAR not found")
            sys.exit(1)
        
        # Run TeraGen
        cmd = f"hadoop jar {terasort_jar} teragen {num_records} {hdfs_input_dir}"
        
        print(f"  Command: {cmd}")
        start_time = time.time()
        stdout, stderr, code = self.run_command(cmd)
        generation_time = time.time() - start_time
        
        if code == 0:
            print(f"  ✓ TeraGen completed in {generation_time:.2f} seconds")
            return hdfs_input_dir
        else:
            print(f"  ✗ TeraGen failed: {stderr[:500]}")
            sys.exit(1)
    
    def prepare_data(self, data_label, local_file=None):
        """Prepare data based on TASK_TYPE."""
        if TASK_TYPE.lower() == 'wordcount':
            if not local_file:
                raise ValueError("local_file is required for WordCount")
            return self.upload_wordcount_data(data_label, local_file)
        elif TASK_TYPE.lower() == 'terasort':
            return self.generate_terasort_data(data_label)
        else:
            print(f"✗ Error: Unknown task type: {TASK_TYPE}")
            sys.exit(1)
    
    def clean_output_dir(self, output_dir):
        """Remove HDFS output directory if it exists."""
        print(f"    Cleaning output directory: {output_dir}")
        stdout, stderr, code = self.run_command(f"hdfs dfs -rm -r -f {output_dir}")
        if code == 0:
            if stdout.strip():  # If there was output, directory existed and was deleted
                print(f"    ✓ Removed existing output directory")
            else:
                print(f"    ✓ Output directory doesn't exist (clean)")
        else:
            print(f"    ⚠ Warning: Failed to clean output directory")
            print(f"    Error: {stderr[:200]}")
            # Try to check if directory exists
            stdout2, stderr2, code2 = self.run_command(f"hdfs dfs -test -d {output_dir}")
            if code2 == 0:
                print(f"    ✗ Error: Output directory exists and couldn't be removed!")
                print(f"    Please manually remove: hdfs dfs -rm -r {output_dir}")
                sys.exit(1)
    
    def run_single_job(self, data_label, slowstart, run_number, hdfs_input_dir):
        """Run a single MapReduce job with specified parameters."""
        task_prefix = TASK_TYPE.lower()
        output_dir = f"{HDFS_BASE_DIR}/output_{task_prefix}_{data_label}_s{int(slowstart*100):03d}_run{run_number}"
        
        # Build job name with all parameters
        task_type_display = TASK_TYPE.capitalize()  # WordCount or TeraSort
        slowstart_str = f"s{int(slowstart*100):03d}"  # s005, s010, etc.
        job_name = f"Task2_{task_type_display}_{data_label}_{slowstart_str}_run{run_number}"
        
        print(f"\n    Run #{run_number}: slowstart={slowstart}")
        print(f"    Output: {output_dir}")
        print(f"    Job Name: {job_name}")
        
        # Clean output directory
        self.clean_output_dir(output_dir)
        
        # Construct Hadoop command based on task type
        if TASK_TYPE.lower() == 'wordcount':
            cmd = f"hadoop jar {WORDCOUNT_JAR} WordCount " \
                  f"-Dmapreduce.job.name={job_name} " \
                  f"{hdfs_input_dir} {output_dir} {slowstart} {NUM_REDUCERS}"
        elif TASK_TYPE.lower() == 'terasort':
            # Find TeraSort JAR
            terasort_jar = self.get_terasort_jar()
            if not terasort_jar:
                print("    ✗ Error: TeraSort JAR not found")
                return None
            cmd = f"hadoop jar {terasort_jar} terasort " \
                  f"-Dmapreduce.job.name={job_name} " \
                  f"-Dmapreduce.job.reduce.slowstart.completedmaps={slowstart} " \
                  f"-Dmapreduce.job.reduces={NUM_REDUCERS} " \
                  f"{hdfs_input_dir} {output_dir}"
        else:
            print(f"    ✗ Error: Unknown task type: {TASK_TYPE}")
            return None
        
        print(f"    Command: {cmd}")
        
        # Record start time
        start_time = time.time()
        submit_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Run the job
        stdout, stderr, code = self.run_command(cmd)
        
        # Record end time
        end_time = time.time()
        total_time = end_time - start_time
        
        # Extract job ID from output
        job_id = None
        application_id = None
        for line in (stdout + stderr).split('\n'):
            if 'Running job:' in line:
                match = re.search(r'job_\d+_\d+', line)
                if match:
                    job_id = match.group(0)
            if 'Submitted application' in line:
                match = re.search(r'application_\d+_\d+', line)
                if match:
                    application_id = match.group(0)
        
        if code != 0:
            print(f"    ✗ Job failed with exit code {code}")
            print(f"    Error: {stderr[:500]}")  # Print first 500 chars of error
            return None
        
        print(f"    ✓ Job completed successfully in {total_time:.2f} seconds")
        if job_id:
            print(f"    Job ID: {job_id}")
        if application_id:
            print(f"    Application ID: {application_id}")
        
        # Create metrics record
        metrics = {
            'task_type': TASK_TYPE.lower(),
            'data_size': data_label,
            'slowstart': slowstart,
            'run_number': run_number,
            'job_id': job_id or 'unknown',
            'application_id': application_id or 'unknown',
            'submit_time': submit_time,
            'total_time': total_time,
            'num_reducers': NUM_REDUCERS
        }
        
        return metrics
    
    def run_experiments_for_data_size(self, data_label, local_file):
        """Run all experiments for a specific data size."""
        print(f"\n{'='*80}")
        print(f"Testing with data size: {data_label}")
        print(f"{'='*80}")
        
        # Prepare data (upload WordCount or generate TeraSort)
        if TASK_TYPE.lower() == 'wordcount':
            hdfs_input_dir = self.prepare_data(data_label, local_file)
        else:
            hdfs_input_dir = self.prepare_data(data_label)
        
        # Test each slowstart value
        for slowstart in SLOWSTART_VALUES:
            print(f"\n  {'─'*76}")
            print(f"  Testing slowstart = {slowstart}")
            print(f"  {'─'*76}")
            
            for run in range(1, RUNS_PER_CONFIG + 1):
                metrics = self.run_single_job(
                    data_label, slowstart, run, hdfs_input_dir
                )
                
                if metrics:
                    self.results.append(metrics)
                
                # Short pause between runs
                if run < RUNS_PER_CONFIG:
                    print("    Waiting 5 seconds before next run...")
                    time.sleep(5)
            
            # Pause between slowstart values
            if slowstart != SLOWSTART_VALUES[-1]:
                print("\n  Waiting 10 seconds before next slowstart value...")
                time.sleep(10)
    
    def run_all_experiments(self):
        """Run all experiments for all data sizes."""
        print("\n" + "="*80)
        print("Task 2: Data Scalability Testing")
        print("="*80)
        print(f"Configuration:")
        print(f"  - Task Type: {TASK_TYPE}")
        print(f"  - Data Sizes: {', '.join([ds[0] for ds in DATA_SIZES])}")
        print(f"  - Slowstart Values: {SLOWSTART_VALUES}")
        print(f"  - Runs per Configuration: {RUNS_PER_CONFIG}")
        print(f"  - Number of Reducers: {NUM_REDUCERS}")
        print("="*80)
        
        total_experiments = len(DATA_SIZES) * len(SLOWSTART_VALUES) * RUNS_PER_CONFIG
        print(f"\nTotal experiments to run: {total_experiments}")
        print(f"Estimated time: {total_experiments * 3}-{total_experiments * 8} minutes")
        print()
        
        # Run experiments for each data size
        for idx, (data_label, filename) in enumerate(DATA_SIZES, 1):
            local_file = os.path.join(LOCAL_DATA_DIR, filename)
            
            # Check data file existence based on task type
            if TASK_TYPE.lower() == 'wordcount':
                if not os.path.exists(local_file):
                    print(f"\n✗ Error: WordCount data file not found: {local_file}")
                    print("  Please run: python3 scripts/generate_data.py")
                    continue
                file_size_gb = os.path.getsize(local_file) / (1024 * 1024 * 1024)
                print(f"\n[Data Size {idx}/{len(DATA_SIZES)}]")
                print(f"File: {filename} ({file_size_gb:.2f}GB)")
                self.run_experiments_for_data_size(data_label, local_file)
            elif TASK_TYPE.lower() == 'terasort':
                print(f"\n[Data Size {idx}/{len(DATA_SIZES)}]")
                print(f"Data size: {data_label} (will be generated using TeraGen)")
                self.run_experiments_for_data_size(data_label, None)
    
    def save_results(self):
        """Save experimental results to JSON file with timestamp."""
        results_dir = '/root/Exp-hadoop/EXP/task2/results'
        os.makedirs(results_dir, exist_ok=True)
        
        print("\n" + "="*80)
        print("Saving Results")
        print("="*80)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_file = os.path.join(results_dir, f'raw_results_{timestamp}.json')
        
        with open(json_file, 'w') as f:
            json.dump({
                'experiment_start': self.experiment_start_time.isoformat(),
                'experiment_end': datetime.now().isoformat(),
                'configuration': {
                    'task_type': TASK_TYPE,
                    'data_sizes': [ds[0] for ds in DATA_SIZES],
                    'slowstart_values': SLOWSTART_VALUES,
                    'runs_per_config': RUNS_PER_CONFIG,
                    'num_reducers': NUM_REDUCERS
                },
                'results': self.results
            }, f, indent=2)
        
        print(f"✓ Results saved to: {json_file}")
        print(f"  Total successful experiments: {len(self.results)}")
        
        return json_file
    
    def enhance_results(self, results_file):
        """Extract detailed timing info using extract_job_timing.py"""
        print("\n" + "="*80)
        print("Extracting Detailed Timing Information")
        print("="*80)
        
        tools_dir = '/root/Exp-hadoop/EXP/tools'
        extract_script = f'{tools_dir}/extract_job_timing.py'
        
        if not os.path.exists(extract_script):
            print(f"⚠ Warning: Timing extraction tool not found at {extract_script}")
            return
        
        print(f"Running: python3 {extract_script} --batch {results_file}")
        stdout, stderr, code = self.run_command(
            f"cd {tools_dir} && python3 extract_job_timing.py --batch {results_file}"
        )
        
        if code == 0:
            enhanced_file = results_file.replace('.json', '_enhanced.json')
            if os.path.exists(enhanced_file):
                os.rename(enhanced_file, results_file)
                print(f"✓ Detailed timing information added to results")
        else:
            print(f"⚠ Warning: Failed to extract timing information")

def main():
    print("="*80)
    print("Task 2: Data Scalability Testing - Experiment Runner")
    print("="*80)
    
    # Validate task type
    if TASK_TYPE.lower() not in ['wordcount', 'terasort']:
        print(f"\n✗ Error: Invalid task type: {TASK_TYPE}")
        print("  Valid options: 'wordcount', 'terasort'")
        sys.exit(1)
    
    # Create runner instance for checks
    runner = ExperimentRunner()
    
    # Check if JAR file exists based on task type
    if TASK_TYPE.lower() == 'wordcount':
        if not os.path.exists(WORDCOUNT_JAR):
            print(f"\n✗ Error: WordCount JAR not found at {WORDCOUNT_JAR}")
            print("  Please compile WordCount.java first:")
            print("  cd /root/Exp-hadoop/EXP/task2 && ./compile.sh")
            sys.exit(1)
    elif TASK_TYPE.lower() == 'terasort':
        terasort_jar = runner.get_terasort_jar()
        if not terasort_jar:
            print(f"\n✗ Error: TeraSort JAR not found")
            print(f"  Expected location: {TERASORT_JAR}")
            print("  Please check Hadoop installation and HADOOP_HOME environment variable")
            sys.exit(1)
        print(f"✓ Found TeraSort JAR: {terasort_jar}")
    
    # Check data files existence based on task type
    if TASK_TYPE.lower() == 'wordcount':
        missing_files = []
        for data_label, filename in DATA_SIZES:
            local_file = os.path.join(LOCAL_DATA_DIR, filename)
            if not os.path.exists(local_file):
                missing_files.append(filename)
        
        if missing_files:
            print(f"\n⚠ Warning: Some WordCount data files are missing:")
            for filename in missing_files:
                print(f"  - {filename}")
            print("\n  Please generate data first:")
            print("  python3 scripts/generate_data.py")
            user_input = input("\nContinue with available files? (y/N): ").strip().lower()
            if user_input != 'y':
                sys.exit(1)
    elif TASK_TYPE.lower() == 'terasort':
        print(f"✓ TeraSort data will be generated using TeraGen (no local files needed)")
    
    try:
        # Run all experiments
        runner.run_all_experiments()
        
        # Save results
        results_file = runner.save_results()
        
        # Extract detailed timing information
        runner.enhance_results(results_file)
        
        print("\n" + "="*80)
        print("✓ All experiments completed successfully!")
        print("="*80)
        print(f"\nResults saved to: {results_file}")
        print("\nTo view results:")
        print(f"  cat {results_file} | python3 -m json.tool")
        
    except KeyboardInterrupt:
        print("\n\n✗ Experiments interrupted by user")
        if runner.results:
            print("  Saving partial results...")
            runner.save_results()
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Error during experiments: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()

