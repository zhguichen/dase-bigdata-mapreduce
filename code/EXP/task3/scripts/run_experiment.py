#!/usr/bin/env python3
"""
Automated experiment runner for Task 3: Different Workload Comparison.
Compares IO-intensive (TeraSort) vs CPU-intensive (WordCount) jobs with different slowstart values.
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
HDFS_BASE_DIR = '/user/root/task3'
LOCAL_DATA_DIR = '/root/Exp-hadoop/EXP/task3/data'
WORDCOUNT_JAR = '/root/Exp-hadoop/EXP/task3/wordcount.jar'
TERASORT_JAR = f'{HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar'
NUM_REDUCERS = 4

# Fixed data size for comparison
DATA_SIZE = '1GB'
WORDCOUNT_INPUT_FILE = 'input_wordcount_1gb.txt'

# TeraGen configuration (100-byte records)
# 1GB = 1,073,741,824 bytes / 100 bytes per record = ~10,737,418 records
TERAGEN_NUM_RECORDS = 10737418  # Approximately 1GB

SLOWSTART_VALUES = [0.05, 0.10, 0.20, 0.30, 0.50, 0.70, 0.80, 0.90, 1.00]
RUNS_PER_CONFIG = 3
# SLOWSTART_VALUES = [ 0.50]
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
    
    def upload_wordcount_data(self):
        """Upload WordCount input data to HDFS."""
        hdfs_input_dir = f"{HDFS_BASE_DIR}/input_wordcount"
        local_file = os.path.join(LOCAL_DATA_DIR, WORDCOUNT_INPUT_FILE)
        
        print(f"\n  Uploading WordCount data to HDFS...")
        print(f"  Local file: {local_file}")
        print(f"  HDFS path: {hdfs_input_dir}")
        
        # Check if local file exists
        if not os.path.exists(local_file):
            print(f"  ✗ Error: Local file not found: {local_file}")
            print("  Please run: python3 scripts/generate_data.py")
            sys.exit(1)
        
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
    
    def generate_terasort_data(self):
        """Generate TeraSort input data using TeraGen."""
        hdfs_input_dir = f"{HDFS_BASE_DIR}/input_terasort"
        
        print(f"\n  Generating TeraSort data using TeraGen...")
        print(f"  Records: {TERAGEN_NUM_RECORDS:,} (~1GB)")
        print(f"  HDFS path: {hdfs_input_dir}")
        
        # Remove existing directory
        self.run_command(f"hdfs dfs -rm -r -f {hdfs_input_dir}")
        
        # Find TeraSort JAR
        terasort_jar = self.get_terasort_jar()
        if not terasort_jar:
            print("  ✗ Error: TeraSort JAR not found")
            sys.exit(1)
        
        # Run TeraGen
        cmd = f"hadoop jar {terasort_jar} teragen {TERAGEN_NUM_RECORDS} {hdfs_input_dir}"
        
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
    
    def clean_output_dir(self, output_dir):
        """Remove HDFS output directory if it exists."""
        self.run_command(f"hdfs dfs -rm -r -f {output_dir}")
    
    def run_wordcount_job(self, slowstart, run_number, hdfs_input_dir):
        """Run a single WordCount job with specified parameters."""
        output_dir = f"{HDFS_BASE_DIR}/output_wordcount_s{int(slowstart*100):03d}_run{run_number}"
        
        print(f"\n    Run #{run_number}: slowstart={slowstart}")
        print(f"    Output: {output_dir}")
        
        # Clean output directory
        self.clean_output_dir(output_dir)
        
        # Construct Hadoop command
        cmd = f"hadoop jar {WORDCOUNT_JAR} WordCount " \
              f"{hdfs_input_dir} {output_dir} {slowstart} {NUM_REDUCERS}"
        
        print(f"    Command: {cmd}")
        
        # Record submit time
        submit_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Run the job
        stdout, stderr, code = self.run_command(cmd)
        
        # Extract job information
        job_id, application_id = self.extract_job_info(stdout + stderr)
        
        if code != 0:
            print(f"    ✗ Job failed with exit code {code}")
            print(f"    Error: {stderr[:500]}")
            return None
        
        print(f"    ✓ Job completed successfully")
        if job_id:
            print(f"    Job ID: {job_id}")
        if application_id:
            print(f"    Application ID: {application_id}")
        
        # Only record basic info
        metrics = {
            'job_type': 'WordCount',
            'workload_type': 'CPU-intensive',
            'slowstart': slowstart,
            'run_number': run_number,
            'job_id': job_id or 'unknown',
            'application_id': application_id or 'unknown',
            'submit_time': submit_time,
            'num_reducers': NUM_REDUCERS
        }
        
        return metrics
    
    def run_terasort_job(self, slowstart, run_number, hdfs_input_dir):
        """Run a single TeraSort job with specified parameters."""
        output_dir = f"{HDFS_BASE_DIR}/output_terasort_s{int(slowstart*100):03d}_run{run_number}"
        
        print(f"\n    Run #{run_number}: slowstart={slowstart}")
        print(f"    Output: {output_dir}")
        
        # Clean output directory
        self.clean_output_dir(output_dir)
        
        # Find TeraSort JAR
        terasort_jar = self.get_terasort_jar()
        if not terasort_jar:
            print("    ✗ Error: TeraSort JAR not found")
            return None
        
        # Construct Hadoop command with configuration parameters
        cmd = f"hadoop jar {terasort_jar} terasort " \
              f"-Dmapreduce.job.reduce.slowstart.completedmaps={slowstart} " \
              f"-Dmapreduce.job.reduces={NUM_REDUCERS} " \
              f"{hdfs_input_dir} {output_dir}"
        
        print(f"    Command: {cmd}")
        
        # Record submit time
        submit_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Run the job
        stdout, stderr, code = self.run_command(cmd)
        
        # Extract job information
        job_id, application_id = self.extract_job_info(stdout + stderr)
        
        if code != 0:
            print(f"    ✗ Job failed with exit code {code}")
            print(f"    Error: {stderr[:500]}")
            return None
        
        print(f"    ✓ Job completed successfully")
        if job_id:
            print(f"    Job ID: {job_id}")
        if application_id:
            print(f"    Application ID: {application_id}")
        
        # Only record basic info
        metrics = {
            'job_type': 'TeraSort',
            'workload_type': 'IO-intensive',
            'slowstart': slowstart,
            'run_number': run_number,
            'job_id': job_id or 'unknown',
            'application_id': application_id or 'unknown',
            'submit_time': submit_time,
            'num_reducers': NUM_REDUCERS
        }
        
        return metrics
    
    def extract_job_info(self, output):
        """Extract job ID and application ID from command output."""
        job_id = None
        application_id = None
        
        for line in output.split('\n'):
            if 'Running job:' in line:
                match = re.search(r'job_\d+_\d+', line)
                if match:
                    job_id = match.group(0)
            if 'Submitted application' in line:
                match = re.search(r'application_\d+_\d+', line)
                if match:
                    application_id = match.group(0)
        
        return job_id, application_id
    
    def run_wordcount_experiments(self, hdfs_input_dir):
        """Run all WordCount experiments."""
        print(f"\n{'='*80}")
        print(f"Testing WordCount (CPU-intensive)")
        print(f"{'='*80}")
        
        for slowstart in SLOWSTART_VALUES:
            print(f"\n  {'─'*76}")
            print(f"  Testing slowstart = {slowstart}")
            print(f"  {'─'*76}")
            
            for run in range(1, RUNS_PER_CONFIG + 1):
                metrics = self.run_wordcount_job(slowstart, run, hdfs_input_dir)
                
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
    
    def run_terasort_experiments(self, hdfs_input_dir):
        """Run all TeraSort experiments."""
        print(f"\n{'='*80}")
        print(f"Testing TeraSort (IO-intensive)")
        print(f"{'='*80}")
        
        for slowstart in SLOWSTART_VALUES:
            print(f"\n  {'─'*76}")
            print(f"  Testing slowstart = {slowstart}")
            print(f"  {'─'*76}")
            
            for run in range(1, RUNS_PER_CONFIG + 1):
                metrics = self.run_terasort_job(slowstart, run, hdfs_input_dir)
                
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
        """Run all experiments for both workload types."""
        print("\n" + "="*80)
        print("Task 3: Different Workload Comparison")
        print("="*80)
        print(f"Configuration:")
        print(f"  - Data Size: {DATA_SIZE} (fixed)")
        print(f"  - Workloads: WordCount (CPU-intensive), TeraSort (IO-intensive)")
        print(f"  - Slowstart Values: {SLOWSTART_VALUES}")
        print(f"  - Runs per Configuration: {RUNS_PER_CONFIG}")
        print(f"  - Number of Reducers: {NUM_REDUCERS}")
        print("="*80)
        
        total_experiments = 2 * len(SLOWSTART_VALUES) * RUNS_PER_CONFIG
        print(f"\nTotal experiments to run: {total_experiments}")
        print(f"  - WordCount: {len(SLOWSTART_VALUES) * RUNS_PER_CONFIG} experiments")
        print(f"  - TeraSort: {len(SLOWSTART_VALUES) * RUNS_PER_CONFIG} experiments")
        print(f"Estimated time: {total_experiments * 2}-{total_experiments * 5} minutes")
        print()
        
        # Prepare data
        print("="*80)
        print("Step 1: Preparing Data")
        print("="*80)
        
        wordcount_input = self.upload_wordcount_data()
        terasort_input = self.generate_terasort_data()
        
        # Run WordCount experiments
        print("\n" + "="*80)
        print("Step 2: Running WordCount Experiments")
        print("="*80)
        self.run_wordcount_experiments(wordcount_input)
        
        # Run TeraSort experiments
        print("\n" + "="*80)
        print("Step 3: Running TeraSort Experiments")
        print("="*80)
        self.run_terasort_experiments(terasort_input)
    
    def save_results(self):
        """Save experimental results to JSON file with timestamp."""
        results_dir = '/root/Exp-hadoop/EXP/task3/results'
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
                    'data_size': DATA_SIZE,
                    'slowstart_values': SLOWSTART_VALUES,
                    'runs_per_config': RUNS_PER_CONFIG,
                    'num_reducers': NUM_REDUCERS,
                    'teragen_records': TERAGEN_NUM_RECORDS
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
            print(f"⚠ Warning: Timing extraction tool not found")
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
    print("Task 3: Different Workload Comparison - Experiment Runner")
    print("="*80)
    
    # Check if WordCount JAR exists
    if not os.path.exists(WORDCOUNT_JAR):
        print(f"\n✗ Error: WordCount JAR not found at {WORDCOUNT_JAR}")
        print("  Please compile WordCount.java first:")
        print("  cd /root/Exp-hadoop/EXP/task3 && ./compile.sh")
        sys.exit(1)
    
    # Check if WordCount data exists
    wordcount_data = os.path.join(LOCAL_DATA_DIR, WORDCOUNT_INPUT_FILE)
    if not os.path.exists(wordcount_data):
        print(f"\n✗ Error: WordCount data not found at {wordcount_data}")
        print("  Please generate data first:")
        print("  python3 scripts/generate_data.py")
        sys.exit(1)
    
    runner = ExperimentRunner()
    
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

