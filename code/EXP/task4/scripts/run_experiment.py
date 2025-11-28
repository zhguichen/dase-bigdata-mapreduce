#!/usr/bin/env python3
"""
Automated experiment runner for Task 4: Data Skew Testing.
Tests skewed vs uniform data with different slowstart values.
Tracks individual Reduce task completion times to identify stragglers.
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
HDFS_BASE_DIR = '/user/root/task4'
LOCAL_DATA_DIR = '/root/Exp-hadoop/EXP/task4/data'
WORDCOUNT_JAR = '/root/Exp-hadoop/EXP/task4/wordcount.jar'
NUM_REDUCERS = 8  # Use 4 reducers to better observe skew effects

# Test configurations
DATA_TYPES = [
    ('skewed', 'input_skewed_1gb.txt'),
    ('uniform', 'input_uniform_1gb.txt'),
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
    
    def upload_data_to_hdfs(self, data_type, local_file):
        """Upload a specific data file to HDFS."""
        hdfs_input_dir = f"{HDFS_BASE_DIR}/input_{data_type}"
        
        print(f"\n  Uploading {data_type} data to HDFS...")
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
    
    def clean_output_dir(self, output_dir):
        """Remove HDFS output directory if it exists."""
        self.run_command(f"hdfs dfs -rm -r -f {output_dir}")
    
    def get_reduce_task_times(self, application_id):
        """
        Retrieve individual Reduce task completion times from YARN.
        Returns list of reduce finish times relative to job start.
        """
        try:
            # Try to get task information from YARN REST API
            # Note: In practice, you might need to parse JobHistory logs
            # This is a simplified version that tries to extract from yarn logs
            
            cmd = f"yarn logs -applicationId {application_id} 2>/dev/null | grep -i 'reduce.*complete\\|reduce.*finished' | head -20"
            stdout, stderr, code = self.run_command(cmd)
            
            # For now, return empty list - actual implementation would parse logs
            # The user will analyze logs manually as requested
            return []
            
        except Exception as e:
            print(f"    Warning: Could not retrieve reduce task times: {e}")
            return []
    
    def run_single_job(self, data_type, slowstart, run_number, hdfs_input_dir):
        """Run a single MapReduce job with specified parameters."""
        output_dir = f"{HDFS_BASE_DIR}/output_{data_type}_s{int(slowstart*100):03d}_run{run_number}"
        
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
        
        # Extract job ID and application ID from output
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
            print(f"    Error: {stderr[:500]}")
            return None
        
        print(f"    ✓ Job completed successfully")
        if job_id:
            print(f"    Job ID: {job_id}")
        if application_id:
            print(f"    Application ID: {application_id}")
        
        # Only record basic info, detailed timing will be extracted later
        metrics = {
            'data_type': data_type,
            'slowstart': slowstart,
            'run_number': run_number,
            'job_id': job_id or 'unknown',
            'application_id': application_id or 'unknown',
            'submit_time': submit_time,
            'num_reducers': NUM_REDUCERS,
        }
        
        return metrics
    
    def run_experiments_for_data_type(self, data_type, local_file):
        """Run all experiments for a specific data type."""
        print(f"\n{'='*80}")
        print(f"Testing with data type: {data_type.upper()}")
        print(f"{'='*80}")
        
        # Upload data to HDFS
        hdfs_input_dir = self.upload_data_to_hdfs(data_type, local_file)
        
        # Test each slowstart value
        for slowstart in SLOWSTART_VALUES:
            print(f"\n  {'─'*76}")
            print(f"  Testing slowstart = {slowstart}")
            print(f"  {'─'*76}")
            
            for run in range(1, RUNS_PER_CONFIG + 1):
                metrics = self.run_single_job(
                    data_type, slowstart, run, hdfs_input_dir
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
        """Run all experiments for all data types."""
        print("\n" + "="*80)
        print("Task 4: Data Skew Testing")
        print("="*80)
        print(f"Configuration:")
        print(f"  - Data Types: Skewed (60% hotkey), Uniform (control)")
        print(f"  - Data Size: 1GB each")
        print(f"  - Slowstart Values: {SLOWSTART_VALUES}")
        print(f"  - Runs per Configuration: {RUNS_PER_CONFIG}")
        print(f"  - Number of Reducers: {NUM_REDUCERS}")
        print("="*80)
        
        total_experiments = len(DATA_TYPES) * len(SLOWSTART_VALUES) * RUNS_PER_CONFIG
        print(f"\nTotal experiments to run: {total_experiments}")
        print(f"Estimated time: {total_experiments * 2}-{total_experiments * 5} minutes")
        print()
        
        # Run experiments for each data type
        for idx, (data_type, filename) in enumerate(DATA_TYPES, 1):
            local_file = os.path.join(LOCAL_DATA_DIR, filename)
            
            # Check if file exists
            if not os.path.exists(local_file):
                print(f"\n✗ Error: Data file not found: {local_file}")
                print("  Please run: python3 scripts/generate_data.py")
                continue
            
            file_size_gb = os.path.getsize(local_file) / (1024 * 1024 * 1024)
            print(f"\n[Data Type {idx}/{len(DATA_TYPES)}]")
            print(f"File: {filename} ({file_size_gb:.2f}GB)")
            
            self.run_experiments_for_data_type(data_type, local_file)
    
    def save_results(self):
        """Save experimental results to JSON file with timestamp."""
        results_dir = '/root/Exp-hadoop/EXP/task4/results'
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
                    'data_types': [dt[0] for dt in DATA_TYPES],
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
    print("Task 4: Data Skew Testing - Experiment Runner")
    print("="*80)
    
    # Check if JAR file exists
    if not os.path.exists(WORDCOUNT_JAR):
        print(f"\n✗ Error: WordCount JAR not found at {WORDCOUNT_JAR}")
        print("  Please compile WordCount.java first:")
        print("  cd /root/Exp-hadoop/EXP/task4 && ./compile.sh")
        sys.exit(1)
    
    # Check if data files exist
    missing_files = []
    for data_type, filename in DATA_TYPES:
        local_file = os.path.join(LOCAL_DATA_DIR, filename)
        if not os.path.exists(local_file):
            missing_files.append(filename)
    
    if missing_files:
        print(f"\n✗ Error: Required data files are missing:")
        for filename in missing_files:
            print(f"  - {filename}")
        print("\n  Please generate data first:")
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
        print("\nFor Task 4 analysis, detailed Reduce task times can be extracted from JobHistory")
        print("  Access: http://47.116.112.198:19888")
        
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

