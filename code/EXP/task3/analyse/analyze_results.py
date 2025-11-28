#!/usr/bin/env python3
"""
Task 3: Workload Type Comparison Analysis
Compare IO-intensive (TeraSort) vs CPU-intensive (WordCount) jobs
"""

import json
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import seaborn as sns

# Set style
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

# Configuration
RESULTS_FILE = '../results/raw_results_20251127_043910_enhanced.json'
OUTPUT_DIR = Path('.')

def load_data(filename):
    """Load experimental results from JSON file."""
    with open(filename, 'r') as f:
        data = json.load(f)
    return pd.DataFrame(data['results']), data['configuration']

def compute_statistics(df):
    """Compute average and std for each job_type + slowstart configuration."""
    stats = df.groupby(['job_type', 'slowstart']).agg({
        'total_time_from_api': ['mean', 'std', 'count'],
        'map_completion_time': ['mean'],
        'first_reduce_start_time': ['mean'],
        'reduce_completion_time': ['mean'],
        'avg_shuffle_time': ['mean', 'std'],
        'avg_reduce_time': ['mean'],
        'cpu_time': ['mean'],
        'reduce_shuffle_bytes': ['mean'],
        'hdfs_bytes_read': ['mean'],
        'hdfs_bytes_written': ['mean']
    }).reset_index()
    
    # Flatten column names
    stats.columns = ['_'.join(col).strip('_') if col[1] else col[0] 
                     for col in stats.columns.values]
    
    return stats

def plot_workload_comparison(stats, output_file='fig1_workload_comparison.png'):
    """
    Figure 1: Main comparison - Total time for both workload types
    """
    fig, ax = plt.subplots(figsize=(12, 6))
    
    job_types = stats['job_type'].unique()
    
    for job_type in job_types:
        subset = stats[stats['job_type'] == job_type]
        label = 'TeraSort (IO-intensive)' if job_type == 'TeraSort' else 'WordCount (CPU-intensive)'
        ax.plot(subset['slowstart'], subset['total_time_from_api_mean'], 
               marker='o', linewidth=2.5, markersize=10, label=label)
        
        # Add error bars
        ax.errorbar(subset['slowstart'], subset['total_time_from_api_mean'],
                   yerr=subset['total_time_from_api_std'], 
                   fmt='none', alpha=0.3, capsize=5)
    
    ax.set_xlabel('Slowstart Value', fontsize=14, fontweight='bold')
    ax.set_ylabel('Average Total Time (s)', fontsize=14, fontweight='bold')
    ax.set_title('Task 3: Workload Type Comparison - TeraSort vs WordCount', 
                fontsize=15, fontweight='bold')
    ax.legend(fontsize=12, loc='best')
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file}")
    plt.close()

def plot_shuffle_comparison(stats, output_file='fig2_shuffle_comparison.png'):
    """
    Figure 2: Shuffle phase comparison
    """
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    job_types = stats['job_type'].unique()
    
    # Left: Shuffle time
    for job_type in job_types:
        subset = stats[stats['job_type'] == job_type]
        label = 'TeraSort' if job_type == 'TeraSort' else 'WordCount'
        ax1.plot(subset['slowstart'], subset['avg_shuffle_time_mean'], 
                marker='s', linewidth=2, markersize=8, label=label)
    
    ax1.set_xlabel('Slowstart Value', fontsize=12)
    ax1.set_ylabel('Average Shuffle Time (s)', fontsize=12)
    ax1.set_title('Shuffle Phase Duration', fontsize=13, fontweight='bold')
    ax1.legend(fontsize=10)
    ax1.grid(True, alpha=0.3)
    
    # Right: Shuffle bytes
    for job_type in job_types:
        subset = stats[stats['job_type'] == job_type]
        shuffle_gb = subset['reduce_shuffle_bytes_mean'] / (1024**3)
        label = 'TeraSort' if job_type == 'TeraSort' else 'WordCount'
        ax2.plot(subset['slowstart'], shuffle_gb, 
                marker='o', linewidth=2, markersize=8, label=label)
    
    ax2.set_xlabel('Slowstart Value', fontsize=12)
    ax2.set_ylabel('Shuffle Data Volume (GB)', fontsize=12)
    ax2.set_title('Shuffle Data Volume', fontsize=13, fontweight='bold')
    ax2.legend(fontsize=10)
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file}")
    plt.close()

def plot_sensitivity_comparison(stats, output_file='fig3_sensitivity_comparison.png'):
    """
    Figure 3: Sensitivity to slowstart parameter
    """
    fig, ax = plt.subplots(figsize=(10, 6))
    
    job_types = stats['job_type'].unique()
    
    for job_type in job_types:
        subset = stats[stats['job_type'] == job_type]
        # Normalize to show relative sensitivity
        values = subset['total_time_from_api_mean']
        normalized = (values - values.min()) / (values.max() - values.min()) * 100
        label = 'TeraSort' if job_type == 'TeraSort' else 'WordCount'
        ax.plot(subset['slowstart'], normalized, 
               marker='o', linewidth=2.5, markersize=10, label=label)
    
    ax.set_xlabel('Slowstart Value', fontsize=14, fontweight='bold')
    ax.set_ylabel('Relative Performance Impact (%)', fontsize=12)
    ax.set_title('Task 3: Slowstart Sensitivity Comparison', fontsize=15, fontweight='bold')
    ax.legend(fontsize=12)
    ax.grid(True, alpha=0.3)
    ax.set_ylim([-5, 105])
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file}")
    plt.close()

def plot_io_vs_cpu(stats, output_file='fig4_io_vs_cpu.png'):
    """
    Figure 4: IO vs CPU characteristics
    """
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    job_types = stats['job_type'].unique()
    
    # Left: CPU time
    for job_type in job_types:
        subset = stats[stats['job_type'] == job_type]
        label = 'TeraSort' if job_type == 'TeraSort' else 'WordCount'
        ax1.plot(subset['slowstart'], subset['cpu_time_mean'], 
                marker='o', linewidth=2, markersize=8, label=label)
    
    ax1.set_xlabel('Slowstart Value', fontsize=12)
    ax1.set_ylabel('CPU Time (s)', fontsize=12)
    ax1.set_title('CPU Usage (CPU-intensive indicator)', fontsize=13, fontweight='bold')
    ax1.legend(fontsize=10)
    ax1.grid(True, alpha=0.3)
    
    # Right: IO ratio (Shuffle bytes / CPU time)
    for job_type in job_types:
        subset = stats[stats['job_type'] == job_type]
        io_ratio = (subset['reduce_shuffle_bytes_mean'] / (1024**2)) / subset['cpu_time_mean']
        label = 'TeraSort' if job_type == 'TeraSort' else 'WordCount'
        ax2.plot(subset['slowstart'], io_ratio, 
                marker='s', linewidth=2, markersize=8, label=label)
    
    ax2.set_xlabel('Slowstart Value', fontsize=12)
    ax2.set_ylabel('IO Ratio (MB shuffled per CPU second)', fontsize=12)
    ax2.set_title('IO Intensity (IO-intensive indicator)', fontsize=13, fontweight='bold')
    ax2.legend(fontsize=10)
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file}")
    plt.close()

def plot_optimal_comparison(stats, output_file='fig5_optimal_comparison.png'):
    """
    Figure 5: Optimal slowstart for each workload type
    """
    fig, ax = plt.subplots(figsize=(10, 6))
    
    job_types = ['TeraSort', 'WordCount']
    optimal_data = []
    
    for job_type in job_types:
        subset = stats[stats['job_type'] == job_type]
        min_idx = subset['total_time_from_api_mean'].idxmin()
        optimal_slowstart = subset.loc[min_idx, 'slowstart']
        optimal_time = subset.loc[min_idx, 'total_time_from_api_mean']
        optimal_data.append({
            'job_type': job_type,
            'optimal_slowstart': optimal_slowstart,
            'optimal_time': optimal_time
        })
    
    df_optimal = pd.DataFrame(optimal_data)
    
    x = np.arange(len(df_optimal))
    bars = ax.bar(x, df_optimal['optimal_slowstart'], color=['#ff7f0e', '#1f77b4'], 
                   alpha=0.8, edgecolor='black', linewidth=1.5)
    
    ax.set_xlabel('Job Type', fontsize=14, fontweight='bold')
    ax.set_ylabel('Optimal Slowstart Value', fontsize=12)
    ax.set_title('Task 3: Optimal Slowstart by Workload Type', fontsize=15, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(df_optimal['job_type'], fontsize=12)
    ax.set_ylim([0, 1.1])
    ax.grid(True, alpha=0.3, axis='y')
    
    # Add value labels
    for i, (bar, row) in enumerate(zip(bars, df_optimal.itertuples())):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height + 0.02,
                f'{row.optimal_slowstart:.2f}\n({row.optimal_time:.1f}s)',
                ha='center', va='bottom', fontsize=11, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file}")
    plt.close()
    
    return df_optimal

def create_summary_tables(stats):
    """Create summary tables."""
    # Table 4: TeraSort
    terasort = stats[stats['job_type'] == 'TeraSort'][['slowstart', 'total_time_from_api_mean',
                                                         'map_completion_time_mean',
                                                         'first_reduce_start_time_mean',
                                                         'avg_shuffle_time_mean',
                                                         'reduce_completion_time_mean']].copy()
    terasort.columns = ['Slowstart', 'Avg Total Time (s)', 'Avg Map Complete (s)',
                        'Avg Reduce Start (s)', 'Avg Shuffle Time (s)', 'Avg Reduce Complete (s)']
    terasort = terasort.round(2)
    
    # Table 5: WordCount
    wordcount = stats[stats['job_type'] == 'WordCount'][['slowstart', 'total_time_from_api_mean',
                                                           'map_completion_time_mean',
                                                           'first_reduce_start_time_mean',
                                                           'avg_shuffle_time_mean',
                                                           'reduce_completion_time_mean']].copy()
    wordcount.columns = ['Slowstart', 'Avg Total Time (s)', 'Avg Map Complete (s)',
                          'Avg Reduce Start (s)', 'Avg Shuffle Time (s)', 'Avg Reduce Complete (s)']
    wordcount = wordcount.round(2)
    
    return terasort, wordcount

def main():
    print("="*80)
    print("Task 3: Workload Type Comparison Analysis")
    print("="*80)
    
    # Load data
    print("\n[1/4] Loading experimental data...")
    df, config = load_data(RESULTS_FILE)
    print(f"  ✓ Loaded {len(df)} experimental runs")
    print(f"  ✓ Job types: {df['job_type'].unique().tolist()}")
    print(f"  ✓ Slowstart values: {sorted(df['slowstart'].unique())}")
    
    # Compute statistics
    print("\n[2/4] Computing statistics...")
    stats = compute_statistics(df)
    print(f"  ✓ Computed statistics for {len(stats)} configurations")
    
    # Create tables
    print("\n[3/4] Generating summary tables...")
    terasort_table, wordcount_table = create_summary_tables(stats)
    terasort_table.to_csv('table4_terasort.csv', index=False)
    wordcount_table.to_csv('table5_wordcount.csv', index=False)
    print(f"  ✓ Saved: table4_terasort.csv")
    print(f"  ✓ Saved: table5_wordcount.csv")
    
    # Generate figures
    print("\n[4/4] Generating figures...")
    plot_workload_comparison(stats)
    plot_shuffle_comparison(stats)
    plot_sensitivity_comparison(stats)
    plot_io_vs_cpu(stats)
    optimal_df = plot_optimal_comparison(stats)
    
    # Print key findings
    print("\n" + "="*80)
    print("Key Findings:")
    print("="*80)
    
    print("\nOptimal Slowstart by Workload Type:")
    for _, row in optimal_df.iterrows():
        print(f"  {row['job_type']:12s}: slowstart = {row['optimal_slowstart']:.2f}, "
              f"time = {row['optimal_time']:.2f}s")
    
    # Compare sensitivity
    for job_type in ['TeraSort', 'WordCount']:
        subset = stats[stats['job_type'] == job_type]
        min_time = subset['total_time_from_api_mean'].min()
        max_time = subset['total_time_from_api_mean'].max()
        sensitivity = (max_time - min_time) / min_time * 100
        print(f"\n{job_type} Sensitivity to Slowstart: {sensitivity:.1f}%")
        print(f"  Best: {min_time:.2f}s, Worst: {max_time:.2f}s")
    
    print("\n" + "="*80)
    print("✓ Analysis complete! All figures and tables saved.")
    print("="*80)

if __name__ == '__main__':
    main()

