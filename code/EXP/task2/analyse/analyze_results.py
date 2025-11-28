#!/usr/bin/env python3
"""
Task 2: Data Scalability Analysis
Analyze the impact of data size on optimal slowstart value
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
RESULTS_FILE = '../results/raw_results_20251127_032327_enhanced.json'
OUTPUT_DIR = Path('.')

def load_data(filename):
    """Load experimental results from JSON file."""
    with open(filename, 'r') as f:
        data = json.load(f)
    return pd.DataFrame(data['results']), data['configuration']

def compute_statistics(df):
    """Compute average and std for each configuration."""
    stats = df.groupby(['data_size', 'slowstart']).agg({
        'total_time_from_api': ['mean', 'std', 'count'],
        'map_completion_time': ['mean', 'std'],
        'first_reduce_start_time': ['mean', 'std'],
        'reduce_completion_time': ['mean', 'std'],
        'cpu_time': ['mean', 'std'],
        'physical_memory_bytes': ['mean', 'std'],
        'hdfs_bytes_read': ['mean'],
        'total_map_time': ['mean'],
        'total_reduce_time': ['mean'],
        'avg_shuffle_time': ['mean', 'std'],
        'reduce_elapsed_stddev': ['mean']
    }).reset_index()
    
    # Flatten column names
    stats.columns = ['_'.join(col).strip('_') if col[1] else col[0] 
                     for col in stats.columns.values]
    
    return stats

def create_summary_table(stats):
    """Create summary table (Table 3 from requirement)."""
    # Select key columns for the summary table
    summary = stats[['data_size', 'slowstart', 'total_time_from_api_mean', 
                     'total_time_from_api_std', 'total_time_from_api_count']].copy()
    
    summary.columns = ['Data Size', 'Slowstart', 'Avg Total Time (s)', 
                       'Std Dev (s)', 'Num Runs']
    
    # Round numeric values
    summary['Avg Total Time (s)'] = summary['Avg Total Time (s)'].round(2)
    summary['Std Dev (s)'] = summary['Std Dev (s)'].round(2)
    
    return summary

def create_detailed_table(stats):
    """Create detailed table with timing breakdown."""
    detailed = stats[['data_size', 'slowstart', 
                      'total_time_from_api_mean', 
                      'map_completion_time_mean',
                      'first_reduce_start_time_mean',
                      'reduce_completion_time_mean',
                      'avg_shuffle_time_mean',
                      'cpu_time_mean',
                      'physical_memory_bytes_mean']].copy()
    
    detailed.columns = ['Data Size', 'Slowstart', 
                        'Avg Total Time (s)',
                        'Avg Map Completion (s)',
                        'Avg Reduce Start (s)', 
                        'Avg Reduce Complete (s)',
                        'Avg Shuffle Time (s)',
                        'Avg CPU Time (s)',
                        'Avg Memory (MB)']
    
    # Convert memory to MB
    detailed['Avg Memory (MB)'] = (detailed['Avg Memory (MB)'] / (1024**2)).round(2)
    
    # Round all numeric columns
    numeric_cols = detailed.select_dtypes(include=[np.number]).columns
    detailed[numeric_cols] = detailed[numeric_cols].round(2)
    
    return detailed

def plot_scalability_line(stats, output_file='fig1_scalability_lines.png'):
    """
    Figure 1: Line plot showing total time vs slowstart for different data sizes
    """
    plt.figure(figsize=(10, 6))
    
    data_sizes = stats['data_size'].unique()
    
    for data_size in data_sizes:
        subset = stats[stats['data_size'] == data_size]
        plt.plot(subset['slowstart'], subset['total_time_from_api_mean'], 
                marker='o', linewidth=2, markersize=8, label=data_size)
        
        # Add error bars
        plt.errorbar(subset['slowstart'], subset['total_time_from_api_mean'],
                    yerr=subset['total_time_from_api_std'], 
                    fmt='none', alpha=0.3, capsize=3)
    
    plt.xlabel('Slowstart Value', fontsize=12)
    plt.ylabel('Average Total Time (s)', fontsize=12)
    plt.title('Task 2: Data Scalability - Impact of Slowstart on Performance', 
              fontsize=14, fontweight='bold')
    plt.legend(title='Data Size', fontsize=10)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file}")
    plt.close()

def plot_grouped_bars(stats, output_file='fig2_grouped_bars.png'):
    """
    Figure 2: Grouped bar chart comparing slowstart values across data sizes
    """
    fig, ax = plt.subplots(figsize=(12, 6))
    
    data_sizes = stats['data_size'].unique()
    slowstart_values = stats['slowstart'].unique()
    
    x = np.arange(len(data_sizes))
    width = 0.08  # Width of bars (9 slowstart values)
    
    for i, slowstart in enumerate(slowstart_values):
        subset = stats[stats['slowstart'] == slowstart]
        offset = (i - len(slowstart_values)/2) * width
        values = [subset[subset['data_size'] == ds]['total_time_from_api_mean'].values[0] 
                 if len(subset[subset['data_size'] == ds]) > 0 else 0 
                 for ds in data_sizes]
        ax.bar(x + offset, values, width, label=f'{slowstart:.2f}')
    
    ax.set_xlabel('Data Size', fontsize=12)
    ax.set_ylabel('Average Total Time (s)', fontsize=12)
    ax.set_title('Task 2: Performance Comparison Across Data Sizes', 
                 fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(data_sizes)
    ax.legend(title='Slowstart', ncol=3, fontsize=8)
    ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file}")
    plt.close()

def plot_optimal_slowstart(stats, output_file='fig3_optimal_slowstart.png'):
    """
    Figure 3: Identify and visualize optimal slowstart for each data size
    """
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    
    # Find optimal slowstart for each data size
    optimal = []
    data_sizes = stats['data_size'].unique()
    
    for data_size in data_sizes:
        subset = stats[stats['data_size'] == data_size]
        min_idx = subset['total_time_from_api_mean'].idxmin()
        optimal_row = subset.loc[min_idx]
        optimal.append({
            'data_size': data_size,
            'optimal_slowstart': optimal_row['slowstart'],
            'min_time': optimal_row['total_time_from_api_mean']
        })
    
    optimal_df = pd.DataFrame(optimal)
    
    # Plot 1: Optimal slowstart value vs data size
    ax1.plot(optimal_df['data_size'], optimal_df['optimal_slowstart'], 
            marker='o', linewidth=2, markersize=10, color='darkgreen')
    ax1.set_xlabel('Data Size', fontsize=12)
    ax1.set_ylabel('Optimal Slowstart Value', fontsize=12)
    ax1.set_title('Optimal Slowstart vs Data Size', fontsize=12, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    ax1.set_ylim([0, 1.05])
    
    # Add value labels
    for _, row in optimal_df.iterrows():
        ax1.annotate(f"{row['optimal_slowstart']:.2f}", 
                    (row['data_size'], row['optimal_slowstart']),
                    textcoords="offset points", xytext=(0,10), 
                    ha='center', fontsize=10, fontweight='bold')
    
    # Plot 2: Minimum time vs data size
    ax2.plot(optimal_df['data_size'], optimal_df['min_time'], 
            marker='s', linewidth=2, markersize=10, color='darkblue')
    ax2.set_xlabel('Data Size', fontsize=12)
    ax2.set_ylabel('Minimum Total Time (s)', fontsize=12)
    ax2.set_title('Best Performance vs Data Size', fontsize=12, fontweight='bold')
    ax2.grid(True, alpha=0.3)
    
    # Add value labels
    for _, row in optimal_df.iterrows():
        ax2.annotate(f"{row['min_time']:.1f}s", 
                    (row['data_size'], row['min_time']),
                    textcoords="offset points", xytext=(0,10), 
                    ha='center', fontsize=10)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file}")
    plt.close()
    
    return optimal_df

def plot_performance_heatmap(stats, output_file='fig4_heatmap.png'):
    """
    Figure 4: Heatmap showing performance across all configurations
    """
    # Pivot data for heatmap
    pivot_data = stats.pivot(index='slowstart', 
                             columns='data_size', 
                             values='total_time_from_api_mean')
    
    plt.figure(figsize=(10, 8))
    sns.heatmap(pivot_data, annot=True, fmt='.1f', cmap='RdYlGn_r', 
                cbar_kws={'label': 'Average Total Time (s)'})
    plt.xlabel('Data Size', fontsize=12)
    plt.ylabel('Slowstart Value', fontsize=12)
    plt.title('Task 2: Performance Heatmap (Lower is Better)', 
              fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file}")
    plt.close()

def plot_timing_breakdown(stats, output_file='fig5_timing_breakdown.png'):
    """
    Figure 5: Stacked bar chart showing timing breakdown
    """
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    
    data_sizes = ['500MB', '1GB', '1500MB']
    
    for idx, data_size in enumerate(data_sizes):
        subset = stats[stats['data_size'] == data_size]
        
        map_time = subset['map_completion_time_mean']
        reduce_time = subset['reduce_completion_time_mean'] - subset['map_completion_time_mean']
        
        x = np.arange(len(subset))
        
        axes[idx].bar(x, map_time, label='Map Phase', alpha=0.8)
        axes[idx].bar(x, reduce_time, bottom=map_time, label='Reduce Phase', alpha=0.8)
        
        axes[idx].set_xlabel('Slowstart Value', fontsize=10)
        axes[idx].set_ylabel('Time (s)', fontsize=10)
        axes[idx].set_title(f'{data_size}', fontsize=12, fontweight='bold')
        axes[idx].set_xticks(x)
        axes[idx].set_xticklabels([f'{s:.2f}' for s in subset['slowstart']], 
                                  rotation=45, ha='right', fontsize=8)
        axes[idx].legend(fontsize=9)
        axes[idx].grid(True, alpha=0.3, axis='y')
    
    fig.suptitle('Task 2: Map/Reduce Timing Breakdown by Data Size', 
                 fontsize=14, fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file}")
    plt.close()

def plot_resource_usage(stats, output_file='fig6_resource_usage.png'):
    """
    Figure 6: Resource usage (CPU and Memory) across configurations
    """
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    
    data_sizes = stats['data_size'].unique()
    
    # Plot 1: CPU Time
    for data_size in data_sizes:
        subset = stats[stats['data_size'] == data_size]
        ax1.plot(subset['slowstart'], subset['cpu_time_mean'], 
                marker='o', linewidth=2, markersize=8, label=data_size)
    
    ax1.set_xlabel('Slowstart Value', fontsize=12)
    ax1.set_ylabel('Average CPU Time (s)', fontsize=12)
    ax1.set_title('CPU Usage vs Slowstart', fontsize=12, fontweight='bold')
    ax1.legend(title='Data Size')
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: Memory Usage
    for data_size in data_sizes:
        subset = stats[stats['data_size'] == data_size]
        memory_mb = subset['physical_memory_bytes_mean'] / (1024**2)
        ax2.plot(subset['slowstart'], memory_mb, 
                marker='s', linewidth=2, markersize=8, label=data_size)
    
    ax2.set_xlabel('Slowstart Value', fontsize=12)
    ax2.set_ylabel('Average Physical Memory (MB)', fontsize=12)
    ax2.set_title('Memory Usage vs Slowstart', fontsize=12, fontweight='bold')
    ax2.legend(title='Data Size')
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file}")
    plt.close()

def plot_scalability_factor(stats, output_file='fig7_scalability_factor.png'):
    """
    Figure 7: Scalability analysis - time increase vs data increase
    """
    # Calculate scalability metrics
    data_sizes_sorted = ['500MB', '1GB', '1500MB']
    data_sizes_gb = [0.5, 1.0, 1.5]
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    for slowstart in [0.05, 0.30, 0.50, 0.80, 1.00]:
        times = []
        for ds in data_sizes_sorted:
            subset = stats[(stats['data_size'] == ds) & (stats['slowstart'] == slowstart)]
            if len(subset) > 0:
                times.append(subset['total_time_from_api_mean'].values[0])
            else:
                times.append(None)
        
        ax.plot(data_sizes_gb, times, marker='o', linewidth=2, 
               markersize=8, label=f'slowstart={slowstart:.2f}')
    
    # Add ideal linear scaling line (from 500MB as baseline)
    baseline_idx = 0
    for slowstart in [0.50]:  # Show ideal for one config
        subset = stats[(stats['data_size'] == data_sizes_sorted[baseline_idx]) & 
                      (stats['slowstart'] == slowstart)]
        if len(subset) > 0:
            baseline_time = subset['total_time_from_api_mean'].values[0]
            ideal_times = [baseline_time * (size / data_sizes_gb[baseline_idx]) 
                          for size in data_sizes_gb]
            ax.plot(data_sizes_gb, ideal_times, 'k--', linewidth=1.5, 
                   alpha=0.5, label='Ideal Linear Scaling')
    
    ax.set_xlabel('Data Size (GB)', fontsize=12)
    ax.set_ylabel('Total Time (s)', fontsize=12)
    ax.set_title('Task 2: Scalability Analysis', fontsize=14, fontweight='bold')
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file}")
    plt.close()

def plot_reduce_start_analysis(stats, output_file='fig8_reduce_start.png'):
    """
    Figure 8: Analysis of reduce start time relative to map completion
    """
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    data_sizes = stats['data_size'].unique()
    
    # Plot 1: First reduce start time
    for data_size in data_sizes:
        subset = stats[stats['data_size'] == data_size]
        ax = axes[0]
        ax.plot(subset['slowstart'], subset['first_reduce_start_time_mean'], 
               marker='o', linewidth=2, markersize=8, label=data_size)
    
    axes[0].set_xlabel('Slowstart Value', fontsize=12)
    axes[0].set_ylabel('First Reduce Start Time (s)', fontsize=12)
    axes[0].set_title('Reduce Start Time vs Slowstart', fontsize=12, fontweight='bold')
    axes[0].legend(title='Data Size')
    axes[0].grid(True, alpha=0.3)
    
    # Plot 2: Reduce start time as % of map completion
    for data_size in data_sizes:
        subset = stats[stats['data_size'] == data_size]
        start_ratio = (subset['first_reduce_start_time_mean'] / 
                      subset['map_completion_time_mean'] * 100)
        ax = axes[1]
        ax.plot(subset['slowstart'], start_ratio, 
               marker='s', linewidth=2, markersize=8, label=data_size)
    
    # Add reference line for expected slowstart
    ax = axes[1]
    x_range = np.linspace(0, 1, 100)
    ax.plot(x_range, x_range * 100, 'k--', alpha=0.5, linewidth=1.5,
           label='Expected (ideal)')
    
    axes[1].set_xlabel('Slowstart Value', fontsize=12)
    axes[1].set_ylabel('Reduce Start / Map Complete (%)', fontsize=12)
    axes[1].set_title('Reduce Start Timing Ratio', fontsize=12, fontweight='bold')
    axes[1].legend(fontsize=9)
    axes[1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file}")
    plt.close()

def analyze_performance_variance(stats, output_file='fig9_variance_analysis.png'):
    """
    Figure 9: Analyze performance variance across runs
    """
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    
    data_sizes = stats['data_size'].unique()
    
    # Plot 1: Standard deviation of total time
    for data_size in data_sizes:
        subset = stats[stats['data_size'] == data_size]
        ax1.plot(subset['slowstart'], subset['total_time_from_api_std'], 
                marker='o', linewidth=2, markersize=8, label=data_size)
    
    ax1.set_xlabel('Slowstart Value', fontsize=12)
    ax1.set_ylabel('Standard Deviation (s)', fontsize=12)
    ax1.set_title('Performance Variance Across Runs', fontsize=12, fontweight='bold')
    ax1.legend(title='Data Size')
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: Coefficient of variation (CV)
    for data_size in data_sizes:
        subset = stats[stats['data_size'] == data_size]
        cv = (subset['total_time_from_api_std'] / 
              subset['total_time_from_api_mean'] * 100)
        ax2.plot(subset['slowstart'], cv, 
                marker='s', linewidth=2, markersize=8, label=data_size)
    
    ax2.set_xlabel('Slowstart Value', fontsize=12)
    ax2.set_ylabel('Coefficient of Variation (%)', fontsize=12)
    ax2.set_title('Relative Performance Stability', fontsize=12, fontweight='bold')
    ax2.legend(title='Data Size')
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file}")
    plt.close()

def main():
    print("="*80)
    print("Task 2: Data Scalability Analysis")
    print("="*80)
    
    # Load data
    print("\n[1/4] Loading experimental data...")
    df, config = load_data(RESULTS_FILE)
    print(f"  ✓ Loaded {len(df)} experimental runs")
    print(f"  ✓ Data sizes: {config['data_sizes']}")
    print(f"  ✓ Slowstart values: {config['slowstart_values']}")
    
    # Compute statistics
    print("\n[2/4] Computing statistics...")
    stats = compute_statistics(df)
    print(f"  ✓ Computed statistics for {len(stats)} configurations")
    
    # Create tables
    print("\n[3/4] Generating summary tables...")
    summary_table = create_summary_table(stats)
    summary_table.to_csv('table1_summary.csv', index=False)
    print(f"  ✓ Saved: table1_summary.csv")
    
    detailed_table = create_detailed_table(stats)
    detailed_table.to_csv('table2_detailed.csv', index=False)
    print(f"  ✓ Saved: table2_detailed.csv")
    
    # Generate all figures
    print("\n[4/4] Generating figures...")
    plot_scalability_line(stats)
    plot_grouped_bars(stats)
    optimal_df = plot_optimal_slowstart(stats)
    plot_performance_heatmap(stats)
    plot_timing_breakdown(stats)
    plot_resource_usage(stats)
    plot_scalability_factor(stats)
    plot_reduce_start_analysis(stats)
    analyze_performance_variance(stats)
    
    # Print key findings
    print("\n" + "="*80)
    print("Key Findings:")
    print("="*80)
    print("\nOptimal Slowstart Values:")
    for _, row in optimal_df.iterrows():
        print(f"  {row['data_size']:>7s}: slowstart = {row['optimal_slowstart']:.2f}, "
              f"time = {row['min_time']:.2f}s")
    
    # Check if optimal slowstart drifts
    optimal_values = optimal_df['optimal_slowstart'].values
    if len(set(optimal_values)) == 1:
        print(f"\n✓ Optimal slowstart is CONSISTENT across data sizes: {optimal_values[0]:.2f}")
    else:
        print(f"\n⚠ Optimal slowstart DRIFTS across data sizes")
        print(f"  Range: {optimal_values.min():.2f} - {optimal_values.max():.2f}")
    
    print("\n" + "="*80)
    print("✓ Analysis complete! All figures and tables saved.")
    print("="*80)

if __name__ == '__main__':
    main()

