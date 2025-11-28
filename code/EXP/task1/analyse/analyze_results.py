#!/usr/bin/env python3
"""
Task 1: Slowstart Parameter Sensitivity Analysis
Analyze the impact of slowstart parameter on fixed-size workload (1GB)
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
RESULTS_FILE = '../results/raw_results_20251127_010317_enhanced.json'
OUTPUT_DIR = Path('.')

def load_data(filename):
    """Load experimental results from JSON file."""
    with open(filename, 'r') as f:
        data = json.load(f)
    return pd.DataFrame(data)

def compute_statistics(df):
    """Compute average and std for each slowstart configuration."""
    stats = df.groupby('slowstart').agg({
        'total_time_from_api': ['mean', 'std', 'count'],
        'map_completion_time': ['mean', 'std'],
        'first_reduce_start_time': ['mean', 'std'],
        'reduce_completion_time': ['mean', 'std'],
        'avg_shuffle_time': ['mean', 'std'],
        'avg_merge_time': ['mean'],
        'avg_reduce_time': ['mean'],
        'cpu_time': ['mean', 'std'],
        'physical_memory_bytes': ['mean'],
        'reduce_elapsed_stddev': ['mean'],
        'hdfs_bytes_read': ['mean']
    }).reset_index()
    
    # Flatten column names
    stats.columns = ['_'.join(col).strip('_') if col[1] else col[0] 
                     for col in stats.columns.values]
    
    return stats

def create_summary_tables(stats):
    """Create summary tables (Table 1 and 2 from requirements)."""
    # Table 1: Single run records (from raw data)
    # Table 2: Summary with averages
    table2 = stats[['slowstart', 'total_time_from_api_mean', 
                    'map_completion_time_mean', 'first_reduce_start_time_mean',
                    'reduce_completion_time_mean']].copy()
    
    table2.columns = ['Slowstart', 'Avg Total Time (s)', 'Avg Map完成时间 (s)',
                      'Avg Reduce启动时间 (s)', 'Avg Reduce完成时间 (s)']
    
    table2 = table2.round(2)
    return table2

def plot_sensitivity_curve(stats, output_file='fig1_slowstart_sensitivity.png'):
    """
    Figure 1: Main sensitivity curve - Total time vs slowstart
    """
    fig, ax = plt.subplots(figsize=(10, 6))
    
    ax.plot(stats['slowstart'], stats['total_time_from_api_mean'], 
           marker='o', linewidth=2.5, markersize=10, color='darkblue', label='Total Time')
    
    # Add error bars
    ax.errorbar(stats['slowstart'], stats['total_time_from_api_mean'],
               yerr=stats['total_time_from_api_std'], 
               fmt='none', alpha=0.3, capsize=5, color='darkblue')
    
    # Highlight the optimal point
    min_idx = stats['total_time_from_api_mean'].idxmin()
    optimal_slowstart = stats.loc[min_idx, 'slowstart']
    optimal_time = stats.loc[min_idx, 'total_time_from_api_mean']
    
    ax.plot(optimal_slowstart, optimal_time, marker='*', markersize=20, 
           color='red', label=f'Optimal (slowstart={optimal_slowstart:.2f})')
    
    ax.set_xlabel('Slowstart Value', fontsize=14, fontweight='bold')
    ax.set_ylabel('Average Total Time (s)', fontsize=14, fontweight='bold')
    ax.set_title('Task 1: Slowstart Parameter Sensitivity Analysis - Fixed Data Size (1GB)', 
                fontsize=15, fontweight='bold')
    ax.legend(fontsize=12, loc='upper right')
    ax.grid(True, alpha=0.3)
    
    # Add value labels
    for _, row in stats.iterrows():
        ax.annotate(f"{row['total_time_from_api_mean']:.1f}s", 
                   (row['slowstart'], row['total_time_from_api_mean']),
                   textcoords="offset points", xytext=(0,10), 
                   ha='center', fontsize=9, alpha=0.7)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ 已保存: {output_file}")
    plt.close()

def plot_timing_phases(stats, output_file='fig2_timing_phases.png'):
    """
    Figure 2: Timing breakdown - Map/Shuffle/Reduce phases
    """
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    # Left: Absolute times
    x = np.arange(len(stats))
    width = 0.25
    
    map_time = stats['map_completion_time_mean']
    shuffle_time = stats['avg_shuffle_time_mean']
    reduce_time = stats['avg_reduce_time_mean']
    
    ax1.bar(x - width, map_time, width, label='Map Phase', alpha=0.8)
    ax1.bar(x, shuffle_time, width, label='Shuffle Phase', alpha=0.8)
    ax1.bar(x + width, reduce_time, width, label='Reduce Compute', alpha=0.8)
    
    ax1.set_xlabel('Slowstart Value', fontsize=12)
    ax1.set_ylabel('Time (s)', fontsize=12)
    ax1.set_title('Phase Timing Breakdown', fontsize=13, fontweight='bold')
    ax1.set_xticks(x)
    ax1.set_xticklabels([f'{s:.2f}' for s in stats['slowstart']], rotation=45)
    ax1.legend(fontsize=10)
    ax1.grid(True, alpha=0.3, axis='y')
    
    # Right: Reduce start time analysis
    ax2.plot(stats['slowstart'], stats['first_reduce_start_time_mean'], 
            marker='s', linewidth=2, markersize=8, label='First Reduce Start Time', color='green')
    ax2.plot(stats['slowstart'], stats['map_completion_time_mean'], 
            marker='o', linewidth=2, markersize=8, label='Map Completion Time', color='orange')
    
    ax2.set_xlabel('Slowstart Value', fontsize=12)
    ax2.set_ylabel('Time (s)', fontsize=12)
    ax2.set_title('Reduce Start Timing Analysis', fontsize=13, fontweight='bold')
    ax2.legend(fontsize=10)
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ 已保存: {output_file}")
    plt.close()

def plot_performance_variance(stats, output_file='fig3_performance_variance.png'):
    """
    Figure 3: Performance variance across runs
    """
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    
    # Left: Standard deviation
    ax1.bar(range(len(stats)), stats['total_time_from_api_std'], 
           color='coral', alpha=0.7, edgecolor='black')
    ax1.set_xlabel('Slowstart Value', fontsize=12)
    ax1.set_ylabel('Standard Deviation (s)', fontsize=12)
    ax1.set_title('Performance Variance Analysis', fontsize=13, fontweight='bold')
    ax1.set_xticks(range(len(stats)))
    ax1.set_xticklabels([f'{s:.2f}' for s in stats['slowstart']], rotation=45)
    ax1.grid(True, alpha=0.3, axis='y')
    
    # Right: Coefficient of variation
    cv = (stats['total_time_from_api_std'] / stats['total_time_from_api_mean'] * 100)
    ax2.bar(range(len(stats)), cv, color='skyblue', alpha=0.7, edgecolor='black')
    ax2.set_xlabel('Slowstart Value', fontsize=12)
    ax2.set_ylabel('Coefficient of Variation (%)', fontsize=12)
    ax2.set_title('Relative Performance Stability', fontsize=13, fontweight='bold')
    ax2.set_xticks(range(len(stats)))
    ax2.set_xticklabels([f'{s:.2f}' for s in stats['slowstart']], rotation=45)
    ax2.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ 已保存: {output_file}")
    plt.close()

def plot_resource_utilization(stats, output_file='fig4_resource_utilization.png'):
    """
    Figure 4: CPU and Memory utilization
    """
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    
    # Left: CPU time
    ax1.plot(stats['slowstart'], stats['cpu_time_mean'], 
            marker='o', linewidth=2, markersize=10, color='purple')
    ax1.fill_between(stats['slowstart'], 
                     stats['cpu_time_mean'] - stats['cpu_time_std'],
                     stats['cpu_time_mean'] + stats['cpu_time_std'],
                     alpha=0.2, color='purple')
    ax1.set_xlabel('Slowstart Value', fontsize=12)
    ax1.set_ylabel('CPU Time (s)', fontsize=12)
    ax1.set_title('CPU Utilization', fontsize=13, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    
    # Right: Memory usage
    memory_mb = stats['physical_memory_bytes_mean'] / (1024**2)
    ax2.plot(stats['slowstart'], memory_mb, 
            marker='s', linewidth=2, markersize=10, color='brown')
    ax2.set_xlabel('Slowstart Value', fontsize=12)
    ax2.set_ylabel('Physical Memory (MB)', fontsize=12)
    ax2.set_title('Memory Usage', fontsize=13, fontweight='bold')
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ 已保存: {output_file}")
    plt.close()

def plot_reduce_start_ratio(stats, output_file='fig5_reduce_start_ratio.png'):
    """
    Figure 5: Reduce start time as percentage of Map completion
    """
    fig, ax = plt.subplots(figsize=(10, 6))
    
    start_ratio = (stats['first_reduce_start_time_mean'] / 
                  stats['map_completion_time_mean'] * 100)
    
    ax.plot(stats['slowstart'], start_ratio, 
           marker='o', linewidth=2.5, markersize=10, color='teal', label='Actual Start Ratio')
    
    # Expected line
    expected_ratio = stats['slowstart'] * 100
    ax.plot(stats['slowstart'], expected_ratio, 
           'r--', linewidth=2, alpha=0.7, label='Expected Start Ratio')
    
    ax.set_xlabel('Slowstart Value', fontsize=14, fontweight='bold')
    ax.set_ylabel('Reduce Start Time / Map Completion Time (%)', fontsize=12)
    ax.set_title('Reduce Start Timing: Actual vs Expected', fontsize=15, fontweight='bold')
    ax.legend(fontsize=12)
    ax.grid(True, alpha=0.3)
    
    # Add annotations for deviations
    for i, row in stats.iterrows():
        actual = start_ratio.iloc[i]
        expected = expected_ratio.iloc[i]
        if abs(actual - expected) > 5:  # Significant deviation
            ax.annotate(f'{actual:.1f}%', 
                       (row['slowstart'], actual),
                       textcoords="offset points", xytext=(10,5), 
                       fontsize=9, alpha=0.7)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ 已保存: {output_file}")
    plt.close()

def plot_comprehensive_comparison(stats, output_file='fig6_comprehensive.png'):
    """
    Figure 6: Comprehensive comparison - normalized metrics
    """
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Normalize all metrics to 0-1 scale for comparison
    metrics = {
        'Total Time': stats['total_time_from_api_mean'],
        'Map Completion': stats['map_completion_time_mean'],
        'Shuffle Time': stats['avg_shuffle_time_mean'],
        'CPU Time': stats['cpu_time_mean'],
    }
    
    for name, values in metrics.items():
        normalized = (values - values.min()) / (values.max() - values.min())
        ax.plot(stats['slowstart'], normalized, marker='o', linewidth=2, 
               markersize=8, label=name, alpha=0.8)
    
    ax.set_xlabel('Slowstart Value', fontsize=14, fontweight='bold')
    ax.set_ylabel('Normalized Value (0-1)', fontsize=12)
    ax.set_title('Task 1: Comprehensive Performance Metrics (Normalized)', fontsize=15, fontweight='bold')
    ax.legend(fontsize=11, loc='upper left')
    ax.grid(True, alpha=0.3)
    ax.set_ylim([-0.05, 1.05])
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ 已保存: {output_file}")
    plt.close()

def main():
    print("="*80)
    print("Task 1: Slowstart参数敏感性分析")
    print("="*80)
    
    # Load data
    print("\n[1/4] 加载实验数据...")
    df = load_data(RESULTS_FILE)
    print(f"  ✓ 已加载 {len(df)} 次实验运行")
    print(f"  ✓ Slowstart值: {sorted(df['slowstart'].unique())}")
    
    # Compute statistics
    print("\n[2/4] 计算统计数据...")
    stats = compute_statistics(df)
    print(f"  ✓ 已计算 {len(stats)} 个配置的统计信息")
    
    # Create tables
    print("\n[3/4] 生成汇总表格...")
    table2 = create_summary_tables(stats)
    table2.to_csv('table2_summary.csv', index=False)
    print(f"  ✓ 已保存: table2_summary.csv")
    
    # Generate figures
    print("\n[4/4] 生成图表...")
    plot_sensitivity_curve(stats)
    plot_timing_phases(stats)
    plot_performance_variance(stats)
    plot_resource_utilization(stats)
    plot_reduce_start_ratio(stats)
    plot_comprehensive_comparison(stats)
    
    # Find optimal configuration
    print("\n" + "="*80)
    print("关键发现:")
    print("="*80)
    
    min_idx = stats['total_time_from_api_mean'].idxmin()
    optimal = stats.loc[min_idx]
    
    print(f"\n最佳Slowstart配置: {optimal['slowstart']:.2f}")
    print(f"  - 平均执行时间: {optimal['total_time_from_api_mean']:.2f}秒")
    print(f"  - 标准差: {optimal['total_time_from_api_std']:.2f}秒")
    print(f"  - Map完成时间: {optimal['map_completion_time_mean']:.2f}秒")
    print(f"  - Reduce启动时间: {optimal['first_reduce_start_time_mean']:.2f}秒")
    
    # Compare with worst
    max_idx = stats['total_time_from_api_mean'].idxmax()
    worst = stats.loc[max_idx]
    improvement = ((worst['total_time_from_api_mean'] - optimal['total_time_from_api_mean']) / 
                   worst['total_time_from_api_mean'] * 100)
    
    print(f"\n最差Slowstart配置: {worst['slowstart']:.2f}")
    print(f"  - 平均执行时间: {worst['total_time_from_api_mean']:.2f}秒")
    print(f"\n性能提升: {improvement:.1f}% (相比最差配置)")
    
    print("\n" + "="*80)
    print("✓ 分析完成！所有图表和表格已保存。")
    print("="*80)

if __name__ == '__main__':
    main()

