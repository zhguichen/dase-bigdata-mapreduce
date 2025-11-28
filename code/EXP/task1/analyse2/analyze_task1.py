#!/usr/bin/env python3
"""
Task 1: 基准测试与参数敏感性分析 - 数据分析脚本
分析 slowstart 参数对 TeraSort 作业性能的影响
"""

import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import warnings
warnings.filterwarnings('ignore')

# 设置绘图风格
plt.style.use('seaborn-v0_8-whitegrid')
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['font.size'] = 12
plt.rcParams['axes.titlesize'] = 14
plt.rcParams['axes.labelsize'] = 12
plt.rcParams['lines.linewidth'] = 2
plt.rcParams['lines.markersize'] = 8

# 定义颜色方案
COLORS = {
    'primary': '#2E86AB',
    'secondary': '#A23B72',
    'tertiary': '#F18F01',
    'quaternary': '#C73E1D',
    'success': '#3A7D44',
    'warning': '#E9C46A',
    'info': '#6A0572',
}

def load_data(json_path):
    """加载实验数据"""
    with open(json_path, 'r') as f:
        data = json.load(f)
    return data

def create_dataframe(data):
    """将 JSON 数据转换为 DataFrame"""
    results = data['results']
    df = pd.DataFrame(results)
    return df

def calculate_summary_stats(df):
    """计算每个 slowstart 值的汇总统计"""
    summary = df.groupby('slowstart').agg({
        'job_elapsed_time': ['mean', 'std', 'min', 'max'],
        'avg_map_time': 'mean',
        'avg_shuffle_time': 'mean',
        'avg_merge_time': 'mean',
        'avg_reduce_time': 'mean',
        'map_completion_time': 'mean',
        'first_reduce_start_time': 'mean',
        'reduce_completion_time': 'mean',
        'total_map_time': 'mean',
        'total_reduce_time': 'mean',
        'cpu_time': 'mean',
        'gc_time': 'mean',
        'physical_memory_bytes': 'mean',
        'virtual_memory_bytes': 'mean',
        'peak_map_physical_memory': 'mean',
        'peak_reduce_physical_memory': 'mean',
        'reduce_shuffle_bytes': 'mean',
        'reduce_elapsed_stddev': 'mean',
    }).reset_index()
    
    # 扁平化列名
    summary.columns = ['_'.join(col).strip('_') if isinstance(col, tuple) else col 
                       for col in summary.columns.values]
    
    return summary

def plot_job_time_vs_slowstart(df, summary, output_dir):
    """图1: Job总时间随slowstart变化"""
    fig, ax = plt.subplots(figsize=(12, 7))
    
    # 绘制每次运行的散点
    for run in df['run_number'].unique():
        run_data = df[df['run_number'] == run]
        ax.scatter(run_data['slowstart'], run_data['job_elapsed_time'], 
                   alpha=0.6, s=60, label=f'Run {run}')
    
    # 绘制平均值曲线
    ax.plot(summary['slowstart'], summary['job_elapsed_time_mean'], 
            'o-', color=COLORS['quaternary'], linewidth=2.5, markersize=10,
            label='Average', zorder=5)
    
    # 添加误差棒
    ax.errorbar(summary['slowstart'], summary['job_elapsed_time_mean'],
                yerr=summary['job_elapsed_time_std'], fmt='none', 
                color=COLORS['quaternary'], alpha=0.5, capsize=5)
    
    ax.set_xlabel('Slowstart Parameter', fontsize=13)
    ax.set_ylabel('Job Elapsed Time (seconds)', fontsize=13)
    ax.set_title('Job Total Execution Time vs Slowstart Parameter\n(TeraSort 1GB, 4 Reducers)', fontsize=15)
    ax.legend(loc='upper left')
    ax.set_xticks(summary['slowstart'])
    
    # 标注最小值
    min_idx = summary['job_elapsed_time_mean'].idxmin()
    min_slowstart = summary.loc[min_idx, 'slowstart']
    min_time = summary.loc[min_idx, 'job_elapsed_time_mean']
    ax.annotate(f'Optimal: {min_slowstart}\n({min_time:.1f}s)', 
                xy=(min_slowstart, min_time), xytext=(min_slowstart+0.1, min_time+30),
                arrowprops=dict(arrowstyle='->', color='green'),
                fontsize=11, color='green', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/fig1_job_time_vs_slowstart.png', dpi=150, bbox_inches='tight')
    plt.close()
    print("Generated: fig1_job_time_vs_slowstart.png")

def plot_phase_times(summary, output_dir):
    """图2: 各阶段时间分解"""
    fig, axes = plt.subplots(1, 2, figsize=(16, 7))
    
    # 子图1: 堆叠柱状图 - 平均阶段时间
    ax1 = axes[0]
    x = np.arange(len(summary['slowstart']))
    width = 0.6
    
    # 计算Reduce等待时间 = first_reduce_start_time - 实际开始时刻(约0)
    # 注意：Reduce等待时间可以用 first_reduce_start_time 近似
    wait_time = summary['first_reduce_start_time_mean']
    shuffle_time = summary['avg_shuffle_time_mean'] * 4  # 4 reducers
    reduce_time = summary['avg_reduce_time_mean'] * 4
    map_time = summary['avg_map_time_mean']
    
    bars1 = ax1.bar(x, map_time, width, label='Avg Map Time', color=COLORS['primary'])
    bars2 = ax1.bar(x, summary['avg_shuffle_time_mean'], width, bottom=map_time, 
                    label='Avg Shuffle Time', color=COLORS['secondary'])
    bars3 = ax1.bar(x, summary['avg_merge_time_mean'], width, 
                    bottom=map_time + summary['avg_shuffle_time_mean'],
                    label='Avg Merge Time', color=COLORS['warning'])
    bars4 = ax1.bar(x, summary['avg_reduce_time_mean'], width, 
                    bottom=map_time + summary['avg_shuffle_time_mean'] + summary['avg_merge_time_mean'],
                    label='Avg Reduce Time', color=COLORS['tertiary'])
    
    ax1.set_xlabel('Slowstart Parameter', fontsize=12)
    ax1.set_ylabel('Time (seconds)', fontsize=12)
    ax1.set_title('Phase Time Breakdown by Slowstart', fontsize=14)
    ax1.set_xticks(x)
    ax1.set_xticklabels([f'{s:.2f}' for s in summary['slowstart']])
    ax1.legend(loc='upper left')
    
    # 子图2: 折线图 - 关键时间点
    ax2 = axes[1]
    ax2.plot(summary['slowstart'], summary['map_completion_time_mean'], 
             'o-', label='Map Completion Time', color=COLORS['primary'], linewidth=2)
    ax2.plot(summary['slowstart'], summary['first_reduce_start_time_mean'], 
             's--', label='First Reduce Start Time', color=COLORS['secondary'], linewidth=2)
    ax2.plot(summary['slowstart'], summary['reduce_completion_time_mean'], 
             '^-', label='Reduce Completion Time', color=COLORS['tertiary'], linewidth=2)
    ax2.plot(summary['slowstart'], summary['job_elapsed_time_mean'], 
             'D-', label='Job Elapsed Time', color=COLORS['quaternary'], linewidth=2.5)
    
    ax2.set_xlabel('Slowstart Parameter', fontsize=12)
    ax2.set_ylabel('Time (seconds)', fontsize=12)
    ax2.set_title('Key Time Points vs Slowstart', fontsize=14)
    ax2.legend(loc='upper left')
    ax2.set_xticks(summary['slowstart'])
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/fig2_phase_times.png', dpi=150, bbox_inches='tight')
    plt.close()
    print("Generated: fig2_phase_times.png")

def plot_map_reduce_timing(summary, output_dir):
    """图3: Map与Reduce时间关系"""
    fig, axes = plt.subplots(1, 2, figsize=(16, 7))
    
    # 子图1: Total Map Time vs Total Reduce Time
    ax1 = axes[0]
    ax1.plot(summary['slowstart'], summary['total_map_time_mean'], 
             'o-', label='Total Map Time', color=COLORS['primary'], linewidth=2.5)
    ax1.plot(summary['slowstart'], summary['total_reduce_time_mean'], 
             's-', label='Total Reduce Time', color=COLORS['secondary'], linewidth=2.5)
    
    ax1.set_xlabel('Slowstart Parameter', fontsize=12)
    ax1.set_ylabel('Total Time (seconds)', fontsize=12)
    ax1.set_title('Total Map vs Reduce Time', fontsize=14)
    ax1.legend()
    ax1.set_xticks(summary['slowstart'])
    
    # 子图2: Reduce等待时间分析
    ax2 = axes[1]
    # 计算Reduce等待时间 (等待Map完成的时间)
    reduce_wait_time = summary['first_reduce_start_time_mean']
    # Reduce overlap with Map = max(0, map_completion - first_reduce_start)
    overlap = np.maximum(0, summary['map_completion_time_mean'] - summary['first_reduce_start_time_mean'])
    
    ax2.bar(np.arange(len(summary)), reduce_wait_time, width=0.35, 
            label='Reduce Start Time', color=COLORS['primary'], alpha=0.8)
    ax2.bar(np.arange(len(summary)) + 0.35, overlap, width=0.35,
            label='Map-Reduce Overlap', color=COLORS['success'], alpha=0.8)
    
    ax2.set_xlabel('Slowstart Parameter', fontsize=12)
    ax2.set_ylabel('Time (seconds)', fontsize=12)
    ax2.set_title('Reduce Wait Time & Map-Reduce Overlap', fontsize=14)
    ax2.set_xticks(np.arange(len(summary)) + 0.175)
    ax2.set_xticklabels([f'{s:.2f}' for s in summary['slowstart']])
    ax2.legend()
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/fig3_map_reduce_timing.png', dpi=150, bbox_inches='tight')
    plt.close()
    print("Generated: fig3_map_reduce_timing.png")

def plot_resource_utilization(summary, output_dir):
    """图4: 资源利用情况 (CPU、内存)"""
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    # 子图1: CPU时间
    ax1 = axes[0, 0]
    ax1.plot(summary['slowstart'], summary['cpu_time_mean'], 
             'o-', color=COLORS['primary'], linewidth=2.5, markersize=8)
    ax1.fill_between(summary['slowstart'], summary['cpu_time_mean'], alpha=0.3, color=COLORS['primary'])
    ax1.set_xlabel('Slowstart Parameter', fontsize=12)
    ax1.set_ylabel('CPU Time (seconds)', fontsize=12)
    ax1.set_title('CPU Time vs Slowstart', fontsize=14)
    ax1.set_xticks(summary['slowstart'])
    
    # 子图2: GC时间
    ax2 = axes[0, 1]
    ax2.plot(summary['slowstart'], summary['gc_time_mean'], 
             's-', color=COLORS['secondary'], linewidth=2.5, markersize=8)
    ax2.fill_between(summary['slowstart'], summary['gc_time_mean'], alpha=0.3, color=COLORS['secondary'])
    ax2.set_xlabel('Slowstart Parameter', fontsize=12)
    ax2.set_ylabel('GC Time (seconds)', fontsize=12)
    ax2.set_title('Garbage Collection Time vs Slowstart', fontsize=14)
    ax2.set_xticks(summary['slowstart'])
    
    # 子图3: 物理内存使用
    ax3 = axes[1, 0]
    memory_gb = summary['physical_memory_bytes_mean'] / (1024**3)
    ax3.bar(range(len(summary)), memory_gb, color=COLORS['tertiary'], alpha=0.8)
    ax3.set_xlabel('Slowstart Parameter', fontsize=12)
    ax3.set_ylabel('Physical Memory (GB)', fontsize=12)
    ax3.set_title('Physical Memory Usage vs Slowstart', fontsize=14)
    ax3.set_xticks(range(len(summary)))
    ax3.set_xticklabels([f'{s:.2f}' for s in summary['slowstart']])
    
    # 子图4: Map vs Reduce 峰值内存
    ax4 = axes[1, 1]
    x = np.arange(len(summary))
    width = 0.35
    map_peak_mb = summary['peak_map_physical_memory_mean'] / (1024**2)
    reduce_peak_mb = summary['peak_reduce_physical_memory_mean'] / (1024**2)
    
    ax4.bar(x - width/2, map_peak_mb, width, label='Map Peak Memory', color=COLORS['primary'])
    ax4.bar(x + width/2, reduce_peak_mb, width, label='Reduce Peak Memory', color=COLORS['secondary'])
    ax4.set_xlabel('Slowstart Parameter', fontsize=12)
    ax4.set_ylabel('Peak Memory (MB)', fontsize=12)
    ax4.set_title('Map vs Reduce Peak Physical Memory', fontsize=14)
    ax4.set_xticks(x)
    ax4.set_xticklabels([f'{s:.2f}' for s in summary['slowstart']])
    ax4.legend()
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/fig4_resource_utilization.png', dpi=150, bbox_inches='tight')
    plt.close()
    print("Generated: fig4_resource_utilization.png")

def plot_shuffle_analysis(df, summary, output_dir):
    """图5: Shuffle性能分析"""
    fig, axes = plt.subplots(1, 2, figsize=(16, 7))
    
    # 子图1: Shuffle时间分析
    ax1 = axes[0]
    
    # 获取每个slowstart的shuffle时间统计
    shuffle_data = []
    for _, row in df.iterrows():
        if isinstance(row.get('shuffle_time'), dict):
            shuffle_data.append({
                'slowstart': row['slowstart'],
                'min': row['shuffle_time']['min'],
                'max': row['shuffle_time']['max'],
                'avg': row['shuffle_time']['avg']
            })
    
    if shuffle_data:
        shuffle_df = pd.DataFrame(shuffle_data)
        shuffle_summary = shuffle_df.groupby('slowstart').mean().reset_index()
        
        ax1.errorbar(shuffle_summary['slowstart'], shuffle_summary['avg'],
                     yerr=[shuffle_summary['avg'] - shuffle_summary['min'],
                           shuffle_summary['max'] - shuffle_summary['avg']],
                     fmt='o-', capsize=5, capthick=2, color=COLORS['primary'],
                     linewidth=2.5, markersize=8, label='Shuffle Time (avg ± range)')
    
    ax1.set_xlabel('Slowstart Parameter', fontsize=12)
    ax1.set_ylabel('Shuffle Time (seconds)', fontsize=12)
    ax1.set_title('Shuffle Time Distribution by Slowstart', fontsize=14)
    ax1.legend()
    ax1.set_xticks(summary['slowstart'])
    
    # 子图2: Shuffle数据量与带宽
    ax2 = axes[1]
    shuffle_mb = summary['reduce_shuffle_bytes_mean'] / (1024**2)
    # 估算带宽 = shuffle数据量 / shuffle时间
    avg_shuffle_time = summary['avg_shuffle_time_mean']
    bandwidth_mbps = np.where(avg_shuffle_time > 0, shuffle_mb / avg_shuffle_time, 0)
    
    ax2_twin = ax2.twinx()
    
    bars = ax2.bar(range(len(summary)), shuffle_mb, color=COLORS['tertiary'], alpha=0.7, label='Shuffle Data')
    line, = ax2_twin.plot(range(len(summary)), bandwidth_mbps, 'D-', color=COLORS['quaternary'], 
                          linewidth=2.5, markersize=8, label='Effective Bandwidth')
    
    ax2.set_xlabel('Slowstart Parameter', fontsize=12)
    ax2.set_ylabel('Shuffle Data (MB)', fontsize=12, color=COLORS['tertiary'])
    ax2_twin.set_ylabel('Effective Bandwidth (MB/s)', fontsize=12, color=COLORS['quaternary'])
    ax2.set_title('Shuffle Data Volume & Effective Bandwidth', fontsize=14)
    ax2.set_xticks(range(len(summary)))
    ax2.set_xticklabels([f'{s:.2f}' for s in summary['slowstart']])
    
    # 合并图例
    lines1, labels1 = ax2.get_legend_handles_labels()
    lines2, labels2 = ax2_twin.get_legend_handles_labels()
    ax2.legend(lines1 + lines2, labels1 + labels2, loc='upper right')
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/fig5_shuffle_analysis.png', dpi=150, bbox_inches='tight')
    plt.close()
    print("Generated: fig5_shuffle_analysis.png")

def plot_reduce_variance(df, summary, output_dir):
    """图6: Reduce任务方差分析"""
    fig, axes = plt.subplots(1, 2, figsize=(16, 7))
    
    # 子图1: Reduce完成时间标准差
    ax1 = axes[0]
    ax1.bar(range(len(summary)), summary['reduce_elapsed_stddev_mean'], 
            color=COLORS['info'], alpha=0.8)
    ax1.set_xlabel('Slowstart Parameter', fontsize=12)
    ax1.set_ylabel('Reduce Elapsed Time Std Dev (seconds)', fontsize=12)
    ax1.set_title('Reduce Task Time Variance by Slowstart', fontsize=14)
    ax1.set_xticks(range(len(summary)))
    ax1.set_xticklabels([f'{s:.2f}' for s in summary['slowstart']])
    
    # 子图2: Reduce时间范围 (min, avg, max)
    ax2 = axes[1]
    reduce_data = []
    for _, row in df.iterrows():
        reduce_data.append({
            'slowstart': row['slowstart'],
            'min_elapsed': row.get('min_reduce_elapsed', 0),
            'max_elapsed': row.get('max_reduce_elapsed', 0),
            'avg_elapsed': row.get('avg_reduce_elapsed', 0)
        })
    
    reduce_df = pd.DataFrame(reduce_data)
    reduce_summary = reduce_df.groupby('slowstart').mean().reset_index()
    
    x = range(len(reduce_summary))
    ax2.fill_between(x, reduce_summary['min_elapsed'], reduce_summary['max_elapsed'], 
                     alpha=0.3, color=COLORS['secondary'], label='Range (min-max)')
    ax2.plot(x, reduce_summary['avg_elapsed'], 'o-', color=COLORS['secondary'], 
             linewidth=2.5, markersize=8, label='Average')
    ax2.plot(x, reduce_summary['min_elapsed'], '--', color=COLORS['success'], 
             linewidth=1.5, label='Minimum')
    ax2.plot(x, reduce_summary['max_elapsed'], '--', color=COLORS['quaternary'], 
             linewidth=1.5, label='Maximum')
    
    ax2.set_xlabel('Slowstart Parameter', fontsize=12)
    ax2.set_ylabel('Reduce Elapsed Time (seconds)', fontsize=12)
    ax2.set_title('Reduce Task Time Range by Slowstart', fontsize=14)
    ax2.set_xticks(x)
    ax2.set_xticklabels([f'{s:.2f}' for s in reduce_summary['slowstart']])
    ax2.legend()
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/fig6_reduce_variance.png', dpi=150, bbox_inches='tight')
    plt.close()
    print("Generated: fig6_reduce_variance.png")

def plot_comprehensive_overview(summary, output_dir):
    """图7: 综合性能概览"""
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # 使用雷达图或热力图展示综合性能
    # 这里使用归一化后的多指标对比
    metrics = {
        'Job Time': summary['job_elapsed_time_mean'],
        'CPU Time': summary['cpu_time_mean'],
        'Shuffle Time': summary['avg_shuffle_time_mean'],
        'Reduce Variance': summary['reduce_elapsed_stddev_mean'],
        'Memory Usage': summary['physical_memory_bytes_mean'] / (1024**3),
    }
    
    # 归一化到0-1范围
    normalized = {}
    for key, values in metrics.items():
        min_val, max_val = values.min(), values.max()
        if max_val - min_val > 0:
            normalized[key] = (values - min_val) / (max_val - min_val)
        else:
            normalized[key] = values * 0
    
    x = np.arange(len(summary))
    width = 0.15
    
    for i, (key, values) in enumerate(normalized.items()):
        ax.bar(x + i*width, values, width, label=key, alpha=0.8)
    
    ax.set_xlabel('Slowstart Parameter', fontsize=12)
    ax.set_ylabel('Normalized Value (0-1, lower is better)', fontsize=12)
    ax.set_title('Comprehensive Performance Overview\n(All metrics normalized, lower is better)', fontsize=14)
    ax.set_xticks(x + width*2)
    ax.set_xticklabels([f'{s:.2f}' for s in summary['slowstart']])
    ax.legend(loc='upper right')
    ax.axhline(y=0.5, color='gray', linestyle='--', alpha=0.5)
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/fig7_comprehensive_overview.png', dpi=150, bbox_inches='tight')
    plt.close()
    print("Generated: fig7_comprehensive_overview.png")

def generate_tables(df, summary, output_dir):
    """生成CSV表格"""
    
    # 表1: 原始运行记录
    table1 = df[['slowstart', 'run_number', 'job_id', 'job_elapsed_time', 
                 'map_completion_time', 'first_reduce_start_time', 'reduce_completion_time',
                 'cpu_time', 'avg_shuffle_time', 'avg_reduce_time']].copy()
    table1.columns = ['Slowstart', 'Run', 'Job ID', 'Total Time (s)', 
                      'Map Completion (s)', 'Reduce Start (s)', 'Reduce Completion (s)',
                      'CPU Time (s)', 'Avg Shuffle (s)', 'Avg Reduce (s)']
    table1.to_csv(f'{output_dir}/table1_raw_results.csv', index=False)
    print("Generated: table1_raw_results.csv")
    
    # 表2: 汇总统计
    table2 = pd.DataFrame({
        'Slowstart': summary['slowstart'],
        'Avg Total Time (s)': summary['job_elapsed_time_mean'].round(2),
        'Std Dev (s)': summary['job_elapsed_time_std'].round(2),
        'Avg Map Completion (s)': summary['map_completion_time_mean'].round(2),
        'Avg Reduce Start (s)': summary['first_reduce_start_time_mean'].round(2),
        'Avg Reduce Completion (s)': summary['reduce_completion_time_mean'].round(2),
        'Avg CPU Time (s)': summary['cpu_time_mean'].round(2),
        'Avg Shuffle Time (s)': summary['avg_shuffle_time_mean'].round(2),
        'Reduce Variance (s)': summary['reduce_elapsed_stddev_mean'].round(2),
    })
    table2.to_csv(f'{output_dir}/table2_summary.csv', index=False)
    print("Generated: table2_summary.csv")
    
    # 表3: 资源使用统计
    table3 = pd.DataFrame({
        'Slowstart': summary['slowstart'],
        'Total Map Time (s)': summary['total_map_time_mean'].round(2),
        'Total Reduce Time (s)': summary['total_reduce_time_mean'].round(2),
        'CPU Time (s)': summary['cpu_time_mean'].round(2),
        'GC Time (s)': summary['gc_time_mean'].round(2),
        'Physical Memory (GB)': (summary['physical_memory_bytes_mean'] / (1024**3)).round(2),
        'Map Peak Mem (MB)': (summary['peak_map_physical_memory_mean'] / (1024**2)).round(2),
        'Reduce Peak Mem (MB)': (summary['peak_reduce_physical_memory_mean'] / (1024**2)).round(2),
        'Shuffle Data (MB)': (summary['reduce_shuffle_bytes_mean'] / (1024**2)).round(2),
    })
    table3.to_csv(f'{output_dir}/table3_resources.csv', index=False)
    print("Generated: table3_resources.csv")
    
    return table1, table2, table3

def analyze_optimal_slowstart(summary):
    """分析最佳slowstart值"""
    min_idx = summary['job_elapsed_time_mean'].idxmin()
    optimal = summary.loc[min_idx]
    
    analysis = {
        'optimal_slowstart': optimal['slowstart'],
        'optimal_time': optimal['job_elapsed_time_mean'],
        'worst_slowstart': summary.loc[summary['job_elapsed_time_mean'].idxmax(), 'slowstart'],
        'worst_time': summary['job_elapsed_time_mean'].max(),
        'improvement': ((summary['job_elapsed_time_mean'].max() - optimal['job_elapsed_time_mean']) 
                        / summary['job_elapsed_time_mean'].max() * 100),
    }
    
    return analysis

def main():
    # 数据文件路径
    json_path = '/root/Exp-hadoop/EXP/task1/results/raw_results_20251128_130009.json'
    output_dir = '/root/Exp-hadoop/EXP/task1/analyse2'
    
    print("="*60)
    print("Task 1: 参数敏感性分析 - 数据分析")
    print("="*60)
    
    # 加载数据
    print("\n1. 加载数据...")
    data = load_data(json_path)
    df = create_dataframe(data)
    print(f"   - 配置: {data['configuration']}")
    print(f"   - 总记录数: {len(df)}")
    
    # 计算汇总统计
    print("\n2. 计算汇总统计...")
    summary = calculate_summary_stats(df)
    print(f"   - Slowstart值: {list(summary['slowstart'])}")
    
    # 分析最佳slowstart
    print("\n3. 分析最佳slowstart...")
    analysis = analyze_optimal_slowstart(summary)
    print(f"   - 最佳slowstart: {analysis['optimal_slowstart']}")
    print(f"   - 最佳时间: {analysis['optimal_time']:.2f}s")
    print(f"   - 最差slowstart: {analysis['worst_slowstart']}")
    print(f"   - 最差时间: {analysis['worst_time']:.2f}s")
    print(f"   - 性能提升: {analysis['improvement']:.1f}%")
    
    # 生成图表
    print("\n4. 生成图表...")
    plot_job_time_vs_slowstart(df, summary, output_dir)
    plot_phase_times(summary, output_dir)
    plot_map_reduce_timing(summary, output_dir)
    plot_resource_utilization(summary, output_dir)
    plot_shuffle_analysis(df, summary, output_dir)
    plot_reduce_variance(df, summary, output_dir)
    plot_comprehensive_overview(summary, output_dir)
    
    # 生成表格
    print("\n5. 生成表格...")
    tables = generate_tables(df, summary, output_dir)
    
    # 保存分析结果
    print("\n6. 保存分析结果...")
    with open(f'{output_dir}/analysis_results.json', 'w') as f:
        json.dump({
            'configuration': data['configuration'],
            'optimal_analysis': analysis,
            'summary_stats': summary.to_dict('records')
        }, f, indent=2)
    print("Generated: analysis_results.json")
    
    print("\n" + "="*60)
    print("分析完成！")
    print("="*60)
    
    return df, summary, analysis

if __name__ == '__main__':
    df, summary, analysis = main()







