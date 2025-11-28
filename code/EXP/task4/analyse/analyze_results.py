#!/usr/bin/env python3
"""
Task 4: Data Skew Testing Analysis
Analyze the impact of data skew on MapReduce performance with different slowstart values.
"""

import json
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import seaborn as sns
import warnings
warnings.filterwarnings('ignore')

# Set style
plt.style.use('seaborn-v0_8-whitegrid')
plt.rcParams['font.family'] = 'DejaVu Sans'
plt.rcParams['axes.unicode_minus'] = False

# Configuration
RESULTS_FILE = '../results/raw_results_20251128_205311_enhanced.json'
OUTPUT_DIR = Path('.')

# Define color scheme
COLORS = {
    'skewed': '#e74c3c',  # Red for skewed
    'uniform': '#3498db',  # Blue for uniform
    'highlight': '#f39c12',  # Orange for highlights
    'grid': '#ecf0f1'
}

def load_data(filename):
    """Load experimental results from JSON file."""
    with open(filename, 'r') as f:
        data = json.load(f)
    return pd.DataFrame(data['results']), data['configuration']

def compute_statistics(df):
    """Compute average and std for each data_type + slowstart configuration."""
    # Define aggregation columns based on available data
    agg_dict = {
        'total_time_from_api': ['mean', 'std', 'count'],
        'map_completion_time': ['mean'],
        'first_reduce_start_time': ['mean'],
        'reduce_completion_time': ['mean'],
        'min_reduce_elapsed': ['mean'],
        'max_reduce_elapsed': ['mean'],
        'avg_reduce_elapsed': ['mean'],
        'reduce_elapsed_stddev': ['mean', 'std'],
        'avg_shuffle_time': ['mean'],
        'avg_reduce_time': ['mean'],
        'cpu_time': ['mean'],
        'gc_time': ['mean'],
        'reduce_shuffle_bytes': ['mean'],
        'physical_memory_bytes': ['mean'],
        'hdfs_bytes_read': ['mean'],
    }
    
    # Add shuffle_time, merge_time, reduce_time stats if available
    if 'shuffle_time' in df.columns:
        # Extract nested values
        df['shuffle_time_min'] = df['shuffle_time'].apply(lambda x: x['min'] if isinstance(x, dict) else np.nan)
        df['shuffle_time_max'] = df['shuffle_time'].apply(lambda x: x['max'] if isinstance(x, dict) else np.nan)
        df['shuffle_time_avg'] = df['shuffle_time'].apply(lambda x: x['avg'] if isinstance(x, dict) else np.nan)
        agg_dict.update({
            'shuffle_time_min': ['mean'],
            'shuffle_time_max': ['mean'],
            'shuffle_time_avg': ['mean']
        })
    
    if 'reduce_time' in df.columns:
        df['reduce_time_min'] = df['reduce_time'].apply(lambda x: x['min'] if isinstance(x, dict) else np.nan)
        df['reduce_time_max'] = df['reduce_time'].apply(lambda x: x['max'] if isinstance(x, dict) else np.nan)
        df['reduce_time_avg'] = df['reduce_time'].apply(lambda x: x['avg'] if isinstance(x, dict) else np.nan)
        agg_dict.update({
            'reduce_time_min': ['mean'],
            'reduce_time_max': ['mean'],
            'reduce_time_avg': ['mean']
        })
    
    stats = df.groupby(['data_type', 'slowstart']).agg(agg_dict).reset_index()
    
    # Flatten column names
    stats.columns = ['_'.join(col).strip('_') if col[1] else col[0] 
                     for col in stats.columns.values]
    
    return stats

def compute_derived_metrics(stats):
    """Compute derived metrics for analysis."""
    # Straggler ratio: max/min reduce elapsed time ratio
    stats['straggler_ratio'] = stats['max_reduce_elapsed_mean'] / stats['min_reduce_elapsed_mean']
    
    # Straggler delay: max - avg reduce elapsed time
    stats['straggler_delay'] = stats['max_reduce_elapsed_mean'] - stats['avg_reduce_elapsed_mean']
    
    # Reduce time range: max - min reduce finish time
    stats['reduce_time_range'] = stats['max_reduce_elapsed_mean'] - stats['min_reduce_elapsed_mean']
    
    # Slowest reduce ratio: (max_reduce_elapsed - map_completion) / total_time
    # This indicates how much the slowest reduce contributes to total time
    
    return stats

def plot_total_time_comparison(stats, output_file='fig1_total_time_comparison.png'):
    """
    Figure 1 (4a): Grouped bar chart comparing total time for skewed vs uniform
    """
    fig, ax = plt.subplots(figsize=(14, 7))
    
    slowstart_values = sorted(stats['slowstart'].unique())
    x = np.arange(len(slowstart_values))
    width = 0.35
    
    skewed = stats[stats['data_type'] == 'skewed'].sort_values('slowstart')
    uniform = stats[stats['data_type'] == 'uniform'].sort_values('slowstart')
    
    bars1 = ax.bar(x - width/2, skewed['total_time_from_api_mean'], width, 
                   label='Skewed Data', color=COLORS['skewed'], alpha=0.85,
                   edgecolor='black', linewidth=0.8)
    bars2 = ax.bar(x + width/2, uniform['total_time_from_api_mean'], width,
                   label='Uniform Data', color=COLORS['uniform'], alpha=0.85,
                   edgecolor='black', linewidth=0.8)
    
    # Add error bars
    ax.errorbar(x - width/2, skewed['total_time_from_api_mean'], 
                yerr=skewed['total_time_from_api_std'], fmt='none', 
                color='black', capsize=4, alpha=0.7)
    ax.errorbar(x + width/2, uniform['total_time_from_api_mean'], 
                yerr=uniform['total_time_from_api_std'], fmt='none', 
                color='black', capsize=4, alpha=0.7)
    
    ax.set_xlabel('Slowstart Value', fontsize=14, fontweight='bold')
    ax.set_ylabel('Average Total Time (s)', fontsize=14, fontweight='bold')
    ax.set_title('Task 4: Data Skew Impact on Total Execution Time', 
                fontsize=16, fontweight='bold', pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels([f'{v:.2f}' for v in slowstart_values], fontsize=11)
    ax.legend(fontsize=12, loc='upper right')
    ax.grid(True, alpha=0.3, axis='y')
    
    # Add percentage difference labels
    for i, (s, u) in enumerate(zip(skewed['total_time_from_api_mean'], 
                                    uniform['total_time_from_api_mean'])):
        diff = (s - u) / u * 100
        ax.annotate(f'+{diff:.1f}%', xy=(i, max(s, u) + 2), 
                   fontsize=9, ha='center', color='#c0392b', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file}")
    plt.close()

def plot_reduce_stddev_comparison(stats, output_file='fig2_reduce_stddev.png'):
    """
    Figure 2 (4d): Reduce completion time standard deviation comparison
    """
    fig, ax = plt.subplots(figsize=(12, 6))
    
    for data_type in ['skewed', 'uniform']:
        subset = stats[stats['data_type'] == data_type].sort_values('slowstart')
        label = 'Skewed Data' if data_type == 'skewed' else 'Uniform Data'
        color = COLORS[data_type]
        ax.plot(subset['slowstart'], subset['reduce_elapsed_stddev_mean'], 
               marker='o', linewidth=2.5, markersize=10, label=label, color=color)
        ax.fill_between(subset['slowstart'], 
                        subset['reduce_elapsed_stddev_mean'] - subset['reduce_elapsed_stddev_std'],
                        subset['reduce_elapsed_stddev_mean'] + subset['reduce_elapsed_stddev_std'],
                        alpha=0.2, color=color)
    
    ax.set_xlabel('Slowstart Value', fontsize=14, fontweight='bold')
    ax.set_ylabel('Reduce Elapsed Time Std Dev (s)', fontsize=14, fontweight='bold')
    ax.set_title('Task 4: Data Skew - Reduce Task Time Variability', 
                fontsize=16, fontweight='bold', pad=15)
    ax.legend(fontsize=12, loc='best')
    ax.grid(True, alpha=0.3)
    
    # Add annotation for skew indicator
    ax.axhline(y=5, color='#e74c3c', linestyle='--', alpha=0.5, linewidth=1.5)
    ax.text(0.95, 5.5, 'Severe Skew Threshold', fontsize=10, 
           ha='right', color='#e74c3c', style='italic')
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file}")
    plt.close()

def plot_straggler_analysis(stats, output_file='fig3_straggler_analysis.png'):
    """
    Figure 3 (4c): Straggler delay comparison (max - avg reduce time)
    """
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    slowstart_values = sorted(stats['slowstart'].unique())
    x = np.arange(len(slowstart_values))
    width = 0.35
    
    skewed = stats[stats['data_type'] == 'skewed'].sort_values('slowstart')
    uniform = stats[stats['data_type'] == 'uniform'].sort_values('slowstart')
    
    # Left: Straggler delay (max - avg)
    ax1.bar(x - width/2, skewed['straggler_delay'], width, 
            label='Skewed Data', color=COLORS['skewed'], alpha=0.85,
            edgecolor='black', linewidth=0.8)
    ax1.bar(x + width/2, uniform['straggler_delay'], width,
            label='Uniform Data', color=COLORS['uniform'], alpha=0.85,
            edgecolor='black', linewidth=0.8)
    
    ax1.set_xlabel('Slowstart Value', fontsize=13, fontweight='bold')
    ax1.set_ylabel('Straggler Delay (s)\n(Max - Avg Reduce Time)', fontsize=12, fontweight='bold')
    ax1.set_title('(a) Straggler Delay Analysis', fontsize=14, fontweight='bold')
    ax1.set_xticks(x)
    ax1.set_xticklabels([f'{v:.2f}' for v in slowstart_values], fontsize=10)
    ax1.legend(fontsize=11)
    ax1.grid(True, alpha=0.3, axis='y')
    
    # Right: Straggler ratio (max/min)
    ax2.bar(x - width/2, skewed['straggler_ratio'], width, 
            label='Skewed Data', color=COLORS['skewed'], alpha=0.85,
            edgecolor='black', linewidth=0.8)
    ax2.bar(x + width/2, uniform['straggler_ratio'], width,
            label='Uniform Data', color=COLORS['uniform'], alpha=0.85,
            edgecolor='black', linewidth=0.8)
    
    ax2.set_xlabel('Slowstart Value', fontsize=13, fontweight='bold')
    ax2.set_ylabel('Straggler Ratio\n(Max / Min Reduce Time)', fontsize=12, fontweight='bold')
    ax2.set_title('(b) Straggler Ratio Analysis', fontsize=14, fontweight='bold')
    ax2.set_xticks(x)
    ax2.set_xticklabels([f'{v:.2f}' for v in slowstart_values], fontsize=10)
    ax2.legend(fontsize=11)
    ax2.grid(True, alpha=0.3, axis='y')
    
    # Add threshold line for ratio
    ax2.axhline(y=2.0, color='#e74c3c', linestyle='--', alpha=0.6, linewidth=1.5)
    ax2.text(len(slowstart_values)-0.5, 2.1, 'Skew Threshold (2.0)', fontsize=9, 
            ha='right', color='#e74c3c', style='italic')
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file}")
    plt.close()

def plot_reduce_time_distribution(df, output_file='fig4_reduce_time_boxplot.png'):
    """
    Figure 4 (4b): Box plot showing reduce task time distribution
    """
    fig, ax = plt.subplots(figsize=(14, 7))
    
    # Create data for box plot
    plot_data = []
    labels = []
    positions = []
    colors_list = []
    
    slowstart_values = sorted(df['slowstart'].unique())
    
    pos = 0
    for i, ss in enumerate(slowstart_values):
        for dt in ['skewed', 'uniform']:
            subset = df[(df['slowstart'] == ss) & (df['data_type'] == dt)]
            # Collect all reduce elapsed times for this configuration
            for _, row in subset.iterrows():
                times = [row['min_reduce_elapsed'], row['avg_reduce_elapsed'], row['max_reduce_elapsed']]
                plot_data.append(times)
                labels.append(f'{ss}-{dt[:1].upper()}')
                positions.append(pos)
                colors_list.append(COLORS[dt])
            pos += 0.5
        pos += 0.5  # Extra space between slowstart groups
    
    # Create simplified box plot using min/max/avg
    for i, (ss, dt) in enumerate([(ss, dt) for ss in slowstart_values for dt in ['skewed', 'uniform']]):
        subset = df[(df['slowstart'] == ss) & (df['data_type'] == dt)]
        
        x_pos = i * 1.5 + (0.4 if dt == 'uniform' else 0)
        
        # Get average metrics across runs
        min_val = subset['min_reduce_elapsed'].mean()
        max_val = subset['max_reduce_elapsed'].mean()
        avg_val = subset['avg_reduce_elapsed'].mean()
        std_val = subset['reduce_elapsed_stddev'].mean()
        
        # Draw box (using avg ± std)
        box = plt.Rectangle((x_pos - 0.15, avg_val - std_val), 0.3, 2*std_val,
                            facecolor=COLORS[dt], alpha=0.6, edgecolor='black', linewidth=1)
        ax.add_patch(box)
        
        # Draw whiskers (min and max)
        ax.plot([x_pos, x_pos], [min_val, avg_val - std_val], color='black', linewidth=1)
        ax.plot([x_pos, x_pos], [avg_val + std_val, max_val], color='black', linewidth=1)
        ax.plot([x_pos - 0.1, x_pos + 0.1], [min_val, min_val], color='black', linewidth=1.5)
        ax.plot([x_pos - 0.1, x_pos + 0.1], [max_val, max_val], color='black', linewidth=1.5)
        
        # Draw median line (using avg)
        ax.plot([x_pos - 0.15, x_pos + 0.15], [avg_val, avg_val], color='black', linewidth=2)
    
    # Set labels
    ax.set_xticks([i * 1.5 + 0.2 for i in range(len(slowstart_values))])
    ax.set_xticklabels([f'{ss:.2f}' for ss in slowstart_values], fontsize=11)
    ax.set_xlabel('Slowstart Value', fontsize=14, fontweight='bold')
    ax.set_ylabel('Reduce Task Elapsed Time (s)', fontsize=14, fontweight='bold')
    ax.set_title('Task 4: Reduce Task Time Distribution by Data Type', 
                fontsize=16, fontweight='bold', pad=15)
    
    # Add legend
    from matplotlib.patches import Patch
    legend_elements = [Patch(facecolor=COLORS['skewed'], alpha=0.6, edgecolor='black', label='Skewed Data'),
                       Patch(facecolor=COLORS['uniform'], alpha=0.6, edgecolor='black', label='Uniform Data')]
    ax.legend(handles=legend_elements, fontsize=12, loc='upper right')
    ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file}")
    plt.close()

def plot_shuffle_phase_analysis(stats, output_file='fig5_shuffle_phase.png'):
    """
    Figure 5: Shuffle phase time analysis for skewed vs uniform data
    """
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    for data_type in ['skewed', 'uniform']:
        subset = stats[stats['data_type'] == data_type].sort_values('slowstart')
        label = 'Skewed Data' if data_type == 'skewed' else 'Uniform Data'
        color = COLORS[data_type]
        
        # Left: Average shuffle time
        ax1.plot(subset['slowstart'], subset['avg_shuffle_time_mean'], 
                marker='o', linewidth=2.5, markersize=9, label=label, color=color)
    
    ax1.set_xlabel('Slowstart Value', fontsize=13, fontweight='bold')
    ax1.set_ylabel('Average Shuffle Time (s)', fontsize=13, fontweight='bold')
    ax1.set_title('(a) Average Shuffle Time', fontsize=14, fontweight='bold')
    ax1.legend(fontsize=11)
    ax1.grid(True, alpha=0.3)
    
    # Right: Shuffle time range (max - min) if available
    if 'shuffle_time_max_mean' in stats.columns and 'shuffle_time_min_mean' in stats.columns:
        for data_type in ['skewed', 'uniform']:
            subset = stats[stats['data_type'] == data_type].sort_values('slowstart')
            shuffle_range = subset['shuffle_time_max_mean'] - subset['shuffle_time_min_mean']
            label = 'Skewed Data' if data_type == 'skewed' else 'Uniform Data'
            color = COLORS[data_type]
            ax2.plot(subset['slowstart'], shuffle_range, 
                    marker='s', linewidth=2.5, markersize=9, label=label, color=color)
        
        ax2.set_xlabel('Slowstart Value', fontsize=13, fontweight='bold')
        ax2.set_ylabel('Shuffle Time Range (Max - Min) (s)', fontsize=13, fontweight='bold')
        ax2.set_title('(b) Shuffle Time Variability', fontsize=14, fontweight='bold')
        ax2.legend(fontsize=11)
        ax2.grid(True, alpha=0.3)
    else:
        # Alternative: CPU time comparison
        for data_type in ['skewed', 'uniform']:
            subset = stats[stats['data_type'] == data_type].sort_values('slowstart')
            label = 'Skewed Data' if data_type == 'skewed' else 'Uniform Data'
            color = COLORS[data_type]
            ax2.plot(subset['slowstart'], subset['cpu_time_mean'], 
                    marker='s', linewidth=2.5, markersize=9, label=label, color=color)
        
        ax2.set_xlabel('Slowstart Value', fontsize=13, fontweight='bold')
        ax2.set_ylabel('CPU Time (s)', fontsize=13, fontweight='bold')
        ax2.set_title('(b) CPU Time Comparison', fontsize=14, fontweight='bold')
        ax2.legend(fontsize=11)
        ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file}")
    plt.close()

def plot_optimal_slowstart(stats, output_file='fig6_optimal_slowstart.png'):
    """
    Figure 6: Optimal slowstart for each data type
    """
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    
    optimal_data = []
    
    for data_type in ['skewed', 'uniform']:
        subset = stats[stats['data_type'] == data_type]
        min_idx = subset['total_time_from_api_mean'].idxmin()
        optimal_slowstart = subset.loc[min_idx, 'slowstart']
        optimal_time = subset.loc[min_idx, 'total_time_from_api_mean']
        optimal_stddev = subset.loc[min_idx, 'reduce_elapsed_stddev_mean']
        optimal_data.append({
            'data_type': data_type,
            'optimal_slowstart': optimal_slowstart,
            'optimal_time': optimal_time,
            'optimal_stddev': optimal_stddev
        })
    
    df_optimal = pd.DataFrame(optimal_data)
    
    # Left: Line plot with optimal point highlighted
    for data_type in ['skewed', 'uniform']:
        subset = stats[stats['data_type'] == data_type].sort_values('slowstart')
        label = 'Skewed Data' if data_type == 'skewed' else 'Uniform Data'
        color = COLORS[data_type]
        ax1.plot(subset['slowstart'], subset['total_time_from_api_mean'], 
                marker='o', linewidth=2.5, markersize=8, label=label, color=color, alpha=0.8)
        
        # Highlight optimal point
        opt = df_optimal[df_optimal['data_type'] == data_type].iloc[0]
        ax1.scatter([opt['optimal_slowstart']], [opt['optimal_time']], 
                   s=200, color=color, edgecolor='black', linewidth=2, zorder=5, marker='*')
        ax1.annotate(f'Optimal: {opt["optimal_slowstart"]:.2f}', 
                    xy=(opt['optimal_slowstart'], opt['optimal_time']),
                    xytext=(opt['optimal_slowstart'] + 0.1, opt['optimal_time'] + 2),
                    fontsize=10, fontweight='bold',
                    arrowprops=dict(arrowstyle='->', color=color, alpha=0.7))
    
    ax1.set_xlabel('Slowstart Value', fontsize=13, fontweight='bold')
    ax1.set_ylabel('Average Total Time (s)', fontsize=13, fontweight='bold')
    ax1.set_title('(a) Performance Curve with Optimal Points', fontsize=14, fontweight='bold')
    ax1.legend(fontsize=11)
    ax1.grid(True, alpha=0.3)
    
    # Right: Bar chart of optimal values
    x = np.arange(2)
    width = 0.5
    
    colors_bar = [COLORS['skewed'], COLORS['uniform']]
    bars = ax2.bar(x, df_optimal['optimal_slowstart'], width, 
                   color=colors_bar, alpha=0.85, edgecolor='black', linewidth=1.5)
    
    ax2.set_xlabel('Data Type', fontsize=13, fontweight='bold')
    ax2.set_ylabel('Optimal Slowstart Value', fontsize=13, fontweight='bold')
    ax2.set_title('(b) Optimal Slowstart by Data Type', fontsize=14, fontweight='bold')
    ax2.set_xticks(x)
    ax2.set_xticklabels(['Skewed', 'Uniform'], fontsize=12)
    ax2.set_ylim([0, 1.1])
    ax2.grid(True, alpha=0.3, axis='y')
    
    # Add value labels
    for i, (bar, row) in enumerate(zip(bars, df_optimal.itertuples())):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height + 0.03,
                f'{row.optimal_slowstart:.2f}\n({row.optimal_time:.1f}s)',
                ha='center', va='bottom', fontsize=11, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file}")
    plt.close()
    
    return df_optimal

def plot_comprehensive_heatmap(stats, output_file='fig7_comprehensive_heatmap.png'):
    """
    Figure 7: Heatmap showing multiple metrics
    """
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    slowstart_values = sorted(stats['slowstart'].unique())
    
    metrics = [
        ('total_time_from_api_mean', 'Total Time (s)', axes[0, 0]),
        ('reduce_elapsed_stddev_mean', 'Reduce Time Std Dev (s)', axes[0, 1]),
        ('straggler_ratio', 'Straggler Ratio (Max/Min)', axes[1, 0]),
        ('avg_shuffle_time_mean', 'Avg Shuffle Time (s)', axes[1, 1])
    ]
    
    for metric, title, ax in metrics:
        # Create pivot table
        pivot_data = stats.pivot(index='data_type', columns='slowstart', values=metric)
        pivot_data = pivot_data.reindex(['skewed', 'uniform'])
        
        # Create heatmap
        sns.heatmap(pivot_data, annot=True, fmt='.2f', cmap='YlOrRd', ax=ax,
                   cbar_kws={'label': title}, annot_kws={'size': 10})
        ax.set_title(title, fontsize=13, fontweight='bold')
        ax.set_xlabel('Slowstart Value', fontsize=11)
        ax.set_ylabel('Data Type', fontsize=11)
        ax.set_yticklabels(['Skewed', 'Uniform'], rotation=0, fontsize=10)
    
    plt.suptitle('Task 4: Comprehensive Metrics Heatmap', fontsize=16, fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file}")
    plt.close()

def plot_slowstart_effectiveness(stats, output_file='fig8_slowstart_effectiveness.png'):
    """
    Figure 8: Effectiveness of early reduce start in mitigating skew
    """
    fig, ax = plt.subplots(figsize=(12, 7))
    
    # Calculate improvement from early start (slowstart=0.05) vs late start (slowstart=1.0)
    skewed = stats[stats['data_type'] == 'skewed'].sort_values('slowstart')
    uniform = stats[stats['data_type'] == 'uniform'].sort_values('slowstart')
    
    # For skewed data
    skewed_early = skewed[skewed['slowstart'] == 0.05]['total_time_from_api_mean'].values[0]
    skewed_late = skewed[skewed['slowstart'] == 1.0]['total_time_from_api_mean'].values[0]
    
    # For uniform data
    uniform_early = uniform[uniform['slowstart'] == 0.05]['total_time_from_api_mean'].values[0]
    uniform_late = uniform[uniform['slowstart'] == 1.0]['total_time_from_api_mean'].values[0]
    
    # Plot time reduction
    categories = ['Skewed Data', 'Uniform Data']
    early_times = [skewed_early, uniform_early]
    late_times = [skewed_late, uniform_late]
    
    x = np.arange(len(categories))
    width = 0.35
    
    bars1 = ax.bar(x - width/2, early_times, width, label='Early Start (0.05)', 
                   color='#27ae60', alpha=0.85, edgecolor='black', linewidth=0.8)
    bars2 = ax.bar(x + width/2, late_times, width, label='Late Start (1.00)',
                   color='#c0392b', alpha=0.85, edgecolor='black', linewidth=0.8)
    
    ax.set_xlabel('Data Type', fontsize=14, fontweight='bold')
    ax.set_ylabel('Total Execution Time (s)', fontsize=14, fontweight='bold')
    ax.set_title('Task 4: Early vs Late Reduce Start Effectiveness', 
                fontsize=16, fontweight='bold', pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(categories, fontsize=12)
    ax.legend(fontsize=12)
    ax.grid(True, alpha=0.3, axis='y')
    
    # Add improvement percentages
    for i, (early, late) in enumerate(zip(early_times, late_times)):
        if late > early:
            improvement = (late - early) / late * 100
            ax.annotate(f'-{improvement:.1f}%', xy=(i - width/2, early), 
                       xytext=(i - width/2, early - 3),
                       fontsize=11, ha='center', fontweight='bold', color='#27ae60')
        else:
            increase = (early - late) / late * 100
            ax.annotate(f'+{increase:.1f}%', xy=(i - width/2, early), 
                       xytext=(i - width/2, early + 1),
                       fontsize=11, ha='center', fontweight='bold', color='#c0392b')
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file}")
    plt.close()

def create_summary_tables(df, stats):
    """Create summary tables as per experiment requirements."""
    
    # Table 6: Skewed data detailed results
    skewed_df = df[df['data_type'] == 'skewed'][['slowstart', 'run_number', 
                                                   'total_time_from_api', 'map_completion_time',
                                                   'min_reduce_elapsed', 'avg_reduce_elapsed',
                                                   'max_reduce_elapsed', 'reduce_elapsed_stddev']].copy()
    skewed_df.columns = ['Slowstart', 'Run', 'Total Time (s)', 'Map Complete (s)',
                         'Min Reduce (s)', 'Avg Reduce (s)', 'Max Reduce (s)', 'Reduce Stddev (s)']
    skewed_df = skewed_df.round(2).sort_values(['Slowstart', 'Run'])
    
    # Table 7: Uniform data detailed results
    uniform_df = df[df['data_type'] == 'uniform'][['slowstart', 'run_number',
                                                     'total_time_from_api', 'map_completion_time',
                                                     'min_reduce_elapsed', 'avg_reduce_elapsed',
                                                     'max_reduce_elapsed', 'reduce_elapsed_stddev']].copy()
    uniform_df.columns = ['Slowstart', 'Run', 'Total Time (s)', 'Map Complete (s)',
                          'Min Reduce (s)', 'Avg Reduce (s)', 'Max Reduce (s)', 'Reduce Stddev (s)']
    uniform_df = uniform_df.round(2).sort_values(['Slowstart', 'Run'])
    
    # Table 8: Summary comparison table
    summary_data = []
    for data_type in ['skewed', 'uniform']:
        for ss in sorted(stats['slowstart'].unique()):
            row = stats[(stats['data_type'] == data_type) & (stats['slowstart'] == ss)].iloc[0]
            summary_data.append({
                'Data Type': 'Skewed' if data_type == 'skewed' else 'Uniform',
                'Slowstart': ss,
                'Avg Total Time (s)': round(row['total_time_from_api_mean'], 2),
                'Reduce Stddev (s)': round(row['reduce_elapsed_stddev_mean'], 2),
                'Straggler Ratio': round(row['straggler_ratio'], 2),
                'Max Reduce (s)': round(row['max_reduce_elapsed_mean'], 2),
                'Min Reduce (s)': round(row['min_reduce_elapsed_mean'], 2)
            })
    
    summary_df = pd.DataFrame(summary_data)
    
    return skewed_df, uniform_df, summary_df

def main():
    print("="*80)
    print("Task 4: Data Skew Testing Analysis")
    print("="*80)
    
    # Load data
    print("\n[1/6] Loading experimental data...")
    df, config = load_data(RESULTS_FILE)
    print(f"  ✓ Loaded {len(df)} experimental runs")
    print(f"  ✓ Data types: {df['data_type'].unique().tolist()}")
    print(f"  ✓ Slowstart values: {sorted(df['slowstart'].unique())}")
    print(f"  ✓ Reducers: {config['num_reducers']}")
    
    # Compute statistics
    print("\n[2/6] Computing statistics...")
    stats = compute_statistics(df)
    stats = compute_derived_metrics(stats)
    print(f"  ✓ Computed statistics for {len(stats)} configurations")
    
    # Create tables
    print("\n[3/6] Generating summary tables...")
    skewed_table, uniform_table, summary_table = create_summary_tables(df, stats)
    skewed_table.to_csv('table6_skewed_data.csv', index=False)
    uniform_table.to_csv('table7_uniform_data.csv', index=False)
    summary_table.to_csv('table8_summary_comparison.csv', index=False)
    print(f"  ✓ Saved: table6_skewed_data.csv")
    print(f"  ✓ Saved: table7_uniform_data.csv")
    print(f"  ✓ Saved: table8_summary_comparison.csv")
    
    # Generate figures
    print("\n[4/6] Generating figures...")
    plot_total_time_comparison(stats)
    plot_reduce_stddev_comparison(stats)
    plot_straggler_analysis(stats)
    plot_reduce_time_distribution(df)
    plot_shuffle_phase_analysis(stats)
    optimal_df = plot_optimal_slowstart(stats)
    plot_comprehensive_heatmap(stats)
    plot_slowstart_effectiveness(stats)
    
    # Print key findings
    print("\n" + "="*80)
    print("Key Findings:")
    print("="*80)
    
    print("\n1. Optimal Slowstart by Data Type:")
    for _, row in optimal_df.iterrows():
        dtype = 'Skewed' if row['data_type'] == 'skewed' else 'Uniform'
        print(f"   {dtype:8s}: slowstart = {row['optimal_slowstart']:.2f}, "
              f"time = {row['optimal_time']:.2f}s, stddev = {row['optimal_stddev']:.2f}s")
    
    print("\n2. Data Skew Impact Analysis:")
    for ss in [0.05, 0.50, 1.00]:
        skewed_row = stats[(stats['data_type'] == 'skewed') & (stats['slowstart'] == ss)].iloc[0]
        uniform_row = stats[(stats['data_type'] == 'uniform') & (stats['slowstart'] == ss)].iloc[0]
        time_diff = skewed_row['total_time_from_api_mean'] - uniform_row['total_time_from_api_mean']
        time_pct = time_diff / uniform_row['total_time_from_api_mean'] * 100
        stddev_ratio = skewed_row['reduce_elapsed_stddev_mean'] / uniform_row['reduce_elapsed_stddev_mean']
        print(f"   slowstart={ss:.2f}: Skewed is {time_pct:+.1f}% slower, "
              f"Stddev ratio: {stddev_ratio:.2f}x")
    
    print("\n3. Straggler Analysis (slowstart=0.05):")
    for dtype in ['skewed', 'uniform']:
        row = stats[(stats['data_type'] == dtype) & (stats['slowstart'] == 0.05)].iloc[0]
        print(f"   {dtype.capitalize():8s}: Straggler ratio = {row['straggler_ratio']:.2f}, "
              f"Delay = {row['straggler_delay']:.2f}s")
    
    print("\n4. Early Start Effectiveness:")
    for dtype in ['skewed', 'uniform']:
        early = stats[(stats['data_type'] == dtype) & (stats['slowstart'] == 0.05)]['total_time_from_api_mean'].values[0]
        late = stats[(stats['data_type'] == dtype) & (stats['slowstart'] == 1.0)]['total_time_from_api_mean'].values[0]
        improvement = (late - early) / late * 100
        print(f"   {dtype.capitalize():8s}: Early start saves {improvement:.1f}% ({late-early:.2f}s)")
    
    print("\n" + "="*80)
    print("✓ Analysis complete! All figures and tables saved.")
    print("="*80)

if __name__ == '__main__':
    main()

