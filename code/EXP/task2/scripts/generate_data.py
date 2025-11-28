#!/usr/bin/env python3
"""
Generate text data for Task 2 scalability testing.
Generates 500MB, 1GB, and 2GB text files.
"""

import random
import os
import sys

def generate_random_text(file_path, size_gb=1.0):
    """
    Generate a text file with random words.
    
    Args:
        file_path: Path to output file
        size_gb: Target file size in GB
    """
    target_size = int(size_gb * 1024 * 1024 * 1024)  # Convert to bytes
    
    # Common English words to make it more realistic
    common_words = [
        'the', 'be', 'to', 'of', 'and', 'a', 'in', 'that', 'have', 'i',
        'it', 'for', 'not', 'on', 'with', 'he', 'as', 'you', 'do', 'at',
        'this', 'but', 'his', 'by', 'from', 'they', 'we', 'say', 'her', 'she',
        'or', 'an', 'will', 'my', 'one', 'all', 'would', 'there', 'their',
        'what', 'so', 'up', 'out', 'if', 'about', 'who', 'get', 'which', 'go',
        'me', 'when', 'make', 'can', 'like', 'time', 'no', 'just', 'him', 'know',
        'take', 'people', 'into', 'year', 'your', 'good', 'some', 'could', 'them',
        'see', 'other', 'than', 'then', 'now', 'look', 'only', 'come', 'its', 'over',
        'think', 'also', 'back', 'after', 'use', 'two', 'how', 'our', 'work', 'first',
        'well', 'way', 'even', 'new', 'want', 'because', 'any', 'these', 'give', 'day',
        'most', 'us', 'hadoop', 'mapreduce', 'data', 'processing', 'cluster', 'node',
        'compute', 'storage', 'distributed', 'system', 'task', 'job', 'reduce', 'map',
        'parallel', 'scale', 'performance', 'throughput', 'latency', 'network', 'disk'
    ]
    
    words_per_line = 10
    current_size = 0
    
    print(f"Generating {size_gb:.2f}GB of text data to {file_path}...")
    print(f"Target size: {target_size:,} bytes")
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    with open(file_path, 'w') as f:
        while current_size < target_size:
            # Generate a line with random words
            line_words = [random.choice(common_words) for _ in range(words_per_line)]
            line = ' '.join(line_words) + '\n'
            f.write(line)
            current_size += len(line)
            
            # Print progress every 100MB
            if current_size % (100 * 1024 * 1024) < len(line):
                progress = current_size / target_size * 100
                print(f"  Progress: {progress:.1f}% ({current_size / (1024*1024*1024):.2f}GB)")
    
    final_size_gb = current_size / (1024 * 1024 * 1024)
    final_size_mb = current_size / (1024 * 1024)
    print(f"✓ Data generation complete! Final size: {final_size_gb:.2f}GB ({final_size_mb:.1f}MB)")
    print(f"  File: {file_path}")

def main():
    """Generate all required data files for Task 2."""
    print("="*80)
    print("Task 2: Data Generation for Scalability Testing")
    print("="*80)
    print()
    
    data_dir = '/root/Exp-hadoop/EXP/task2/data'
    
    # Define data sizes
    data_files = [
        ('input_500mb.txt', 0.5),
        ('input_1gb.txt', 1.0),
        ('input_1500mb.txt', 1.5),
        # ('input_2gb.txt', 2.0),
    ]
    
    for filename, size_gb in data_files:
        file_path = os.path.join(data_dir, filename)
        
        # Check if file already exists
        if os.path.exists(file_path):
            existing_size_gb = os.path.getsize(file_path) / (1024 * 1024 * 1024)
            print(f"File {filename} already exists ({existing_size_gb:.2f}GB)")
            user_input = input(f"  Regenerate? (y/N): ").strip().lower()
            if user_input != 'y':
                print(f"  Skipping {filename}")
                print()
                continue
        
        print(f"\nGenerating {filename} ({size_gb:.2f}GB)...")
        print("-"*80)
        generate_random_text(file_path, size_gb)
        print()
    
    print("="*80)
    print("✓ All data files generated successfully!")
    print("="*80)
    print()
    print("Generated files:")
    for filename, _ in data_files:
        file_path = os.path.join(data_dir, filename)
        if os.path.exists(file_path):
            size_gb = os.path.getsize(file_path) / (1024 * 1024 * 1024)
            size_mb = os.path.getsize(file_path) / (1024 * 1024)
            print(f"  - {filename}: {size_gb:.2f}GB ({size_mb:.1f}MB)")
    print()

if __name__ == '__main__':
    main()

