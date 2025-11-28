#!/usr/bin/env python3
"""
Generate skewed and uniform data for Task 4: Data Skew Testing.

This script generates two types of datasets:
1. Skewed Data: One hotkey ("hotkey") accounts for 60% of all words
2. Uniform Data: Words are uniformly distributed (control group)

Both datasets are 1GB in size for fair comparison.
"""

import random
import os
import sys

def generate_skewed_data(file_path, size_gb=1.0, hotkey_ratio=0.6):
    """
    Generate a text file with skewed key distribution.
    
    Args:
        file_path: Path to output file
        size_gb: Target file size in GB
        hotkey_ratio: Ratio of the hotkey (e.g., 0.6 means 60%)
    """
    target_size = int(size_gb * 1024 * 1024 * 1024)  # Convert to bytes
    
    # Generate a pool of normal words
    normal_words = [f"word{i:05d}" for i in range(10000)]
    hotkey = "hotkey"
    
    print(f"Generating {size_gb:.2f}GB of SKEWED data to {file_path}...")
    print(f"  Hotkey: '{hotkey}' (ratio: {hotkey_ratio*100:.0f}%)")
    print(f"  Normal words: {len(normal_words)} unique words")
    print(f"  Target size: {target_size:,} bytes")
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    # Calculate word counts
    # Assuming average word length + space is ~10 bytes
    total_words = target_size // 10
    hotkey_count = int(total_words * hotkey_ratio)
    normal_count = total_words - hotkey_count
    
    print(f"  Estimated words: {total_words:,}")
    print(f"    - Hotkey occurrences: {hotkey_count:,}")
    print(f"    - Normal words: {normal_count:,}")
    
    current_size = 0
    words_written = 0
    hotkey_written = 0
    
    # Pre-generate a shuffled list to ensure proper distribution
    word_list = [hotkey] * hotkey_count + [random.choice(normal_words) for _ in range(normal_count)]
    random.shuffle(word_list)
    
    print("\n  Writing data...")
    with open(file_path, 'w') as f:
        words_per_line = 10
        line_words = []
        
        for word in word_list:
            line_words.append(word)
            if word == hotkey:
                hotkey_written += 1
            words_written += 1
            
            if len(line_words) >= words_per_line:
                line = ' '.join(line_words) + '\n'
                f.write(line)
                current_size += len(line)
                line_words = []
                
                # Print progress every 100MB
                if current_size % (100 * 1024 * 1024) < 200:
                    progress = current_size / target_size * 100
                    print(f"    Progress: {progress:.1f}% ({current_size / (1024*1024*1024):.2f}GB)")
        
        # Write remaining words
        if line_words:
            line = ' '.join(line_words) + '\n'
            f.write(line)
            current_size += len(line)
    
    final_size_gb = current_size / (1024 * 1024 * 1024)
    final_size_mb = current_size / (1024 * 1024)
    actual_hotkey_ratio = hotkey_written / words_written if words_written > 0 else 0
    
    print(f"\n  ✓ Skewed data generation complete!")
    print(f"    File size: {final_size_gb:.2f}GB ({final_size_mb:.1f}MB)")
    print(f"    Total words: {words_written:,}")
    print(f"    Hotkey occurrences: {hotkey_written:,} ({actual_hotkey_ratio*100:.1f}%)")
    print(f"    File: {file_path}")

def generate_uniform_data(file_path, size_gb=1.0):
    """
    Generate a text file with uniform key distribution (control group).
    
    Args:
        file_path: Path to output file
        size_gb: Target file size in GB
    """
    target_size = int(size_gb * 1024 * 1024 * 1024)  # Convert to bytes
    
    # Generate a pool of words - same size as skewed version
    words_pool = [f"word{i:05d}" for i in range(10000)]
    
    print(f"Generating {size_gb:.2f}GB of UNIFORM data to {file_path}...")
    print(f"  Word pool: {len(words_pool)} unique words")
    print(f"  Target size: {target_size:,} bytes")
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    current_size = 0
    words_written = 0
    words_per_line = 10
    
    print("\n  Writing data...")
    with open(file_path, 'w') as f:
        while current_size < target_size:
            # Generate a line with random words (uniformly distributed)
            line_words = [random.choice(words_pool) for _ in range(words_per_line)]
            line = ' '.join(line_words) + '\n'
            f.write(line)
            current_size += len(line)
            words_written += words_per_line
            
            # Print progress every 100MB
            if current_size % (100 * 1024 * 1024) < 200:
                progress = current_size / target_size * 100
                print(f"    Progress: {progress:.1f}% ({current_size / (1024*1024*1024):.2f}GB)")
    
    final_size_gb = current_size / (1024 * 1024 * 1024)
    final_size_mb = current_size / (1024 * 1024)
    
    print(f"\n  ✓ Uniform data generation complete!")
    print(f"    File size: {final_size_gb:.2f}GB ({final_size_mb:.1f}MB)")
    print(f"    Total words: {words_written:,}")
    print(f"    File: {file_path}")

def main():
    """Generate both skewed and uniform datasets for Task 4."""
    print("="*80)
    print("Task 4: Data Skew Testing - Data Generation")
    print("="*80)
    print()
    print("This script will generate two datasets:")
    print("  1. Skewed Data:  1GB with 60% hotkey concentration")
    print("  2. Uniform Data: 1GB with uniform distribution (control)")
    print()
    
    data_dir = '/root/Exp-hadoop/EXP/task4/data'
    skewed_file = os.path.join(data_dir, 'input_skewed_1gb.txt')
    uniform_file = os.path.join(data_dir, 'input_uniform_1gb.txt')
    
    # Check existing files
    existing_files = []
    for file_path in [skewed_file, uniform_file]:
        if os.path.exists(file_path):
            size_gb = os.path.getsize(file_path) / (1024 * 1024 * 1024)
            existing_files.append((os.path.basename(file_path), size_gb))
    
    if existing_files:
        print("Existing files:")
        for filename, size_gb in existing_files:
            print(f"  - {filename}: {size_gb:.2f}GB")
        print()
        user_input = input("Regenerate all files? (y/N): ").strip().lower()
        if user_input != 'y':
            print("Using existing files.")
            return
        print()
    
    # Generate skewed data
    print("-"*80)
    print("STEP 1: Generating Skewed Data")
    print("-"*80)
    generate_skewed_data(skewed_file, size_gb=1.0, hotkey_ratio=0.6)
    
    print()
    print("-"*80)
    print("STEP 2: Generating Uniform Data (Control)")
    print("-"*80)
    generate_uniform_data(uniform_file, size_gb=1.0)
    
    print()
    print("="*80)
    print("✓ Data generation complete!")
    print("="*80)
    print()
    print("Generated files:")
    for file_path in [skewed_file, uniform_file]:
        if os.path.exists(file_path):
            size_gb = os.path.getsize(file_path) / (1024 * 1024 * 1024)
            size_mb = os.path.getsize(file_path) / (1024 * 1024)
            print(f"  - {os.path.basename(file_path)}: {size_gb:.2f}GB ({size_mb:.1f}MB)")
    print()
    print("Next step: Compile WordCount and run experiments")
    print("  ./compile.sh")
    print("  python3 scripts/run_experiment.py")
    print()

if __name__ == '__main__':
    main()

