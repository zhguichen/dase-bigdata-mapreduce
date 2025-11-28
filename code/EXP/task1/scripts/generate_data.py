#!/usr/bin/env python3
"""
Generate text data for WordCount testing.
Generates approximately 1GB of random text data.
"""

import random
import string
import sys

def generate_random_text(file_path, size_gb=0.5):
    """
    Generate a text file with random words.
    
    Args:
        file_path: Path to output file
        size_gb: Target file size in GB
    """
    target_size = size_gb * 1024 * 1024 * 1024  # Convert to bytes
    
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
        'compute', 'storage', 'distributed', 'system', 'task', 'job', 'reduce', 'map'
    ]
    
    words_per_line = 10
    current_size = 0
    
    print(f"Generating {size_gb}GB of text data to {file_path}...")
    
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
                print(f"Progress: {progress:.1f}% ({current_size / (1024*1024*1024):.2f}GB)")
    
    final_size_gb = current_size / (1024 * 1024 * 1024)
    print(f"Data generation complete! Final size: {final_size_gb:.2f}GB")

if __name__ == '__main__':
    output_file = '/root/Exp-hadoop/EXP/task1/data/input_500mb.txt'
    if len(sys.argv) > 1:
        output_file = sys.argv[1]
    
    generate_random_text(output_file, size_gb=0.5)


