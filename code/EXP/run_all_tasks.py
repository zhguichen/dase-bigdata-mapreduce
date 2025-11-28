#!/usr/bin/env python3
"""
主脚本：依次运行所有task的实验
依次调用task1-task4的run_experiment.py脚本
"""

import subprocess
import sys
import os
from datetime import datetime

# 获取脚本所在目录
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# 定义各个task的脚本路径
TASKS = [
    {
        'name': 'Task 1',
        'script': os.path.join(SCRIPT_DIR, 'task1', 'scripts', 'run_experiment.py')
    },
    {
        'name': 'Task 2',
        'script': os.path.join(SCRIPT_DIR, 'task2', 'scripts', 'run_experiment.py')
    },
    {
        'name': 'Task 3',
        'script': os.path.join(SCRIPT_DIR, 'task3', 'scripts', 'run_experiment.py')
    },
    {
        'name': 'Task 4',
        'script': os.path.join(SCRIPT_DIR, 'task4', 'scripts', 'run_experiment.py')
    }
]


def run_task(task_info):
    """运行单个task的实验脚本"""
    task_name = task_info['name']
    script_path = task_info['script']
    
    print("\n" + "="*80)
    print(f"开始运行: {task_name}")
    print(f"脚本路径: {script_path}")
    print("="*80)
    
    # 检查脚本是否存在
    if not os.path.exists(script_path):
        print(f"✗ 错误: 脚本不存在: {script_path}")
        return False
    
    # 确保脚本有执行权限
    os.chmod(script_path, 0o755)
    
    # 运行脚本
    try:
        start_time = datetime.now()
        result = subprocess.run(
            [sys.executable, script_path],
            cwd=os.path.dirname(script_path),
            check=True
        )
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print(f"\n✓ {task_name} 完成 (耗时: {duration:.2f}秒)")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"\n✗ {task_name} 执行失败，退出码: {e.returncode}")
        return False
    except KeyboardInterrupt:
        print(f"\n\n✗ {task_name} 被用户中断")
        return False
    except Exception as e:
        print(f"\n✗ {task_name} 执行出错: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """主函数：依次运行所有task"""
    print("\n" + "="*80)
    print("Hadoop MapReduce 实验 - 批量运行所有Task")
    print("="*80)
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"总共需要运行 {len(TASKS)} 个task")
    
    results = []
    overall_start_time = datetime.now()
    
    for i, task_info in enumerate(TASKS, 1):
        print(f"\n[{i}/{len(TASKS)}] 准备运行 {task_info['name']}")
        
        success = run_task(task_info)
        results.append({
            'task': task_info['name'],
            'success': success
        })
        
        if not success:
            print(f"\n⚠ 警告: {task_info['name']} 执行失败")
            response = input("\n是否继续运行下一个task? (y/n): ").strip().lower()
            if response != 'y':
                print("\n用户选择停止执行")
                break
    
    overall_end_time = datetime.now()
    overall_duration = (overall_end_time - overall_start_time).total_seconds()
    
    # 打印总结
    print("\n" + "="*80)
    print("执行总结")
    print("="*80)
    print(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"总耗时: {overall_duration:.2f}秒 ({overall_duration/60:.2f}分钟)")
    print("\n各Task执行结果:")
    for result in results:
        status = "✓ 成功" if result['success'] else "✗ 失败"
        print(f"  {result['task']}: {status}")
    
    # 统计成功和失败的数量
    success_count = sum(1 for r in results if r['success'])
    fail_count = len(results) - success_count
    
    print(f"\n成功: {success_count}/{len(results)}, 失败: {fail_count}/{len(results)}")
    
    if fail_count > 0:
        print("\n⚠ 部分task执行失败，请检查日志")
        sys.exit(1)
    else:
        print("\n✓ 所有task执行成功！")
        sys.exit(0)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n✗ 程序被用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ 程序执行出错: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

