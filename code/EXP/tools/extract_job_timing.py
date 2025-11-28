#!/usr/bin/env python3
"""
自动化工具：从 Hadoop JobHistory Server 提取作业时间信息

提取以下关键时间点：
1. Map 阶段完成时间点（最后一个 Map task 完成的时间）
2. 第一个 Reduce 启动时间点（第一个 Reduce task 开始的时间）
3. 所有 Reduce 完成时间点（最后一个 Reduce task 完成的时间）

使用方式：
    python3 extract_job_timing.py <job_id>
    python3 extract_job_timing.py job_1764138085950_0002

或从结果文件批量处理：
    python3 extract_job_timing.py --batch <results_json_file>
"""

import requests
import json
import sys
from datetime import datetime
from pathlib import Path

# JobHistory Server 配置
JOBHISTORY_HOST = "172.31.12.133"
JOBHISTORY_PORT = "19888"
JOBHISTORY_API_BASE = f"http://{JOBHISTORY_HOST}:{JOBHISTORY_PORT}/ws/v1/history/mapreduce"


class JobTimingExtractor:
    """作业时间信息提取器"""
    
    def __init__(self, job_id):
        self.job_id = job_id
        self.job_info = None
        self.tasks = None
        self.counters = []
        
    def fetch_job_info(self):
        """获取作业基本信息"""
        url = f"{JOBHISTORY_API_BASE}/jobs/{self.job_id}"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            self.job_info = data.get('job', {})
            return True
        except Exception as e:
            print(f"错误：无法获取作业信息 - {e}")
            return False
    
    def fetch_tasks(self):
        """获取作业的所有 tasks"""
        url = f"{JOBHISTORY_API_BASE}/jobs/{self.job_id}/tasks"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            self.tasks = data.get('tasks', {}).get('task', [])
            return True
        except Exception as e:
            print(f"错误：无法获取任务信息 - {e}")
            return False
    
    def fetch_task_attempts(self, task_id):
        """获取单个task的attempt详细信息"""
        url = f"{JOBHISTORY_API_BASE}/jobs/{self.job_id}/tasks/{task_id}/attempts"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            attempts = data.get('taskAttempts', {}).get('taskAttempt', [])
            # 返回成功的attempt
            for attempt in attempts:
                if attempt.get('state') == 'SUCCEEDED':
                    return attempt
            return None
        except Exception as e:
            return None
    
    def fetch_counters(self):
        """获取作业的 Counters 信息"""
        url = f"{JOBHISTORY_API_BASE}/jobs/{self.job_id}/counters"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            self.counters = data.get('jobCounters', {}).get('counterGroup', [])
            return True
        except Exception as e:
            print(f"警告：无法获取 Counters 信息 - {e}")
            self.counters = []
            return False
    
    def get_counter_value(self, group_name, counter_name):
        """从 counters 中获取特定值"""
        if not self.counters:
            return None
        
        for group in self.counters:
            if group_name in group.get('counterGroupName', ''):
                for counter in group.get('counter', []):
                    if counter.get('name') == counter_name:
                        return counter.get('totalCounterValue', 0)
        return None
    
    def extract_timing_info(self):
        """提取关键时间点信息"""
        if not self.job_info or not self.tasks:
            return None
        
        # 作业开始时间（用作基准）
        job_start_time = self.job_info.get('startTime', 0)
        job_finish_time = self.job_info.get('finishTime', 0)
        
        # 计算相对时间的辅助函数
        def ms_to_seconds(ms_time):
            if ms_time and job_start_time:
                return (ms_time - job_start_time) / 1000.0
            return None
        
        # 分离 MAP 和 REDUCE tasks
        map_tasks = [t for t in self.tasks if t.get('type') == 'MAP']
        reduce_tasks = [t for t in self.tasks if t.get('type') == 'REDUCE']
        
        if not map_tasks:
            print(f"警告：作业 {self.job_id} 没有 MAP tasks")
            return None
        
        # 1. Map 阶段完成时间点（最后一个 Map task 完成的时间）
        map_finish_times = [t.get('finishTime', 0) for t in map_tasks]
        map_completion_time = max(map_finish_times) if map_finish_times else 0
        
        # 2. 第一个 Reduce 启动时间点（如果有 reduce tasks）
        first_reduce_start_time = 0
        reduce_completion_time = 0
        
        if reduce_tasks:
            reduce_start_times = [t.get('startTime', 0) for t in reduce_tasks]
            first_reduce_start_time = min(reduce_start_times) if reduce_start_times else 0
            
            # 3. 所有 Reduce 完成时间点（最后一个 Reduce task 完成的时间）
            reduce_finish_times = [t.get('finishTime', 0) for t in reduce_tasks]
            reduce_completion_time = max(reduce_finish_times) if reduce_finish_times else 0
            
            # 4. 每个 Reduce 任务的详细信息（直接使用API提供的elapsedTime）
            reduce_details = []  # 存储每个reduce的详细信息
            reduce_elapsed_times = []  # 每个reduce的总耗时（API已计算）
            reduce_absolute_times = []  # 绝对完成时间戳
            reduce_shuffle_times = []  # shuffle耗时
            reduce_merge_times = []    # merge耗时
            reduce_reduce_times = []   # reduce计算耗时
            
            for t in reduce_tasks:
                # 使用API提供的elapsedTime（已经计算好的）
                elapsed = t.get('elapsedTime', 0)  # 毫秒
                finish_time = t.get('finishTime', 0)
                task_id = t.get('id', '')
                
                if elapsed and finish_time:
                    elapsed_sec = elapsed / 1000.0
                    reduce_elapsed_times.append(elapsed_sec)
                    reduce_absolute_times.append(finish_time)
                    
                    # 获取attempt级别的详细时间
                    attempt = self.fetch_task_attempts(task_id)
                    reduce_detail = {
                        'task_id': task_id,
                        'elapsed_time': round(elapsed_sec, 2),
                        'finish_time': round(ms_to_seconds(finish_time), 2) if job_start_time else None,
                    }
                    
                    if attempt:
                        shuffle_time = attempt.get('elapsedShuffleTime', 0) / 1000.0
                        merge_time = attempt.get('elapsedMergeTime', 0) / 1000.0
                        reduce_time = attempt.get('elapsedReduceTime', 0) / 1000.0
                        
                        reduce_shuffle_times.append(shuffle_time)
                        reduce_merge_times.append(merge_time)
                        reduce_reduce_times.append(reduce_time)
                        
                        reduce_detail['shuffle_time'] = round(shuffle_time, 2)
                        reduce_detail['merge_time'] = round(merge_time, 2)
                        reduce_detail['reduce_time'] = round(reduce_time, 2)
                    
                    reduce_details.append(reduce_detail)
            
            # 统计信息（使用API提供的elapsedTime）
            min_reduce_elapsed = min(reduce_elapsed_times) if reduce_elapsed_times else None
            max_reduce_elapsed = max(reduce_elapsed_times) if reduce_elapsed_times else None
            avg_reduce_elapsed = sum(reduce_elapsed_times) / len(reduce_elapsed_times) if reduce_elapsed_times else None
            
            # 统计信息（绝对完成时间，相对于作业开始）
            min_reduce_finish_abs = min(reduce_absolute_times) if reduce_absolute_times else None
            max_reduce_finish_abs = max(reduce_absolute_times) if reduce_absolute_times else None
            
            # 计算标准差
            reduce_time_stddev = None
            if reduce_elapsed_times and len(reduce_elapsed_times) > 1:
                import statistics
                reduce_time_stddev = statistics.stdev(reduce_elapsed_times)
            
            # 阶段时间的统计
            min_shuffle_time = min(reduce_shuffle_times) if reduce_shuffle_times else None
            max_shuffle_time = max(reduce_shuffle_times) if reduce_shuffle_times else None
            avg_shuffle_time_detail = sum(reduce_shuffle_times) / len(reduce_shuffle_times) if reduce_shuffle_times else None
            
            min_merge_time = min(reduce_merge_times) if reduce_merge_times else None
            max_merge_time = max(reduce_merge_times) if reduce_merge_times else None
            avg_merge_time_detail = sum(reduce_merge_times) / len(reduce_merge_times) if reduce_merge_times else None
            
            min_reduce_time_detail = min(reduce_reduce_times) if reduce_reduce_times else None
            max_reduce_time_detail = max(reduce_reduce_times) if reduce_reduce_times else None
            avg_reduce_time_detail = sum(reduce_reduce_times) / len(reduce_reduce_times) if reduce_reduce_times else None
        
        # 计算相对时间（秒）
        def ms_to_seconds(ms_time):
            if ms_time and job_start_time:
                return (ms_time - job_start_time) / 1000.0
            return None
        
        total_time = (job_finish_time - job_start_time) / 1000.0 if job_finish_time and job_start_time else 0
        
        # 从job_info中提取overview信息
        job_name = self.job_info.get('name', '')
        state = self.job_info.get('state', '')
        uberized = self.job_info.get('uberized', False)
        submit_time = self.job_info.get('submitTime', 0)
        
        # 平均时间（毫秒）
        avg_map_time = self.job_info.get('avgMapTime', 0)
        avg_shuffle_time = self.job_info.get('avgShuffleTime', 0)
        avg_merge_time = self.job_info.get('avgMergeTime', 0)
        avg_reduce_time = self.job_info.get('avgReduceTime', 0)
        
        # 从 Counters 提取信息
        total_map_time = self.get_counter_value('JobCounter', 'MILLIS_MAPS')
        total_reduce_time = self.get_counter_value('JobCounter', 'MILLIS_REDUCES')
        
        cpu_time = self.get_counter_value('TaskCounter', 'CPU_MILLISECONDS')
        gc_time = self.get_counter_value('TaskCounter', 'GC_TIME_MILLIS')
        
        physical_memory = self.get_counter_value('TaskCounter', 'PHYSICAL_MEMORY_BYTES')
        virtual_memory = self.get_counter_value('TaskCounter', 'VIRTUAL_MEMORY_BYTES')
        committed_heap = self.get_counter_value('TaskCounter', 'COMMITTED_HEAP_BYTES')
        peak_map_physical = self.get_counter_value('TaskCounter', 'MAP_PHYSICAL_MEMORY_BYTES_MAX')
        peak_reduce_physical = self.get_counter_value('TaskCounter', 'REDUCE_PHYSICAL_MEMORY_BYTES_MAX')
        peak_map_virtual = self.get_counter_value('TaskCounter', 'MAP_VIRTUAL_MEMORY_BYTES_MAX')
        peak_reduce_virtual = self.get_counter_value('TaskCounter', 'REDUCE_VIRTUAL_MEMORY_BYTES_MAX')
        
        hdfs_bytes_read = self.get_counter_value('FileSystemCounter', 'HDFS_BYTES_READ')
        hdfs_bytes_written = self.get_counter_value('FileSystemCounter', 'HDFS_BYTES_WRITTEN')
        file_bytes_read = self.get_counter_value('FileSystemCounter', 'FILE_BYTES_READ')
        file_bytes_written = self.get_counter_value('FileSystemCounter', 'FILE_BYTES_WRITTEN')
        
        map_input_records = self.get_counter_value('TaskCounter', 'MAP_INPUT_RECORDS')
        map_input_bytes = self.get_counter_value('FileInputFormatCounter', 'BYTES_READ')
        map_output_records = self.get_counter_value('TaskCounter', 'MAP_OUTPUT_RECORDS')
        map_output_bytes = self.get_counter_value('TaskCounter', 'MAP_OUTPUT_BYTES')
        
        reduce_shuffle_bytes = self.get_counter_value('TaskCounter', 'REDUCE_SHUFFLE_BYTES')
        reduce_input_records = self.get_counter_value('TaskCounter', 'REDUCE_INPUT_RECORDS')
        reduce_input_groups = self.get_counter_value('TaskCounter', 'REDUCE_INPUT_GROUPS')
        reduce_output_records = self.get_counter_value('TaskCounter', 'REDUCE_OUTPUT_RECORDS')
        shuffled_maps = self.get_counter_value('TaskCounter', 'SHUFFLED_MAPS')
        
        timing_info = {
            'job_id': self.job_id,
            
            # Overview 信息
            'job_name': job_name,
            'state': state,
            'uberized': uberized,
            
            # 时间戳（毫秒）
            'submit_time': submit_time,
            'job_start_time': job_start_time,
            'job_finish_time': job_finish_time,
            'total_time': round(total_time, 2),
            
            # 可读时间格式
            'submit_time_str': datetime.fromtimestamp(submit_time / 1000).strftime("%a %b %d %H:%M:%S CST %Y") if submit_time else None,
            'start_time_str': datetime.fromtimestamp(job_start_time / 1000).strftime("%a %b %d %H:%M:%S CST %Y") if job_start_time else None,
            'finish_time_str': datetime.fromtimestamp(job_finish_time / 1000).strftime("%a %b %d %H:%M:%S CST %Y") if job_finish_time else None,
            'elapsed_time_str': self._format_elapsed_time(total_time),
            
            # 平均时间（秒）
            'avg_map_time': round(avg_map_time / 1000.0, 2) if avg_map_time else 0,
            'avg_shuffle_time': round(avg_shuffle_time / 1000.0, 2) if avg_shuffle_time else 0,
            'avg_merge_time': round(avg_merge_time / 1000.0, 2) if avg_merge_time else 0,
            'avg_reduce_time': round(avg_reduce_time / 1000.0, 2) if avg_reduce_time else 0,
            
            # 作业时间统计（从 Counters 获取）
            'job_elapsed_time': round(total_time, 2),
            'total_map_time': round(total_map_time / 1000.0, 2) if total_map_time else None,
            'total_reduce_time': round(total_reduce_time / 1000.0, 2) if total_reduce_time else None,
            
            # CPU 和资源使用
            'cpu_time': round(cpu_time / 1000.0, 2) if cpu_time else None,
            'gc_time': round(gc_time / 1000.0, 2) if gc_time else None,
            
            # 内存使用
            'physical_memory_bytes': physical_memory,
            'virtual_memory_bytes': virtual_memory,
            'committed_heap_bytes': committed_heap,
            'peak_map_physical_memory': peak_map_physical,
            'peak_reduce_physical_memory': peak_reduce_physical,
            'peak_map_virtual_memory': peak_map_virtual,
            'peak_reduce_virtual_memory': peak_reduce_virtual,
            
            # 数据规模与 I/O
            'hdfs_bytes_read': hdfs_bytes_read,
            'hdfs_bytes_written': hdfs_bytes_written,
            'file_bytes_read': file_bytes_read,
            'file_bytes_written': file_bytes_written,
            'map_input_records': map_input_records,
            'map_input_bytes': map_input_bytes,
            'map_output_records': map_output_records,
            'map_output_bytes': map_output_bytes,
            
            # Shuffle 阶段
            'reduce_shuffle_bytes': reduce_shuffle_bytes,
            'reduce_input_records': reduce_input_records,
            'reduce_input_groups': reduce_input_groups,
            'reduce_output_records': reduce_output_records,
            'shuffled_maps': shuffled_maps,
            
            # 绝对时间戳（毫秒）
            'map_completion_time_abs': map_completion_time,
            'first_reduce_start_time_abs': first_reduce_start_time,
            'reduce_completion_time_abs': reduce_completion_time,
            
            # 相对时间（秒，相对于作业开始时间）
            'map_completion_time': round(ms_to_seconds(map_completion_time), 2) if map_completion_time else None,
            'first_reduce_start_time': round(ms_to_seconds(first_reduce_start_time), 2) if first_reduce_start_time else None,
            'reduce_completion_time': round(ms_to_seconds(reduce_completion_time), 2) if reduce_completion_time else None,
            
            # 任务统计
            'num_map_tasks': len(map_tasks),
            'num_reduce_tasks': len(reduce_tasks),
            
            # Reduce 任务完成时间统计（用于数据倾斜分析）
            'min_reduce_finish_time': round(ms_to_seconds(min_reduce_finish_abs), 2) if min_reduce_finish_abs else None,
            'max_reduce_finish_time': round(ms_to_seconds(max_reduce_finish_abs), 2) if max_reduce_finish_abs else None,
            'min_reduce_elapsed': round(min_reduce_elapsed, 2) if min_reduce_elapsed else None,
            'max_reduce_elapsed': round(max_reduce_elapsed, 2) if max_reduce_elapsed else None,
            'avg_reduce_elapsed': round(avg_reduce_elapsed, 2) if avg_reduce_elapsed else None,
            'reduce_elapsed_stddev': round(reduce_time_stddev, 2) if reduce_time_stddev else None,
            
            # Reduce 详细阶段时间统计（从attempt获取）
            'shuffle_time': {
                'min': round(min_shuffle_time, 2) if min_shuffle_time else None,
                'max': round(max_shuffle_time, 2) if max_shuffle_time else None,
                'avg': round(avg_shuffle_time_detail, 2) if avg_shuffle_time_detail else None
            },
            'merge_time': {
                'min': round(min_merge_time, 2) if min_merge_time else None,
                'max': round(max_merge_time, 2) if max_merge_time else None,
                'avg': round(avg_merge_time_detail, 2) if avg_merge_time_detail else None
            },
            'reduce_time': {
                'min': round(min_reduce_time_detail, 2) if min_reduce_time_detail else None,
                'max': round(max_reduce_time_detail, 2) if max_reduce_time_detail else None,
                'avg': round(avg_reduce_time_detail, 2) if avg_reduce_time_detail else None
            },
            
            # 每个Reduce的详细信息
            # 'reduce_tasks': reduce_details if reduce_details else None,
            
            # Map/Reduce完成时间可读格式
            'map_completion_datetime': datetime.fromtimestamp(map_completion_time / 1000).isoformat() if map_completion_time else None,
            'first_reduce_start_datetime': datetime.fromtimestamp(first_reduce_start_time / 1000).isoformat() if first_reduce_start_time else None,
            'reduce_completion_datetime': datetime.fromtimestamp(reduce_completion_time / 1000).isoformat() if reduce_completion_time else None,
        }
        
        return timing_info
    
    def _format_elapsed_time(self, seconds):
        """格式化耗时为易读格式"""
        if seconds < 60:
            return f"{int(seconds)}sec"
        elif seconds < 3600:
            mins = int(seconds // 60)
            secs = int(seconds % 60)
            return f"{mins}mins, {secs}sec"
        else:
            hours = int(seconds // 3600)
            mins = int((seconds % 3600) // 60)
            secs = int(seconds % 60)
            return f"{hours}hrs, {mins}mins, {secs}sec"
    
    def extract(self):
        """执行完整的提取流程"""
        print(f"正在提取作业 {self.job_id} 的时间信息...")
        
        if not self.fetch_job_info():
            return None
        
        if not self.fetch_tasks():
            return None
        
        # 获取 Counters 信息（可选，失败不影响基本时间提取）
        self.fetch_counters()
        
        timing_info = self.extract_timing_info()
        
        if timing_info:
            print(f"✓ 成功提取时间信息")
            self._print_timing_info(timing_info)
        
        return timing_info
    
    def _print_timing_info(self, info):
        """打印时间信息"""
        print(f"\n{'='*80}")
        print(f"作业时间分析: {info['job_id']}")
        print(f"{'='*80}")
        print(f"Job Name:    {info['job_name']}")
        print(f"State:       {info['state']}")
        print(f"Uberized:    {info['uberized']}")
        print(f"\n时间信息:")
        print(f"  Submitted: {info['submit_time_str']}")
        print(f"  Started:   {info['start_time_str']}")
        print(f"  Finished:  {info['finish_time_str']}")
        print(f"  Elapsed:   {info['elapsed_time_str']}")
        print(f"\n任务统计:")
        print(f"  Map Tasks:    {info['num_map_tasks']}")
        print(f"  Reduce Tasks: {info['num_reduce_tasks']}")
        print(f"\n平均时间:")
        print(f"  Average Map Time:     {info['avg_map_time']:.2f}sec")
        print(f"  Average Shuffle Time: {info['avg_shuffle_time']:.2f}sec")
        print(f"  Average Merge Time:   {info['avg_merge_time']:.2f}sec")
        print(f"  Average Reduce Time:  {info['avg_reduce_time']:.2f}sec")
        
        # CPU 和资源使用
        if info.get('cpu_time') is not None:
            print(f"\nCPU 和资源:")
            print(f"  CPU Time:             {info['cpu_time']:.2f}sec")
            print(f"  GC Time:              {info['gc_time']:.2f}sec")
        
        # 数据规模
        if info.get('hdfs_bytes_read') is not None:
            print(f"\n数据规模:")
            print(f"  HDFS Read:            {info['hdfs_bytes_read']:,} bytes ({info['hdfs_bytes_read']/1024/1024:.2f} MB)")
            print(f"  HDFS Written:         {info['hdfs_bytes_written']:,} bytes ({info['hdfs_bytes_written']/1024/1024:.2f} MB)")
            print(f"  Map Input Records:    {info['map_input_records']:,}")
            print(f"  Reduce Output Records: {info['reduce_output_records']:,}")
            print(f"  Reduce Shuffle Bytes: {info['reduce_shuffle_bytes']:,} bytes ({info['reduce_shuffle_bytes']/1024/1024:.2f} MB)")
        print(f"\n关键时间点（相对于作业开始时间）:")
        print(f"  Map 阶段完成时间:      {info['map_completion_time']:.2f}秒")
        if info['first_reduce_start_time']:
            print(f"  第一个 Reduce 启动时间: {info['first_reduce_start_time']:.2f}秒")
            print(f"  所有 Reduce 完成时间:   {info['reduce_completion_time']:.2f}秒")
        
        # 显示 Reduce 任务完成时间统计（如果有多个 Reduce）
        if info.get('num_reduce_tasks', 0) > 0:
            print(f"\nReduce 任务统计 (使用API提供的elapsedTime):")
            print(f"  最快 Reduce 完成时间:   {info.get('min_reduce_finish_time', 'N/A')}秒 (相对作业开始)")
            print(f"  最慢 Reduce 完成时间:   {info.get('max_reduce_finish_time', 'N/A')}秒 (相对作业开始)")
            if info.get('min_reduce_elapsed') is not None:
                print(f"  最快 Reduce 总耗时:     {info['min_reduce_elapsed']:.2f}秒")
                print(f"  最慢 Reduce 总耗时:     {info['max_reduce_elapsed']:.2f}秒")
                print(f"  平均 Reduce 总耗时:     {info['avg_reduce_elapsed']:.2f}秒")
                if info.get('reduce_elapsed_stddev') is not None:
                    print(f"  Reduce 耗时标准差:      {info['reduce_elapsed_stddev']:.2f}秒")
            if info.get('shuffle_time') and info['shuffle_time'].get('avg') is not None:
                print(f"\nReduce 详细阶段统计:")
                shuffle = info['shuffle_time']
                merge = info['merge_time']
                reduce = info['reduce_time']
                print(f"  Shuffle:  最快={shuffle['min']:.2f}s, 最慢={shuffle['max']:.2f}s, 平均={shuffle['avg']:.2f}s")
                print(f"  Merge:    最快={merge['min']:.2f}s, 最慢={merge['max']:.2f}s, 平均={merge['avg']:.2f}s")
                print(f"  Reduce:   最快={reduce['min']:.2f}s, 最慢={reduce['max']:.2f}s, 平均={reduce['avg']:.2f}s")
        
        print(f"{'='*80}\n")


def extract_single_job(job_id):
    """提取单个作业的时间信息"""
    extractor = JobTimingExtractor(job_id)
    return extractor.extract()


def batch_process_results(results_file):
    """批量处理结果文件中的所有作业"""
    print(f"正在批量处理: {results_file}")
    print(f"{'='*80}\n")
    
    # 读取结果文件
    try:
        with open(results_file, 'r') as f:
            data = json.load(f)
    except Exception as e:
        print(f"错误：无法读取结果文件 - {e}")
        return
    
    # 判断文件格式
    if isinstance(data, dict) and 'results' in data:
        # Task 2/3/4 格式
        results = data['results']
    elif isinstance(data, list):
        # Task 1 格式
        results = data
    else:
        print("错误：未知的结果文件格式")
        return
    
    # 提取所有作业的时间信息
    enhanced_results = []
    
    for i, result in enumerate(results, 1):
        job_id = result.get('job_id')
        if not job_id or job_id == 'unknown':
            print(f"[{i}/{len(results)}] 跳过：无效的 job_id")
            enhanced_results.append(result)
            continue
        
        print(f"[{i}/{len(results)}] 处理 {job_id}...")
        
        extractor = JobTimingExtractor(job_id)
        if extractor.fetch_job_info() and extractor.fetch_tasks():
            extractor.fetch_counters()  # 获取 Counters（失败不影响）
            timing_info = extractor.extract_timing_info()
            if timing_info:
                # 合并所有时间信息到原始结果
                result['job_name'] = timing_info['job_name']
                result['state'] = timing_info['state']
                result['uberized'] = timing_info['uberized']
                result['submit_time_ts'] = timing_info['submit_time']
                result['start_time_ts'] = timing_info['job_start_time']
                result['finish_time_ts'] = timing_info['job_finish_time']
                result['submit_time_str'] = timing_info['submit_time_str']
                result['start_time_str'] = timing_info['start_time_str']
                result['finish_time_str'] = timing_info['finish_time_str']
                result['elapsed_time_str'] = timing_info['elapsed_time_str']
                result['total_time_from_api'] = timing_info['total_time']
                result['avg_map_time'] = timing_info['avg_map_time']
                result['avg_shuffle_time'] = timing_info['avg_shuffle_time']
                result['avg_merge_time'] = timing_info['avg_merge_time']
                result['avg_reduce_time'] = timing_info['avg_reduce_time']
                # 作业时间统计
                result['job_elapsed_time'] = timing_info.get('job_elapsed_time')
                result['total_map_time'] = timing_info.get('total_map_time')
                result['total_reduce_time'] = timing_info.get('total_reduce_time')
                # CPU 和资源
                result['cpu_time'] = timing_info.get('cpu_time')
                result['gc_time'] = timing_info.get('gc_time')
                # 内存
                result['physical_memory_bytes'] = timing_info.get('physical_memory_bytes')
                result['virtual_memory_bytes'] = timing_info.get('virtual_memory_bytes')
                result['committed_heap_bytes'] = timing_info.get('committed_heap_bytes')
                result['peak_map_physical_memory'] = timing_info.get('peak_map_physical_memory')
                result['peak_reduce_physical_memory'] = timing_info.get('peak_reduce_physical_memory')
                result['peak_map_virtual_memory'] = timing_info.get('peak_map_virtual_memory')
                result['peak_reduce_virtual_memory'] = timing_info.get('peak_reduce_virtual_memory')
                # 数据规模
                result['hdfs_bytes_read'] = timing_info.get('hdfs_bytes_read')
                result['hdfs_bytes_written'] = timing_info.get('hdfs_bytes_written')
                result['file_bytes_read'] = timing_info.get('file_bytes_read')
                result['file_bytes_written'] = timing_info.get('file_bytes_written')
                result['map_input_records'] = timing_info.get('map_input_records')
                result['map_input_bytes'] = timing_info.get('map_input_bytes')
                result['map_output_records'] = timing_info.get('map_output_records')
                result['map_output_bytes'] = timing_info.get('map_output_bytes')
                # Shuffle
                result['reduce_shuffle_bytes'] = timing_info.get('reduce_shuffle_bytes')
                result['reduce_input_records'] = timing_info.get('reduce_input_records')
                result['reduce_input_groups'] = timing_info.get('reduce_input_groups')
                result['reduce_output_records'] = timing_info.get('reduce_output_records')
                result['shuffled_maps'] = timing_info.get('shuffled_maps')
                # 时间点
                result['map_completion_time'] = timing_info['map_completion_time']
                result['first_reduce_start_time'] = timing_info['first_reduce_start_time']
                result['reduce_completion_time'] = timing_info['reduce_completion_time']
                result['num_map_tasks'] = timing_info['num_map_tasks']
                result['num_reduce_tasks'] = timing_info['num_reduce_tasks']
                # Reduce 任务统计（使用API提供的elapsedTime）
                result['min_reduce_finish_time'] = timing_info.get('min_reduce_finish_time')
                result['max_reduce_finish_time'] = timing_info.get('max_reduce_finish_time')
                result['min_reduce_elapsed'] = timing_info.get('min_reduce_elapsed')
                result['max_reduce_elapsed'] = timing_info.get('max_reduce_elapsed')
                result['avg_reduce_elapsed'] = timing_info.get('avg_reduce_elapsed')
                result['reduce_elapsed_stddev'] = timing_info.get('reduce_elapsed_stddev')
                # Reduce 详细阶段时间（更直观的结构）
                result['shuffle_time'] = timing_info.get('shuffle_time')
                result['merge_time'] = timing_info.get('merge_time')
                result['reduce_time'] = timing_info.get('reduce_time')
                # 每个Reduce的详细信息
                result['reduce_tasks'] = timing_info.get('reduce_tasks')
                print(f"  ✓ Map完成: {timing_info['map_completion_time']:.2f}s, "
                      f"Reduce启动: {timing_info['first_reduce_start_time']:.2f}s, "
                      f"Reduce完成: {timing_info['reduce_completion_time']:.2f}s")
            else:
                print(f"  ✗ 无法提取时间信息")
        else:
            print(f"  ✗ 无法获取作业信息")
        
        enhanced_results.append(result)
        print()
    
    # 保存增强后的结果
    output_file = results_file.replace('.json', '_enhanced.json')
    
    if isinstance(data, dict):
        data['results'] = enhanced_results
        output_data = data
    else:
        output_data = enhanced_results
    
    try:
        with open(output_file, 'w') as f:
            json.dump(output_data, f, indent=2)
        print(f"\n{'='*80}")
        print(f"✓ 增强后的结果已保存到: {output_file}")
        print(f"{'='*80}\n")
    except Exception as e:
        print(f"错误：无法保存结果文件 - {e}")


def main():
    """主函数"""
    if len(sys.argv) < 2:
        print("用法:")
        print("  单个作业: python3 extract_job_timing.py <job_id>")
        print("  批量处理: python3 extract_job_timing.py --batch <results_json_file>")
        print("\n示例:")
        print("  python3 extract_job_timing.py job_1764138085950_0002")
        print("  python3 extract_job_timing.py --batch ../task1/results/raw_results.json")
        sys.exit(1)
    
    if sys.argv[1] == '--batch':
        if len(sys.argv) < 3:
            print("错误：请指定结果文件路径")
            sys.exit(1)
        batch_process_results(sys.argv[2])
    else:
        job_id = sys.argv[1]
        timing_info = extract_single_job(job_id)
        
        if timing_info:
            # 输出 JSON 格式（方便程序化使用）
            print("\nJSON 格式输出:")
            print(json.dumps(timing_info, indent=2))


if __name__ == '__main__':
    main()

