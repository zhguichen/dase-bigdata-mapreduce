# Hadoop MapReduce 实验数据字段规范

本文档定义实验结果 JSON 文件中所有字段的含义、来源和使用方法。




## 按功能分类的字段列表

### 基本信息 (8 字段)
- `slowstart` - slowstart 参数值
- `run_number` - 运行次序
- `job_id` - MapReduce Job ID
- `application_id` - YARN Application ID (Task2/3/4)
- `submit_time` - 提交时间（脚本记录）
- `num_reducers` - Reducer 数量
- `data_size` - 数据规模 (Task2)
- `job_type` / `data_type` - 作业/数据类型 (Task3/4)

### 作业概览 (10 字段)
- `job_name` - 作业名称
- `state` - 作业状态 (SUCCEEDED/FAILED)
- `uberized` - 是否为 Uber 模式
- `submit_time_ts` / `start_time_ts` / `finish_time_ts` - 时间戳（毫秒）
- `submit_time_str` / `start_time_str` / `finish_time_str` - 可读时间格式
- `elapsed_time_str` - 耗时可读格式
- `total_time_from_api` - 总耗时（秒）

### 时间统计 (8 字段)
- `job_elapsed_time` - 作业总耗时（秒）
- `total_map_time` - 所有 Map tasks 总耗时（秒）
- `total_reduce_time` - 所有 Reduce tasks 总耗时（秒）
- `avg_map_time` - 平均单个 Map 耗时（秒）
- `avg_shuffle_time` - 平均 Shuffle 耗时（秒）
- `avg_merge_time` - 平均 Merge 耗时（秒）
- `avg_reduce_time` - 平均 Reduce 计算耗时（秒）

### 关键时间点 (3 字段)
- `map_completion_time` - Map 阶段完成时间（相对作业开始，秒）
- `first_reduce_start_time` - 第一个 Reduce 启动时间（相对作业开始，秒）
- `reduce_completion_time` - 所有 Reduce 完成时间（相对作业开始，秒）

### CPU 和资源 (2 字段)
- `cpu_time` - CPU 总耗时（秒）
- `gc_time` - GC 总耗时（秒）

### 内存使用 (7 字段)
- `physical_memory_bytes` - 物理内存总使用量（字节）
- `virtual_memory_bytes` - 虚拟内存总使用量（字节）
- `committed_heap_bytes` - 已提交堆内存（字节）
- `peak_map_physical_memory` - Map 峰值物理内存（字节）
- `peak_reduce_physical_memory` - Reduce 峰值物理内存（字节）
- `peak_map_virtual_memory` - Map 峰值虚拟内存（字节）
- `peak_reduce_virtual_memory` - Reduce 峰值虚拟内存（字节）

### 数据规模与 I/O (12 字段)
- `hdfs_bytes_read` / `hdfs_bytes_written` - HDFS 读写字节数
- `file_bytes_read` / `file_bytes_written` - 本地文件读写字节数
- `map_input_records` / `map_input_bytes` - Map 输入记录数和字节数
- `map_output_records` / `map_output_bytes` - Map 输出记录数和字节数
- `reduce_input_records` / `reduce_input_groups` - Reduce 输入记录数和分组数
- `reduce_output_records` - Reduce 输出记录数
- `shuffled_maps` - Shuffle 的 Map 数量

### Shuffle 阶段 (1 字段)
- `reduce_shuffle_bytes` - Shuffle 传输字节数

### Reduce 任务统计 (8 字段) ★数据倾斜分析
- `num_map_tasks` / `num_reduce_tasks` - Map/Reduce task 总数
- `min_reduce_finish_time` / `max_reduce_finish_time` - 最快/最慢 Reduce 完成时间（相对作业开始，秒）
- `min_reduce_elapsed` / `max_reduce_elapsed` - 最快/最慢 Reduce 总耗时（秒）
- `avg_reduce_elapsed` - 平均 Reduce 总耗时（秒）
- `reduce_elapsed_stddev` - Reduce 耗时标准差（秒）★关键倾斜指标

### Reduce 详细阶段 (3 对象)
- `shuffle_time` - {min, max, avg} Shuffle 阶段统计
- `merge_time` - {min, max, avg} Merge 阶段统计
- `reduce_time` - {min, max, avg} Reduce 计算阶段统计

### 每个 Reduce 详情 (1 数组)
- `reduce_tasks[]` - 每个 Reduce 的详细信息数组
  - `task_id` - Reduce Task ID
  - `elapsed_time` - 总耗时（秒）
  - `finish_time` - 完成时间（相对作业开始，秒）
  - `shuffle_time` - Shuffle 耗时（秒）
  - `merge_time` - Merge 耗时（秒）
  - `reduce_time` - Reduce 计算耗时（秒）

## 完整字段定义表

### 基本信息字段

| 字段名 | 类型 | 来源 | 说明 | 示例 |
|--------|------|------|------|------|
| `slowstart` | float | 实验配置 | slowstart 参数值 | 0.05 |
| `run_number` | int | 实验配置 | 运行次序 | 1 |
| `job_id` | string | Hadoop 日志 | MapReduce Job ID | "job_1764138085950_0026" |
| `application_id` | string | Hadoop 日志 | YARN Application ID | "application_1764138085950_0026" |
| `submit_time` | string | 脚本记录 | 提交时间 | "2025-11-26 16:50:10" |
| `num_reducers` | int | 实验配置 | Reducer 数量 | 4 |

### 作业概览信息

| 字段名 | 类型 | 来源 | 说明 | 示例 |
|--------|------|------|------|------|
| `job_name` | string | JobHistory API | 作业名称 | "Task1_Sensitivity_slowstart0.05_reducers4" |
| `state` | string | JobHistory API | 作业状态 | "SUCCEEDED" |
| `uberized` | boolean | JobHistory API | 是否为 Uber 模式 | false |
| `submit_time_ts` | long | JobHistory API | 提交时间戳（毫秒） | 1764147013842 |
| `start_time_ts` | long | JobHistory API | 开始时间戳（毫秒） | 1764147021167 |
| `finish_time_ts` | long | JobHistory API | 完成时间戳（毫秒） | 1764147096290 |
| `submit_time_str` | string | 格式化 | 提交时间（可读） | "Wed Nov 26 16:50:13 CST 2025" |
| `start_time_str` | string | 格式化 | 开始时间（可读） | "Wed Nov 26 16:50:21 CST 2025" |
| `finish_time_str` | string | 格式化 | 完成时间（可读） | "Wed Nov 26 16:51:36 CST 2025" |
| `elapsed_time_str` | string | 格式化 | 耗时（可读） | "1mins, 15sec" |
| `total_time_from_api` | float | JobHistory API | 总耗时（秒） | 75.12 |

### 时间统计字段

| 字段名 | 类型 | 单位 | 来源 | 说明 |
|--------|------|------|------|------|
| `job_elapsed_time` | float | 秒 | Counters API | 作业总耗时 |
| `total_map_time` | float | 秒 | Counters: MILLIS_MAPS | 所有 Map tasks 总耗时 |
| `total_reduce_time` | float | 秒 | Counters: MILLIS_REDUCES | 所有 Reduce tasks 总耗时 |
| `avg_map_time` | float | 秒 | Job Info: avgMapTime | 平均单个 Map task 耗时 |
| `avg_shuffle_time` | float | 秒 | Job Info: avgShuffleTime | 平均 Shuffle 耗时 |
| `avg_merge_time` | float | 秒 | Job Info: avgMergeTime | 平均 Merge 耗时 |
| `avg_reduce_time` | float | 秒 | Job Info: avgReduceTime | 平均 Reduce 计算耗时 |

### 关键时间点

| 字段名 | 类型 | 单位 | 来源 | 说明 |
|--------|------|------|------|------|
| `map_completion_time` | float | 秒 | Tasks API | 最后一个 Map 完成时间 |
| `first_reduce_start_time` | float | 秒 | Tasks API | 第一个 Reduce 启动时间 |
| `reduce_completion_time` | float | 秒 | Tasks API | 最后一个 Reduce 完成时间 |

### CPU 和资源使用

| 字段名 | 类型 | 单位 | 来源 | 说明 |
|--------|------|------|------|------|
| `cpu_time` | float | 秒 | Counters: CPU_MILLISECONDS | CPU 总耗时 |
| `gc_time` | float | 秒 | Counters: GC_TIME_MILLIS | GC 总耗时 |

### 内存使用

| 字段名 | 类型 | 单位 | 来源 | 说明 |
|--------|------|------|------|------|
| `physical_memory_bytes` | long | 字节 | Counters: PHYSICAL_MEMORY_BYTES | 物理内存总使用量 |
| `virtual_memory_bytes` | long | 字节 | Counters: VIRTUAL_MEMORY_BYTES | 虚拟内存总使用量 |
| `committed_heap_bytes` | long | 字节 | Counters: COMMITTED_HEAP_BYTES | 已提交堆内存 |
| `peak_map_physical_memory` | long | 字节 | Counters: MAP_PHYSICAL_MEMORY_BYTES_MAX | Map 峰值物理内存 |
| `peak_reduce_physical_memory` | long | 字节 | Counters: REDUCE_PHYSICAL_MEMORY_BYTES_MAX | Reduce 峰值物理内存 |
| `peak_map_virtual_memory` | long | 字节 | Counters: MAP_VIRTUAL_MEMORY_BYTES_MAX | Map 峰值虚拟内存 |
| `peak_reduce_virtual_memory` | long | 字节 | Counters: REDUCE_VIRTUAL_MEMORY_BYTES_MAX | Reduce 峰值虚拟内存 |

### 数据规模与 I/O

| 字段名 | 类型 | 单位 | 来源 | 说明 |
|--------|------|------|------|------|
| `hdfs_bytes_read` | long | 字节 | Counters: HDFS_BYTES_READ | HDFS 读取字节数 |
| `hdfs_bytes_written` | long | 字节 | Counters: HDFS_BYTES_WRITTEN | HDFS 写入字节数 |
| `file_bytes_read` | long | 字节 | Counters: FILE_BYTES_READ | 本地文件读取字节数 |
| `file_bytes_written` | long | 字节 | Counters: FILE_BYTES_WRITTEN | 本地文件写入字节数 |
| `map_input_records` | long | 条 | Counters: MAP_INPUT_RECORDS | Map 输入记录数 |
| `map_input_bytes` | long | 字节 | Counters: BYTES_READ | Map 输入字节数 |
| `map_output_records` | long | 条 | Counters: MAP_OUTPUT_RECORDS | Map 输出记录数 |
| `map_output_bytes` | long | 字节 | Counters: MAP_OUTPUT_BYTES | Map 输出字节数 |

### Shuffle 阶段

| 字段名 | 类型 | 单位 | 来源 | 说明 |
|--------|------|------|------|------|
| `reduce_shuffle_bytes` | long | 字节 | Counters: REDUCE_SHUFFLE_BYTES | Shuffle 传输字节数 |
| `reduce_input_records` | long | 条 | Counters: REDUCE_INPUT_RECORDS | Reduce 输入记录数 |
| `reduce_input_groups` | long | 组 | Counters: REDUCE_INPUT_GROUPS | Reduce 输入分组数（唯一键数） |
| `reduce_output_records` | long | 条 | Counters: REDUCE_OUTPUT_RECORDS | Reduce 输出记录数 |
| `shuffled_maps` | int | 个 | Counters: SHUFFLED_MAPS | Shuffle 的 Map 数量 |

### Reduce 任务统计（数据倾斜分析）

| 字段名 | 类型 | 单位 | 来源 | 说明 |
|--------|------|------|------|------|
| `num_map_tasks` | int | 个 | Tasks API | Map task 总数 |
| `num_reduce_tasks` | int | 个 | Tasks API | Reduce task 总数 |
| `min_reduce_finish_time` | float | 秒 | Tasks API | 最快 Reduce 完成时间（相对作业开始） |
| `max_reduce_finish_time` | float | 秒 | Tasks API | 最慢 Reduce 完成时间（相对作业开始） |
| `min_reduce_elapsed` | float | 秒 | Tasks API: elapsedTime | 最快 Reduce 总耗时 |
| `max_reduce_elapsed` | float | 秒 | Tasks API: elapsedTime | 最慢 Reduce 总耗时 |
| `avg_reduce_elapsed` | float | 秒 | 计算 | 平均 Reduce 总耗时 |
| `reduce_elapsed_stddev` | float | 秒 | 计算 | Reduce 耗时标准差（数据倾斜指标） |

### Reduce 详细阶段时间

所有 Reduce tasks 的阶段时间统计（min/max/avg）：

| 字段 | 类型 | 单位 | 说明 |
|------|------|------|------|
| `shuffle_time.min` / `max` / `avg` | float | 秒 | Shuffle 阶段最快/最慢/平均耗时 |
| `merge_time.min` / `max` / `avg` | float | 秒 | Merge 阶段最快/最慢/平均耗时 |
| `reduce_time.min` / `max` / `avg` | float | 秒 | Reduce 计算阶段最快/最慢/平均耗时 |

### 每个 Reduce 的详细信息

`reduce_tasks[]` 数组，每个元素包含：

| 字段 | 类型 | 单位 | 说明 |
|------|------|------|------|
| `task_id` | string | - | Reduce Task ID |
| `elapsed_time` | float | 秒 | 总耗时 |
| `finish_time` | float | 秒 | 完成时间（相对作业开始） |
| `shuffle_time` | float | 秒 | Shuffle 耗时 |
| `merge_time` | float | 秒 | Merge 耗时 |
| `reduce_time` | float | 秒 | Reduce 计算耗时 |

---

## Web UI 对应关系

### JobHistory Overview 页面

| Web UI 显示 | JSON 字段 |
|------------|----------|
| Job Name | `job_name` |
| State | `state` |
| Uberized | `uberized` |
| Submitted | `submit_time_str` |
| Started | `start_time_str` |
| Finished | `finish_time_str` |
| Elapsed | `elapsed_time_str` |
| Average Map Time | `avg_map_time` |
| Average Shuffle Time | `avg_shuffle_time` |
| Average Merge Time | `avg_merge_time` |
| Average Reduce Time | `avg_reduce_time` |

### JobHistory Counters 页面

| Web UI 显示 | JSON 字段 |
|------------|----------|
| Job Elapsed Time | `job_elapsed_time` |
| Total time spent by all map tasks | `total_map_time` |
| Total time spent by all reduce tasks | `total_reduce_time` |
| CPU time spent | `cpu_time` |
| GC time elapsed | `gc_time` |
| Peak Map Physical memory | `peak_map_physical_memory` |
| Peak Reduce Physical memory | `peak_reduce_physical_memory` |
| Physical memory snapshot | `physical_memory_bytes` |
| Virtual memory snapshot | `virtual_memory_bytes` |
| Total committed heap usage | `committed_heap_bytes` |
| HDFS: Bytes Read | `hdfs_bytes_read` |
| HDFS: Bytes Written | `hdfs_bytes_written` |
| Map input records | `map_input_records` |
| Map input bytes | `map_input_bytes` |
| Reduce shuffle bytes | `reduce_shuffle_bytes` |
| Reduce input records | `reduce_input_records` |
| Reduce output records | `reduce_output_records` |

### JobHistory Tasks 页面

| Web UI 显示 | JSON 字段 |
|------------|----------|
| 最后 Map 完成时间 | `map_completion_time` |
| 第一个 Reduce 启动 | `first_reduce_start_time` |
| 最后 Reduce 完成 | `reduce_completion_time` |
| 单个 Reduce 耗时 | `reduce_tasks[].elapsed_time` |
| 单个 Reduce Shuffle | `reduce_tasks[].shuffle_time` |

---

## JSON 结构示例

```json
{
  "slowstart": 0.5,
  "run_number": 1,
  "job_id": "job_1764138085950_0026",
  "application_id": "application_1764138085950_0026",
  "submit_time": "2025-11-26 16:50:10",
  "num_reducers": 4,
  
  "job_name": "Task2_WordCount_500MB_slowstart0.5_reducers4",
  "state": "SUCCEEDED",
  "uberized": false,
  
  "submit_time_ts": 1764147013842,
  "start_time_ts": 1764147021167,
  "finish_time_ts": 1764147096290,
  "submit_time_str": "Wed Nov 26 16:50:13 CST 2025",
  "start_time_str": "Wed Nov 26 16:50:21 CST 2025",
  "finish_time_str": "Wed Nov 26 16:51:36 CST 2025",
  "elapsed_time_str": "1mins, 15sec",
  "total_time_from_api": 75.12,
  
  "job_elapsed_time": 75.12,
  "total_map_time": 254.75,
  "total_reduce_time": 21.19,
  "avg_map_time": 63.69,
  "avg_shuffle_time": 4.64,
  "avg_merge_time": 0.06,
  "avg_reduce_time": 0.60,
  
  "map_completion_time": 67.02,
  "first_reduce_start_time": 68.30,
  "reduce_completion_time": 75.09,
  
  "cpu_time": 211.59,
  "gc_time": 2.29,
  
  "physical_memory_bytes": 2863919104,
  "virtual_memory_bytes": 20426027008,
  "committed_heap_bytes": 2478833664,
  "peak_map_physical_memory": 507224064,
  "peak_reduce_physical_memory": 214593536,
  
  "hdfs_bytes_read": 536883751,
  "hdfs_bytes_written": 1445,
  "map_input_records": 10863579,
  "map_output_records": 108635790,
  
  "reduce_shuffle_bytes": 5392,
  "reduce_input_records": 484,
  "reduce_input_groups": 121,
  "reduce_output_records": 121,
  "shuffled_maps": 16,
  
  "num_map_tasks": 4,
  "num_reduce_tasks": 4,
  "min_reduce_finish_time": 72.02,
  "max_reduce_finish_time": 75.09,
  "min_reduce_elapsed": 3.71,
  "max_reduce_elapsed": 5.85,
  "avg_reduce_elapsed": 5.30,
  "reduce_elapsed_stddev": 1.06,
  
  "shuffle_time": {"min": 3.18, "max": 5.20, "avg": 4.64},
  "merge_time": {"min": 0.04, "max": 0.08, "avg": 0.06},
  "reduce_time": {"min": 0.49, "max": 0.72, "avg": 0.60},
  
  "reduce_tasks": [
    {
      "task_id": "task_1764138085950_0026_r_000000",
      "elapsed_time": 5.84,
      "finish_time": 74.14,
      "shuffle_time": 5.03,
      "merge_time": 0.08,
      "reduce_time": 0.72
    }
  ]
}
```


## 单位转换和 API 映射

### 单位转换速查

| 原始单位 | 转换公式 | 目标单位 |
|---------|---------|---------|
| 毫秒 | ÷ 1000 | 秒 |
| 字节 | ÷ 1024 | KB |
| 字节 | ÷ 1048576 | MB |
| 字节 | ÷ 1073741824 | GB |

### API 端点映射

| 字段来源 | REST API URL |
|---------|-------------|
| job_name, state, uberized, *_time_ts, avg_*_time | `/ws/v1/history/mapreduce/jobs/{job_id}` |
| map_completion_time, first_reduce_start_time, reduce_completion_time | `/ws/v1/history/mapreduce/jobs/{job_id}/tasks` |
| shuffle_time, merge_time, reduce_time (per task) | `/ws/v1/history/mapreduce/jobs/{job_id}/tasks/{task_id}/attempts` |
| cpu_time, memory_*, hdfs_*, map_*, reduce_*, shuffle_* | `/ws/v1/history/mapreduce/jobs/{job_id}/counters` |

**API 基础地址**: `http://172.31.12.133:19888/ws/v1/history/mapreduce`

