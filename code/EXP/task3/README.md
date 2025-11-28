# Task 3: 不同计算负载类型的对比测试

## 实验目标

对比 IO 密集型与 CPU 密集型作业对于启动策略的不同敏感度，证明不同类型作业需要不同的启动策略。

## 实验配置

- **数据规模**: 1GB (固定)
- **IO 密集型作业**: TeraSort
- **CPU 密集型作业**: WordCount
- **Reduce 数量**: 4
- **Slowstart 测试值**: 0.05, 0.50, 1.00
- **每组运行次数**: 3次

## 目录结构

```
task3/
├── src/                    # 源代码
│   └── WordCount.java     # MapReduce WordCount 程序
├── scripts/               # 自动化脚本
│   ├── generate_data.py   # 数据生成脚本（WordCount用）
│   └── run_experiment.py  # 实验运行脚本
├── data/                  # 本地数据存储
│   └── input_wordcount_1gb.txt  # WordCount测试数据
├── results/               # 实验结果
│   └── raw_results_YYYYMMDD_HHMMSS.json            # 原始实验数据
│   └── raw_results_YYYYMMDD_HHMMSS_enhanced.json    # 增强后的实验数据
├── build/                 # 编译输出
├── wordcount.jar          # 编译后的 JAR 文件
├── compile.sh             # 编译脚本
├── run_all.sh             # 完整自动化脚本
└── README.md              # 本文件
```

## 实验原理

### IO 密集型 vs CPU 密集型

**IO 密集型作业 (TeraSort)**:
- 主要瓶颈在数据传输和磁盘读写
- Shuffle 阶段时间占比较大
- Map 输出数据量大，需要排序
- 早启动 Reduce 可以利用 Map 运行期间进行 Shuffle，减少总时间

**CPU 密集型作业 (WordCount)**:
- 主要瓶颈在计算（分词、聚合）
- Shuffle 阶段相对较短（有 Combiner）
- Map 和 Reduce 都需要较多 CPU 时间
- 晚启动 Reduce 避免资源竞争，可能更优

### TeraSort 数据格式

- TeraGen 生成固定格式数据：每条记录 100 字节
- 1GB 数据 ≈ 10,737,418 条记录
- 数据随机生成，用于测试排序性能

## 运行步骤

### 前置要求

1. Hadoop 集群已启动并正常运行
2. Python 3 环境已安装
3. 已安装 uv 包管理器

### 快速开始（一键运行）

```bash
cd /root/Exp-hadoop/EXP/task3
chmod +x run_all.sh
./run_all.sh
```

一键脚本会自动完成：
1. 激活虚拟环境
2. 生成 WordCount 测试数据
3. 编译 WordCount.java
4. 运行所有实验（WordCount + TeraSort）
5. 保存并展示结果

**预计总耗时**: 40-80 分钟（取决于集群性能）

---

### 手动运行步骤

如果需要更精细的控制，可以按以下步骤手动执行：

#### 步骤 1: 安装 Python 依赖

```bash
# 使用主目录的虚拟环境
cd /root/Exp-hadoop
source .venv/bin/activate

# 如果虚拟环境不存在，创建并安装依赖
uv venv
source .venv/bin/activate

# 方式1: 使用 requirements.txt（推荐）
uv pip install -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/

# 方式2: 直接安装（如果 requirements.txt 不存在）
uv pip install -i https://mirrors.aliyun.com/pypi/simple/ pandas matplotlib numpy
```

**注意**: 
- Python 虚拟环境配置在主目录 `/root/Exp-hadoop/.venv`，所有 task 共享同一个环境
- 依赖管理文件位于 `/root/Exp-hadoop/requirements.txt`（项目根目录）

#### 步骤 2: 生成 WordCount 测试数据

```bash
cd /root/Exp-hadoop/EXP/task3

# 确保已激活虚拟环境
source /root/Exp-hadoop/.venv/bin/activate

# 生成 1GB WordCount 测试数据
python3 scripts/generate_data.py
```

生成的数据文件位于: `data/input_wordcount_1gb.txt`

**注意**: TeraSort 的数据会在实验运行时自动通过 TeraGen 生成。

#### 步骤 3: 编译 WordCount 程序

```bash
cd /root/Exp-hadoop/EXP/task3

# 编译 Java 代码并创建 JAR 文件
chmod +x compile.sh
./compile.sh
```

编译成功后会生成 `wordcount.jar` 文件。

**注意**: TeraSort 使用 Hadoop 自带的示例程序，无需编译。

#### 步骤 4: 运行实验

```bash
cd /root/Exp-hadoop/EXP/task3

# 确保已激活虚拟环境
source /root/Exp-hadoop/.venv/bin/activate

# 运行完整实验（自动测试两种作业类型）
# 预计耗时: 40-80 分钟
python3 scripts/run_experiment.py
```

实验脚本会：
1. 将 WordCount 数据上传到 HDFS
2. 使用 TeraGen 生成 TeraSort 数据（~1GB）
3. 运行 WordCount 实验（3个slowstart值 × 3次运行）
4. 运行 TeraSort 实验（3个slowstart值 × 3次运行）
5. 记录详细的时间指标
6. 保存结果到 `results/` 目录

**实验总数**: 2 种作业 × 3 个 slowstart 值 × 3 次运行 = 18 个实验

#### 步骤 5: 查看结果

实验完成后，结果保存在 `results/` 目录：

```bash
cd /root/Exp-hadoop/EXP/task3/results

# 查看 TeraSort 汇总表（对应任务书的表4）
cat table4_terasort_summary.csv

# 查看 WordCount 汇总表（对应任务书的表5）
cat table5_wordcount_summary.csv

# 查看详细结果
cat results.csv

# 查看完整 JSON 数据
cat raw_results.json
```

## 实验结果

### 输出文件

实验完成后，结果保存在 `results/` 目录：

1. **raw_results_YYYYMMDD_HHMMSS.json**: 原始实验数据（JSON 格式）
   - 包含每次运行的基本信息：作业类型、slowstart 值、Job ID、总执行时间等
   - 使用 `extract_job_timing.py` 工具增强后，会生成 `raw_results_YYYYMMDD_HHMMSS_enhanced.json`

2. **raw_results_YYYYMMDD_HHMMSS_enhanced.json**: 增强后的详细数据
   - 包含完整的时间信息、资源使用、数据规模等指标
   - 详细字段说明请参考：**[../tools/README.md](../tools/README.md)** 和 **[../tools/FIELD_SPECIFICATION.md](../tools/FIELD_SPECIFICATION.md)**

### 提取详细指标

实验完成后，使用工具提取详细指标：

```bash
cd /root/Exp-hadoop/EXP/tools
python3 extract_job_timing.py --batch ../task3/results/raw_results_YYYYMMDD_HHMMSS.json
```

这会生成增强版 JSON 文件，包含：
- 关键时间点（Map 完成、Reduce 启动、Reduce 完成）
- CPU 和内存使用情况（用于区分 IO 密集型和 CPU 密集型）
- Shuffle 阶段详细统计（Shuffle 字节数、耗时等）
- 工作负载对比所需的所有指标

**详细字段说明和使用方法，请参考：**
- **[../tools/README.md](../tools/README.md)** - 工具使用指南和示例
- **[../tools/FIELD_SPECIFICATION.md](../tools/FIELD_SPECIFICATION.md)** - 完整字段规范文档

## 手动运行单个实验

### 运行单个 WordCount 实验

```bash
# 示例：使用 1GB 数据，slowstart=0.50
hadoop jar wordcount.jar WordCount \
  /user/root/task3/input_wordcount \
  /user/root/task3/output_wc_test \
  0.50 \
  4
```

### 运行单个 TeraSort 实验

```bash
# 步骤 1: 生成 TeraSort 数据（如果尚未生成）
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar \
  teragen \
  10737418 \
  /user/root/task3/input_terasort

# 步骤 2: 运行 TeraSort，使用 slowstart=0.50
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar \
  terasort \
  -Dmapreduce.job.reduce.slowstart.completedmaps=0.50 \
  -Dmapreduce.job.reduces=4 \
  /user/root/task3/input_terasort \
  /user/root/task3/output_ts_test

# 步骤 3: 验证排序结果（可选）
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar \
  teravalidate \
  /user/root/task3/output_ts_test \
  /user/root/task3/validate_ts_test
```

参数说明：
- TeraGen: 记录数（~10.7M ≈ 1GB）、输出路径
- TeraSort: `-D` 参数设置 slowstart、输入路径、输出路径
- TeraValidate: 输入路径、验证结果输出路径

## 查看 Hadoop 作业状态

### Web UI

- **ResourceManager**: http://47.116.112.198:8088
  - 查看运行中和历史作业
  - 监控集群资源使用
  
- **JobHistory**: http://47.116.112.198:19888
  - 查看作业详细历史
  - 分析 Map/Reduce 时间分布
  - 查看 Shuffle 阶段时间

### 命令行

```bash
# 查看运行中的作业
yarn application -list

# 查看作业详情
mapred job -status <job_id>

# 查看作业日志
yarn logs -applicationId <application_id>

# 查看 HDFS 数据
hdfs dfs -ls /user/root/task3

# 查看集群状态
yarn node -list
hdfs dfsadmin -report
```



## 清理实验数据

```bash
# 清理 HDFS 上的实验数据
hdfs dfs -rm -r /user/root/task3

# 清理本地生成的数据（可选）
rm -rf data/input_*.txt

# 清理实验结果（可选）
rm -rf results/*
```
