# Task 2: 数据规模扩展性测试

## 实验目标

验证数据量增大时，最佳 Reduce 启动时机（slowstart）是否发生漂移。

## 实验配置

- **数据规模**: 500MB, 1GB, 2GB
- **作业类型**: WordCount
- **Reduce 数量**: 4
- **Slowstart 测试值**: 0.05, 0.50, 1.00
- **每组运行次数**: 3次

## 目录结构

```
task2/
├── src/                    # 源代码
│   └── WordCount.java     # MapReduce WordCount 程序
├── scripts/               # 自动化脚本
│   ├── generate_data.py   # 数据生成脚本
│   └── run_experiment.py  # 实验运行脚本
├── data/                  # 本地数据存储
│   ├── input_500mb.txt    # 500MB 测试数据
│   ├── input_1gb.txt      # 1GB 测试数据
│   └── input_2gb.txt      # 2GB 测试数据
├── results/               # 实验结果
│   ├── raw_results.json   # 原始实验数据
│   └── raw_results_YYYYMMDD_HHMMSS_enhanced.json    # 增强后的实验数据
├── build/                 # 编译输出
├── wordcount.jar          # 编译后的 JAR 文件
├── compile.sh             # 编译脚本
├── run_all.sh             # 完整自动化脚本
└── README.md              # 本文件
```

## 运行步骤

### 前置要求

1. Hadoop 集群已启动并正常运行
2. Python 3 环境已安装
3. 已安装 uv 包管理器（用于环境管理）

### 步骤 1: 安装 Python 依赖

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

### 步骤 2: 生成测试数据

```bash
cd /root/Exp-hadoop/EXP/task2

# 确保已激活虚拟环境
source /root/Exp-hadoop/.venv/bin/activate

# 生成所有规模的测试数据（500MB, 1GB, 2GB）
# 注意：2GB 数据生成可能需要 5-10 分钟
python3 scripts/generate_data.py
```

生成的数据文件位于: `data/` 目录下
- `input_500mb.txt` (~500MB)
- `input_1gb.txt` (~1GB)
- `input_2gb.txt` (~2GB)

### 步骤 3: 编译 WordCount 程序

```bash
cd /root/Exp-hadoop/EXP/task2

# 编译 Java 代码并创建 JAR 文件
chmod +x compile.sh
./compile.sh
```

编译成功后会生成 `wordcount.jar` 文件。

### 步骤 4: 运行实验

```bash
cd /root/Exp-hadoop/EXP/task2

# 确保已激活虚拟环境
source /root/Exp-hadoop/.venv/bin/activate

# 运行完整实验（自动测试所有数据规模和 slowstart 值）
# 预计耗时: 60-120 分钟（取决于集群性能和数据规模）
python3 scripts/run_experiment.py
```

实验脚本会：
1. 将每个规模的数据上传到 HDFS
2. 对每个数据规模 × slowstart 组合运行 3 次实验
3. 记录详细的执行时间
4. 保存结果到 `results/` 目录

**实验总数**: 3 个数据规模 × 3 个 slowstart 值 × 3 次运行 = 27 个实验

### 步骤 5: 查看结果

实验完成后，结果保存在 `results/` 目录：

```bash
cd /root/Exp-hadoop/EXP/task2/results

# 查看汇总表（对应任务书的表3）
cat table3_summary.csv

# 查看详细结果
cat results.csv

# 查看完整 JSON 数据
cat raw_results.json
```

## 一键运行脚本

如果要完整运行所有步骤（从数据生成到实验执行）：

```bash
cd /root/Exp-hadoop/EXP/task2
chmod +x run_all.sh
./run_all.sh
```

## 实验结果

### 输出文件

实验完成后，结果保存在 `results/` 目录：

1. **raw_results_YYYYMMDD_HHMMSS.json**: 原始实验数据（JSON 格式）
   - 包含每次运行的基本信息：数据规模、slowstart 值、Job ID、总执行时间等
   - 使用 `extract_job_timing.py` 工具增强后，会生成 `raw_results_YYYYMMDD_HHMMSS_enhanced.json`

2. **raw_results_YYYYMMDD_HHMMSS_enhanced.json**: 增强后的详细数据
   - 包含完整的时间信息、资源使用、数据规模等指标
   - 详细字段说明请参考：**[../tools/README.md](../tools/README.md)** 和 **[../tools/FIELD_SPECIFICATION.md](../tools/FIELD_SPECIFICATION.md)**

### 提取详细指标

实验完成后，使用工具提取详细指标：

```bash
cd /root/Exp-hadoop/EXP/tools
python3 extract_job_timing.py --batch ../task2/results/raw_results_YYYYMMDD_HHMMSS.json
```

这会生成增强版 JSON 文件，包含：
- 关键时间点（Map 完成、Reduce 启动、Reduce 完成）
- CPU 和内存使用情况
- 数据规模统计（HDFS 读写、Map/Reduce 输入输出记录数等）
- 扩展性分析所需的所有指标

**详细字段说明和使用方法，请参考：**
- **[../tools/README.md](../tools/README.md)** - 工具使用指南和示例
- **[../tools/FIELD_SPECIFICATION.md](../tools/FIELD_SPECIFICATION.md)** - 完整字段规范文档

## 手动运行单个实验

如果需要手动运行单个配置进行测试：

```bash
# 示例：使用 1GB 数据，slowstart=0.50 运行 WordCount
hadoop jar wordcount.jar WordCount \
  /user/root/task2/input_1GB \
  /user/root/task2/output_test \
  0.50 \
  4
```

参数说明：
- 参数1: HDFS 输入路径
- 参数2: HDFS 输出路径
- 参数3: slowstart 值
- 参数4: reducer 数量

## 查看 Hadoop 作业状态

### Web UI

- ResourceManager: http://47.116.119.3:8088
- JobHistory: http://47.116.119.3:19888

### 命令行

```bash
# 查看运行中的作业
yarn application -list

# 查看作业详情
mapred job -status <job_id>

# 查看作业日志
yarn logs -applicationId <application_id>

# 查看 HDFS 数据
hdfs dfs -ls /user/root/task2
```

## 实验结果分析

根据实验任务书要求，需要关注：

1. **最佳 slowstart 漂移**
   - 不同数据规模下，哪个 slowstart 值最优？
   - 随着数据规模增加，最优值是否发生变化？

2. **性能扩展性**
   - 数据规模翻倍，执行时间如何变化？
   - 是否呈线性扩展？

3. **Slowstart 影响程度**
   - 对于不同数据规模，slowstart 的影响是否一致？
   - 大数据规模下影响是否更显著？

### 分析示例

从 `table3_summary.csv` 可以提取：

```
500MB: 最优 slowstart = ?  (avg_time = ?)
1GB:   最优 slowstart = ?  (avg_time = ?)
2GB:   最优 slowstart = ?  (avg_time = ?)
```

对比不同数据规模下的最优策略是否一致。

## 清理实验数据

```bash
# 清理 HDFS 上的实验数据
hdfs dfs -rm -r /user/root/task2

# 清理本地生成的数据（可选）
rm -rf data/input_*.txt

# 清理实验结果（可选）
rm -rf results/*
```

## 故障排除

### 问题 1: 编译失败

**错误**: `javac: command not found`

**解决**:
```bash
# 检查 Java 环境
java -version
which javac

# 设置 JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

### 问题 2: HDFS 连接失败

**错误**: `Connection refused` 或 `Name node is in safe mode`

**解决**:
```bash
# 检查 HDFS 状态
hdfs dfsadmin -report

# 如果在安全模式，退出安全模式
hdfs dfsadmin -safemode leave

# 检查 NameNode 是否运行
jps | grep NameNode
```

### 问题 3: 数据生成太慢

如果生成大文件太慢，可以：
- 先运行小规模数据的实验（500MB, 1GB）
- 使用已有的 task1 数据（如果规模合适）
- 调整脚本中的 `size_gb` 参数

### 问题 4: 内存不足

**错误**: `Container killed by YARN for exceeding memory limits`

**解决**: 增加内存配置（编辑 WordCount.java 或在命令行中添加）：
```bash
hadoop jar wordcount.jar WordCount \
  -Dmapreduce.map.memory.mb=2048 \
  -Dmapreduce.reduce.memory.mb=4096 \
  <input> <output> <slowstart> <reducers>
```

### 问题 5: 实验时间过长

如果实验运行时间超出预期：
- 检查集群资源利用率：`yarn node -list -all`
- 查看是否有其他作业占用资源
- 考虑减少 `RUNS_PER_CONFIG`（在 `run_experiment.py` 中修改）

## 实验参数调整

如需调整实验参数，编辑 `scripts/run_experiment.py`:

```python
# 调整 Reducer 数量
NUM_REDUCERS = 4  # 可改为 2, 4, 8 等

# 调整数据规模（如果需要其他规模）
DATA_SIZES = [
    ('500MB', 'input_500mb.txt'),
    ('1GB', 'input_1gb.txt'),
    ('2GB', 'input_2gb.txt'),
]

# 调整测试的 slowstart 值
SLOWSTART_VALUES = [0.05, 0.50, 1.00]

# 调整每组运行次数
RUNS_PER_CONFIG = 3  # 可改为 2 以加快实验
```
