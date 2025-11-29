# Task 1: 基准测试与参数敏感性分析

## 实验目标

探究 `mapreduce.job.reduce.slowstart.completedmaps` 参数对单一固定规模作业的影响。

## 实验配置

- **数据规模**: 1GB 文本数据
- **作业类型**: WordCount
- **Reduce 数量**: 4
- **Slowstart 测试值**: 0.05, 0.10, 0.30, 0.50, 0.70, 0.90, 1.00
- **每组运行次数**: 3次

## 目录结构

```
task1/
├── src/                    # 源代码
│   └── WordCount.java     # MapReduce WordCount 程序
├── scripts/               # 自动化脚本
│   ├── generate_data.py   # 数据生成脚本
│   └── run_experiment.py  # 实验运行脚本
├── data/                  # 本地数据存储
│   └── input_1gb.txt     # 生成的测试数据
├── results/               # 实验结果
│   └── raw_results_YYYYMMDD_HHMMSS.json            # 原始实验数据
│   └── raw_results_YYYYMMDD_HHMMSS_enhanced.json    # 增强后的实验数据
├── build/                 # 编译输出
├── wordcount.jar          # 编译后的 JAR 文件
├── compile.sh             # 编译脚本
└── README.md              # 本文件

```

## 运行步骤

### 前置要求

1. Hadoop 集群已启动并正常运行
2. Python 3 环境已安装
3. 已安装 uv 包管理器

### 步骤 1: 安装 Python 依赖

```bash
# 在主目录创建虚拟环境（如果尚未创建）
cd /root/Exp-hadoop
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
cd /root/Exp-hadoop/EXP/task1

# 确保已激活虚拟环境
source /root/Exp-hadoop/.venv/bin/activate

# 生成 1GB 测试数据（约需 2-5 分钟）
python3 scripts/generate_data.py
```

生成的数据文件位于: `data/input_1gb.txt`

### 步骤 3: 编译 WordCount 程序

```bash
# 编译 Java 代码并创建 JAR 文件
chmod +x compile.sh
./compile.sh
```

编译成功后会生成 `wordcount.jar` 文件。

### 步骤 4: 运行实验

```bash
cd /root/Exp-hadoop/EXP/task1

# 确保已激活虚拟环境
source /root/Exp-hadoop/.venv/bin/activate

# 运行完整实验（自动测试所有 slowstart 值）
# 预计耗时: 30-60 分钟（取决于集群性能）
python3 scripts/run_experiment.py
```

实验脚本会：
1. 将数据上传到 HDFS
2. 对每个 slowstart 值运行 3 次实验
3. 记录基本的时间指标（Job ID、总执行时间）
4. 保存结果到 `results/` 目录

### 步骤 5: 提取详细时间信息

实验完成后，使用自动化工具提取详细的阶段时间信息：

```bash
cd /root/Exp-hadoop/EXP/tools

# 自动提取并增强结果
./enhance_results.sh 1
```

或手动提取：

```bash
cd /root/Exp-hadoop/EXP/tools

# 确保已激活虚拟环境
source /root/Exp-hadoop/.venv/bin/activate

# 提取时间信息
python3 extract_job_timing.py --batch ../task1/results/raw_results.json

# 查看增强后的结果
cat ../task1/results/raw_results_enhanced.json
```

工具会自动从 JobHistory Server 提取：
- Map 阶段完成时间
- 第一个 Reduce 启动时间
- 所有 Reduce 完成时间

**注意**: 如果工具无法提取时间信息（作业历史已过期），可以从 JobHistory Web UI (http://47.116.119.3:19888) 手动查看。

## 实验结果

### 输出文件

实验完成后，结果保存在 `results/` 目录：

1. **raw_results_YYYYMMDD_HHMMSS.json**: 原始实验数据（JSON 格式）
   - 包含每次运行的基本信息：Job ID、提交时间、总执行时间等
   - 使用 `extract_job_timing.py` 工具增强后，会生成 `raw_results_YYYYMMDD_HHMMSS_enhanced.json`

2. **raw_results_YYYYMMDD_HHMMSS_enhanced.json**: 增强后的详细数据
   - 包含完整的时间信息、资源使用、数据规模等指标
   - 详细字段说明请参考：**[../tools/README.md](../tools/README.md)** 和 **[../tools/FIELD_SPECIFICATION.md](../tools/FIELD_SPECIFICATION.md)**

### 提取详细指标

实验完成后，使用工具提取详细指标：

```bash
cd /root/Exp-hadoop/EXP/tools
python3 extract_job_timing.py --batch ../task1/results/raw_results_YYYYMMDD_HHMMSS.json
```

这会生成增强版 JSON 文件，包含：
- 关键时间点（Map 完成、Reduce 启动、Reduce 完成）
- CPU 和内存使用情况
- 数据规模统计
- Reduce 任务详细统计（用于数据倾斜分析）

**详细字段说明和使用方法，请参考：**
- **[../tools/README.md](../tools/README.md)** - 工具使用指南和示例
- **[../tools/FIELD_SPECIFICATION.md](../tools/FIELD_SPECIFICATION.md)** - 完整字段规范文档

## 手动运行单个实验

如果需要手动运行单个配置进行测试：

```bash
# 示例：使用 slowstart=0.50 运行 WordCount
hadoop jar wordcount.jar WordCount \
  /user/root/task1/input \
  /user/root/task1/output_test \
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
```

## 清理实验数据

```bash
# 清理 HDFS 上的实验数据
hdfs dfs -rm -r /user/root/task1

# 清理本地生成的数据和结果（可选）
rm -rf data/input_1gb.txt
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

### 问题 3: 内存不足

**错误**: `Container killed by YARN for exceeding memory limits`

**解决**: 在 `run_experiment.py` 中添加内存配置：
```python
conf.set("mapreduce.map.memory.mb", "2048")
conf.set("mapreduce.reduce.memory.mb", "4096")
```

### 问题 4: 数据生成太慢

如果生成 1GB 数据太慢，可以生成较小的文件：

```bash
# 生成 500MB 数据
python3 -c "
from scripts.generate_data import generate_random_text
generate_random_text('data/input_500mb.txt', size_gb=0.5)
"
```

## 实验参数调整

如需调整实验参数，编辑 `scripts/run_experiment.py`:

```python
# 调整 Reducer 数量
NUM_REDUCERS = 4  # 可改为 2, 4, 8 等

# 调整测试的 slowstart 值
SLOWSTART_VALUES = [0.05, 0.25, 0.50, 0.80, 1.00]

# 调整每组运行次数
RUNS_PER_CONFIG = 3  # 增加运行次数可提高结果可靠性
```
