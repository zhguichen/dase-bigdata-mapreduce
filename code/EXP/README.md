# Hadoop MapReduce 实验

本目录包含 Hadoop MapReduce slowstart 参数的系列实验。

## 目录结构

```
EXP/
├── task1/          # Task 1: 基准测试与参数敏感性分析
├── task2/          # Task 2: 数据规模扩展性测试
├── task3/          # Task 3: 不同计算负载类型对比
├── task4/          # Task 4: 数据倾斜场景测试
└── tools/          # 通用实验工具
```

## 实验任务

### Task 1: 基准测试与参数敏感性分析

测试不同 slowstart 值（0.05, 0.25, 0.50, 0.80, 1.00）对 1GB WordCount 作业的影响。

**快速开始**:
```bash
cd task1
./run_all.sh
```

详见: [task1/README.md](task1/README.md)

### Task 2: 数据规模扩展性测试

测试不同数据规模（500MB, 1GB, 2GB）下 slowstart 参数的影响。

**快速开始**:
```bash
cd task2
./run_all.sh
```

详见: [task2/README.md](task2/README.md)

### Task 3: 不同计算负载类型对比

对比 CPU 密集型（WordCount）和 IO 密集型（TeraSort）作业在不同 slowstart 值下的表现。

**快速开始**:
```bash
cd task3
./run_all.sh
```

详见: [task3/README.md](task3/README.md)

### Task 4: 数据倾斜场景测试

测试数据倾斜（60% 热键）vs 均匀分布下，slowstart 参数对长尾效应的影响。

**快速开始**:
```bash
cd task4
./run_all.sh
```

详见: [task4/README.md](task4/README.md)

## 通用工具

### 作业时间信息提取工具

自动从 Hadoop JobHistory Server 提取详细的作业时间信息。

**功能**:
- 自动提取 Map 阶段完成时间
- 自动提取第一个 Reduce 启动时间
- 自动提取所有 Reduce 完成时间

**使用方式**:

```bash
cd tools

# 方式1: 一键增强实验结果
./enhance_results.sh 1    # 增强 Task 1 的结果
./enhance_results.sh 2    # 增强 Task 2 的结果

# 方式2: 手动批量处理
python3 extract_job_timing.py --batch ../task1/results/raw_results.json

# 方式3: 提取单个作业
python3 extract_job_timing.py job_1764138085950_0002
```

详见: [tools/README.md](tools/README.md)

## 统一的实验流程

所有实验遵循统一的流程：

### 1. 环境准备

```bash
# 在项目根目录
cd /root/Exp-hadoop

# 创建并激活虚拟环境
uv venv
source .venv/bin/activate

# 安装依赖
uv pip install -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/
```

### 2. 运行实验

```bash
# 进入对应的 task 目录
cd EXP/task1

# 方式1: 使用一键脚本
./run_all.sh

# 方式2: 分步执行
python3 scripts/generate_data.py    # 生成数据
./compile.sh                         # 编译程序
python3 scripts/run_experiment.py   # 运行实验
```

### 3. 提取详细时间信息

```bash
cd /root/Exp-hadoop/EXP/tools
./enhance_results.sh <task_number>
```

### 4. 分析结果

实验结果保存在各 task 的 `results/` 目录：
- `raw_results.json`: 原始/增强后的实验数据
- `results.csv`: CSV 格式数据
- `*_summary.csv`: 汇总统计（部分 task）

## Python 虚拟环境

所有实验共享同一个 Python 虚拟环境：

**位置**: `/root/Exp-hadoop/.venv`

**依赖**: 见 `/root/Exp-hadoop/requirements.txt`

**激活**:
```bash
source /root/Exp-hadoop/.venv/bin/activate
```

## Hadoop 集群信息

- **Master**: 172.31.12.133 (公网: 47.116.112.198)
- **Slave1**: 172.31.12.134
- **Slave2**: 172.31.12.135
- **Slave3**: 172.31.12.136

### Web UI

- **ResourceManager**: http://47.116.112.198:8088
- **JobHistory Server**: http://47.116.112.198:19888
- **NameNode**: http://47.116.112.198:9870

## 常见问题

### 实验工具找不到作业信息

如果 `extract_job_timing.py` 报告 404 错误，说明 JobHistory Server 中没有该作业的历史记录（可能已过期清理）。

**解决方案**:
1. 运行新的实验，立即提取时间信息
2. 延长 JobHistory Server 的保留时间（修改 `mapreduce.jobhistory.max-age-ms`）
3. 从 Web UI 手动记录时间信息

### Python 依赖安装失败

**问题**: `requests` 或其他库安装失败

**解决**:
```bash
cd /root/Exp-hadoop
source .venv/bin/activate
uv pip install -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/
```

### HDFS 连接失败

**检查 Hadoop 服务状态**:
```bash
jps
hdfs dfsadmin -report
```

## 文件组织规范

每个 task 目录遵循统一的结构：

```
taskN/
├── src/              # Java 源代码
├── scripts/          # Python 脚本
│   ├── generate_data.py
│   └── run_experiment.py
├── data/             # 本地测试数据
├── results/          # 实验结果
├── build/            # 编译输出
├── compile.sh        # 编译脚本
├── run_all.sh        # 一键运行脚本
└── README.md         # 任务说明
```

## 清理实验数据

```bash
# 清理 HDFS 上的所有实验数据
hdfs dfs -rm -r /user/root/task1
hdfs dfs -rm -r /user/root/task2
hdfs dfs -rm -r /user/root/task3
hdfs dfs -rm -r /user/root/task4

# 清理本地数据（可选）
cd /root/Exp-hadoop/EXP
find . -name "input_*.txt" -delete
find . -name "*.jar" -delete
find . -type d -name "build" -exec rm -rf {} +
```


检查：
1. 各 task 目录下的 `README.md`
2. `tools/README.md` - 工具使用说明
3. `/root/Exp-hadoop/Python配置说明.md` - Python 环境配置
4. Hadoop 日志: `$HADOOP_HOME/logs/`

