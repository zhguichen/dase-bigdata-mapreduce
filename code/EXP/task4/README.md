# Task 4: 数据倾斜场景测试

## 实验目标

探究在数据分布极度不均匀的场景下，Reduce 启动策略对长尾任务（straggler）的影响，验证早启动 Reduce 是否能缓解数据倾斜问题。

## 核心问题

1. **数据倾斜的长尾效应**：当某个 key 占据大量数据时，对应的 Reduce 任务会成为瓶颈吗？
2. **Slowstart 的缓解作用**：早启动 Reduce（小 slowstart）能否通过提前 Shuffle 来缓解长尾效应？
3. **对比均匀场景**：倾斜场景与均匀数据场景的性能差异有多大？

## 实验配置

### 数据集设计

1. **倾斜数据集（Skewed Data）**
   - 总大小：1GB
   - 热点单词 "hotkey"：占总词频的 **60%**
   - 其他单词：10,000 个不同单词，均匀分布
   - 目的：模拟生产环境中的数据倾斜场景

2. **均匀数据集（Uniform Data）** - 对照组
   - 总大小：1GB
   - 词汇分布：10,000 个单词均匀随机分布
   - 目的：作为基准对照，突出倾斜效应

### 测试参数

- **作业类型**: WordCount（便于控制 key 分布）
- **Reduce 数量**: 4（多个 Reduce 能更好观察倾斜）
- **Slowstart 测试值**: 0.05（早启动）、0.50（中间值）、1.00（完全等待）
- **每组运行次数**: 3次（取平均值）

## 目录结构

```
task4/
├── src/                          # 源代码
│   └── WordCount.java           # MapReduce WordCount 程序
├── scripts/                     # 自动化脚本
│   ├── generate_data.py         # 数据生成脚本（倾斜+均匀）
│   └── run_experiment.py        # 实验运行脚本
├── data/                        # 本地数据存储
│   ├── input_skewed_1gb.txt     # 倾斜数据（60% hotkey）
│   └── input_uniform_1gb.txt    # 均匀数据（对照组）
├── results/                     # 实验结果
│   ├── raw_results.json         # 原始实验数据
│   ├── results.csv              # CSV 格式结果
│   ├── summary_basic.csv        # 基本汇总统计
│   └── reduce_tasks_template.csv # Reduce 任务详细时间模板
├── build/                       # 编译输出
├── wordcount.jar                # 编译后的 JAR 文件
├── compile.sh                   # 编译脚本
├── run_all.sh                   # 完整自动化脚本
└── README.md                    # 本文件
```

## 运行步骤

### 前置要求

1. Hadoop 集群已启动并正常运行
2. Python 3 环境已安装
3. 已安装 uv 包管理器

### 快速运行（推荐）

```bash
cd /root/Exp-hadoop/EXP/task4
chmod +x run_all.sh
./run_all.sh
```

这将自动执行所有步骤：数据生成、编译、实验运行。

### 分步运行

#### 步骤 1: 安装 Python 依赖

```bash
# 使用主目录的虚拟环境
cd /root/Exp-hadoop
source .venv/bin/activate

# 如果虚拟环境不存在，创建并安装依赖
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/
```

#### 步骤 2: 生成测试数据

```bash
cd /root/Exp-hadoop/EXP/task4
source /root/Exp-hadoop/.venv/bin/activate

# 生成倾斜数据和均匀数据（各 1GB）
# 注意：数据生成可能需要 5-10 分钟
python3 scripts/generate_data.py
```

生成的数据文件：
- `data/input_skewed_1gb.txt`: 倾斜数据（hotkey 占 60%）
- `data/input_uniform_1gb.txt`: 均匀数据（对照组）

#### 步骤 3: 编译 WordCount 程序

```bash
cd /root/Exp-hadoop/EXP/task4
chmod +x compile.sh
./compile.sh
```

#### 步骤 4: 运行实验

```bash
cd /root/Exp-hadoop/EXP/task4
source /root/Exp-hadoop/.venv/bin/activate

# 运行完整实验
# 预计耗时: 40-90 分钟
python3 scripts/run_experiment.py
```
