## <font style="color:rgb(51, 51, 51);">研究目的</font>

<font style="color:rgb(51, 51, 51);">探究 MapReduce 中 Reduce 任务的启动时机及其对作业性能的影响</font>

## <font style="color:rgb(51, 51, 51);">研究内容</font>

本研究围绕 MapReduce 中 Reduce 任务的启动时机展开，主要分析 Reduce 在何种条件下开始执行，以及不同启动时机对整个作业执行效率与资源利用率的影响。研究内容包括：

1. **分析 Reduce 任务的启动时机**  
   探讨 Reduce 是否必须等待全部 Map 任务完成，slowstart 参数如何控制 Reduce 的启动比例，以及启动时机对 Map、Shuffle 与 Reduce 各阶段重叠关系的影响。
2. **研究不同启动时机如何影响整个job的执行效率和资源利用率**  
   分析启动过早、适中与过晚多种情况下，对执行时间、资源占用等性能指标的影响。
3. **探索 Reduce 启动策略在不同场景下的适用性**（扩展研究）  
   包括不同数据规模、不同类型任务以及数据倾斜等条件下，Reduce 启动时机的表现和策略差异，以更全面理解 Reduce 启动时机在多种实际场景下的作用。

## <font style="color:rgb(51, 51, 51);">实验</font>

### <font style="color:rgb(51, 51, 51);">实验环境</font>

#### 1. 硬件环境

本实验的硬件环境大致如下，具体配置可参考：[环境配置](./code/README.md)

本实验在一套在阿里云服务器上租借用的由 **4 个节点** 组成的 Hadoop 集群上进行，集群配置如下：

| 节点角色 | 节点数量 | CPU    | 内存  | 硬盘  | 网络带宽(max) |
| -------- | -------- | ------ | ----- | ----- | ------------- |
| Master   | 1        | 4 vCPU | 16 GB | 40 GB | 4 Gbps        |
| Slave    | 3        | 4 vCPU | 8 GB  | 40 GB | 4 Gbps        |


+ **HDFS DataNode 数量**：4
+ **YARN NodeManager 数量**：4
+ **HDFS 副本数**：3
+ **HDFS 块大小**：64 MB
+ Master 与 Slave 节点上的 `jps` 输出（如图所示）

![](https://cdn.nlark.com/yuque/0/2025/png/63078037/1764296894753-2f0dad14-7b11-4105-8805-3962c7df514b.png)

+ `hdfs dfsadmin -report`（如图所示）

![](https://cdn.nlark.com/yuque/0/2025/png/63078037/1764296609431-1bbfd239-a665-4d10-a721-2c227752e51a.png)![](https://cdn.nlark.com/yuque/0/2025/png/63078037/1764296627690-26aada9a-25a3-4f6e-8c8f-710c724205f2.png)![](https://cdn.nlark.com/yuque/0/2025/png/63078037/1764296636386-dbdd8894-7e2c-4db9-8ef7-819f120b0799.png)![](https://cdn.nlark.com/yuque/0/2025/png/63078037/1764296649618-f91283e3-7556-4e51-bfed-3c768ef65468.png)![](https://cdn.nlark.com/yuque/0/2025/png/63078037/1764296659309-2cad0bac-f76a-4bf0-bccc-8844bc5ccacc.png)

+ `yarn node -list`（如图所示）

![](https://cdn.nlark.com/yuque/0/2025/png/63078037/1764296855233-9005b0c4-579a-4404-83f4-1a518849372d.png)

**集群 Web UI 访问地址**

+ HDFS NameNode: [http://47.116.119.3:9870](about:blank)
+ YARN ResourceManager: [http://47.116.119.3:8088](about:blank)
+ JobHistory Server: [http://47.116.119.3:19888](about:blank)

---

#### 2. 软件环境

集群软件环境如下：

| 软件        | 版本号              |
| ----------- | ------------------- |
| 操作系统    | Ubuntu Server 22.04 |
| JDK 版本    | OpenJDK 1.8         |
| Hadoop 版本 | Hadoop 3.4.2        |
| Python      | Python 3.10         |




+ `java -version` 与 `hadoop version`（如图所示）

      ![](https://cdn.nlark.com/yuque/0/2025/png/63078037/1764296940856-2fdea8b0-1f69-4e00-a05e-08348b2b1ba7.png)

+ `hadoop version`（如图所示）

  ![](https://cdn.nlark.com/yuque/0/2025/png/63078037/1764297014526-223036f3-fd07-4ea8-b471-04576ee22c14.png)

+ Python 环境激活与依赖检查（如图所示）

![](https://cdn.nlark.com/yuque/0/2025/png/63078037/1764296047775-b3985335-3268-4bce-b842-e14163bcc3ea.png?x-oss-process=image%2Fformat%2Cwebp)

### <font style="color:rgb(51, 51, 51);">实验负载</font>

本实验共使用四类数据集与对应的工作负载，以覆盖不同规模、不同任务类型、不同分布特征的典型 MapReduce 场景。数据集均通过实验脚本自动生成，对应的函数与特性如下表所示。

#### 1. 数据集概述

| <font style="color:rgb(51, 51, 51);">Task</font> | <font style="color:rgb(51, 51, 51);">数据生成函数</font> | <font style="color:rgb(51, 51, 51);">词库大小</font>   | <font style="color:rgb(51, 51, 51);">数据特征</font>         | <font style="color:rgb(51, 51, 51);">分布类型</font>    | <font style="color:rgb(51, 51, 51);">特殊设计</font>         |
| ------------------------------------------------ | -------------------------------------------------------- | ------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------- | ------------------------------------------------------------ |
| **Task 1**                                       | `generate_random_text()`                                 | <font style="color:rgb(51, 51, 51);">~100 词</font>    | <font style="color:rgb(51, 51, 51);">常见英文词</font>       | <font style="color:rgb(51, 51, 51);">均匀随机</font>    | <font style="color:rgb(51, 51, 51);">1 GB</font>             |
| **Task 2**                                       | `generate_random_text()`                                 | <font style="color:rgb(51, 51, 51);">~100 词</font>    | <font style="color:rgb(51, 51, 51);">常见英文词 </font>      | <font style="color:rgb(51, 51, 51);">均匀随机</font>    | <font style="color:rgb(51, 51, 51);">多规模数据（500MB / 1GB / 1.5GB）</font> |
| **Task 3**                                       | `generate_random_text()`                                 | <font style="color:rgb(51, 51, 51);">~100 词</font>    | <font style="color:rgb(51, 51, 51);">常见英文词</font>       | <font style="color:rgb(51, 51, 51);">均匀随机</font>    | <font style="color:rgb(51, 51, 51);">WordCount 用该数据、TeraSort 使用 TeraGen</font> |
| **Task 4**                                       | `generate_skewed_dat()` `generate_uniform_data()`        | <font style="color:rgb(51, 51, 51);">~10,000 词</font> | <font style="color:rgb(51, 51, 51);">热点 key（hotkey）+ 10,000 普通词</font> | <font style="color:rgb(51, 51, 51);">倾斜 / 均匀</font> | <font style="color:rgb(51, 51, 51);">倾斜 60% vs 均匀对照</font> |




#### 2. 工作负载简介

本实验共使用三类 MapReduce 工作负载，覆盖 CPU 密集型、IO 密集型，以及数据倾斜敏感型场景。

**（1）WordCount**

适用于 Task1、Task2、Task3、Task4。

+ Map：提取词汇、计数
+ Reduce：聚合计数
+ 特点：
  - 计算量较大，Shuffle 数据相对较小，对 slowstart 敏感度较低
+ 数据来源：上述随机文本、不同规模数据、倾斜/均匀数据

---

**（2）TeraSort**

用于 Task3 对比不同负载类型。

+ 数据由 Hadoop 自带的 **TeraGen** 生成
+ 作业结构：
  - Map：抽取 key/value
  - Shuffle：大量排序数据传输
  - Reduce：最终排序
+ 特点：Shuffle 量极大，对网络和 IO 极其敏感，对 Reduce 启动时机有显著反应

---

**（3）倾斜 WordCount**

用于 Task4。

两种数据分布：

**① 倾斜数据（Skewed）**

+ `hotkey` 占据 60% 数据
+ 其余 10,000 词均匀出现

用于观察 Reduce 启动时机与长尾任务之间的关系。

**② 均匀数据（Uniform）**

+ 所有单词随机出现
+ 作为对照组用于比较倾斜行为

### <font style="color:rgb(51, 51, 51);">实验步骤</font>

#### Task1 

##### 实验1介绍

**目的**

+ 探究不同时间下进行reduce操作（即<font style="color:rgb(51, 51, 51);">在不同 slowstart 值下(0.05-1.00)）</font>，对单一固定规模作业的影响。

**测试参数**

+ <font style="color:rgb(51, 51, 51);">作业类型：</font>**<font style="color:rgb(51, 51, 51);">WordCount</font>**<font style="color:rgb(51, 51, 51);">：解析行、分词、计数。</font>
+ <font style="color:rgb(51, 51, 51);">Reduce数量：4</font>
+ Slowstart测试值：0.05,0.10,0.20,0.30,0.50,0.70,0.80,0.90,1.00
+ 每组运行次数：3次（取平均值）

##### 实验1步骤

**步骤 1：确认 Hadoop 集群正常运行**

在 Master 和各 Slave 节点检查 Hadoop 进程：

```plain
jps
```

+ NameNode、ResourceManager 出现在 Master 节点
+ DataNode、NodeManager 出现在所有 Slave 节点

![](https://cdn.nlark.com/yuque/0/2025/png/63078037/1764295569554-fd3e8cb5-7dba-4459-88bf-cb93d230595f.png)![](https://cdn.nlark.com/yuque/0/2025/png/63078037/1764295706628-65ef5105-854d-4f98-9f33-dbb6d0240487.png)![](https://cdn.nlark.com/yuque/0/2025/png/63078037/1764295767926-3167bdaa-856e-4fb0-b11d-36183a55c94c.png)![](https://cdn.nlark.com/yuque/0/2025/png/63078037/1764295808469-c6eb11df-31b9-4aac-bf36-5c152aef5200.png)



**步骤 2：配置 Python 实验环境**

进入项目根目录并激活虚拟环境：

```plain
cd /root/Exp-hadoop
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/
```

![](https://cdn.nlark.com/yuque/0/2025/png/63078037/1764296047775-b3985335-3268-4bce-b842-e14163bcc3ea.png)

---

**步骤 3：生成 1GB WordCount 测试数据**

执行 Task1 的数据生成脚本：

```plain
cd /root/Exp-hadoop/EXP/task1
python3 scripts/generate_data.py
```

生成的数据文件位于：

```plain
data/input_1gb.txt
```

**如图所示：generate_data.py 生成数据输出input_1gb.txt，是由80个常见英文词词库随机生成的数据集。**  
![](https://cdn.nlark.com/yuque/0/2025/png/63078037/1764313593146-37f1420a-f0f8-4066-a86d-879a5ee4a0e7.png)

![](https://cdn.nlark.com/yuque/0/2025/png/63078037/1764313670833-521e8d83-72be-4a1e-8527-7bc382976a40.png)

---

**步骤 4：编译 WordCount 程序**

执行提供的编译脚本生成 wordcount.jar(此脚本用于编译src/WordCount.java)：

```plain
chmod +x compile.sh
./compile.sh
```

编译后的文件位于：

```plain
wordcount.jar
```

**如图所示：编译成功**

![](https://cdn.nlark.com/yuque/0/2025/png/63078037/1764313756097-4c63dfde-cef5-4518-b508-a91e362efbfb.png)

---

**步骤 5：运行 slowstart 敏感性实验**

运行自动化脚本，依次测试 slowstart ∈ {0.05, 0.10, 0.20, ..., 1.00}：

```plain
source /root/Exp-hadoop/.venv/bin/activate
python3 scripts/run_experiment.py
```

脚本会自动执行：

1. 上传数据至 HDFS
2. 使用不同 slowstart 配置运行 WordCount
3. 每个 slowstart 运行 3–4 次
4. 保存结果至 `results/` 目录

![](https://cdn.nlark.com/yuque/0/2025/png/63078037/1764316404834-42d075b6-843b-43bb-918e-261a0f7be274.png)

![](https://cdn.nlark.com/yuque/0/2025/png/63078037/1764316881988-1b43a674-87ee-4a72-9469-5e0739412a99.png?x-oss-process=image%2Fformat%2Cwebp)



![](https://cdn.nlark.com/yuque/0/2025/png/63078037/1764314624032-86b0d66d-690a-4317-9752-2edea5e5cb32.png)

---

**步骤 6：提取详细时间信息**

从 JobHistory Server 拉取 Map/Reduce 时间点等指标：

```plain
cd /root/Exp-hadoop/EXP/tools
python3 extract_job_timing.py --batch ../task1/results/raw_results_xxx.json
```

生成增强版 JSON 数据：

```plain
raw_results_xxx_enhanced.json
```

---

**步骤 7：整理结果并用于分析**

最终所有运行数据位于：

```plain
task1/results/
```

用于后续绘制图表与性能分析。

![](https://cdn.nlark.com/yuque/0/2025/png/63078037/1764315748750-5e5a48f9-981a-416c-873a-89ec7e52df9b.png)



#### Task2 

##### 实验2介绍

**目的**

+ <font style="color:rgb(51, 51, 51);">验证数据量增大时，最佳 Reduce 启动时机（slowstart）是否发生漂移。</font>
+ <font style="color:rgb(51, 51, 51);">评估 slowstart（即 Reduce 提前启动比例）在不同数据规模（500 MB、1 GB、1.5 GB）下的最优取值。</font>

**测试参数**

+ 数据规模: 500MB, 1GB, 2GB
+ 作业类型: WordCount
+ Reduce 数量: 4
+ Slowstart 测试值: [0.05, 0.10, 0.20, 0.30, 0.50, 0.70, 0.80, 0.90, 1.00]
+ 每组运行次数: 3次

##### 实验2步骤

hadoop集群的启动（HDFS和YARN）在之前的实验步骤中已经详细的展示过了，不再赘述。下面直接从数据集的准备开始讲述实验步骤。

**步骤 1：生成 0.5GB、1GB、1.5GB WordCount 测试数据**

+ 运行 `generate_data.py` 脚本生成 WordCount 测试数据：

```plain
cd /root/Exp-hadoop/EXP/task2

# 确保已激活虚拟环境
source /root/Exp-hadoop/.venv/bin/activate

# 生成 WordCount 测试数据
python3 scripts/generate_data.py
```

生成的数据文件位于 `data/`目录下

**步骤2：编译 WordCount 程序**

```markdown
cd /root/Exp-hadoop/EXP/task2

# 编译 Java 代码并创建 JAR 文件
chmod +x compile.sh
./compile.sh
```

**步骤3：运行实验**

```markdown
cd /root/Exp-hadoop/EXP/task2

# 确保已激活虚拟环境
source /root/Exp-hadoop/.venv/bin/activate

# 运行完整实验（自动测试两种作业类型）
python3 scripts/run_experiment.py
```

实验脚本会：

1. 将 WordCount 数据上传到 HDFS
2. 运行 WordCount 实验（9个slowstart值 × 3次运行 × 3 中规模数据）
3. 记录详细的时间指标
4. 保存结果到 `results/` 目录

**步骤4：查看结果**

实验完成后，结果保存在 `results/` 目录：

**步骤5：提取详细结果**

实验完成后，使用工具提取详细指标：

```markdown
cd /root/Exp-hadoop/EXP/tools
python3 extract_job_timing.py --batch ../task3/results/raw_results_YYYYMMDD_HHMMSS.json
```

#### Task3 

##### **实验3介绍**

**目的**

+ <font style="color:rgb(51, 51, 51);">比较不同作业类型在不同 slowstart 配置下的性能变化。</font>
+ <font style="color:rgb(51, 51, 51);">分析 Reduce 提前/延后启动对不同工作负载阶段重叠度的影响。</font>
+ <font style="color:rgb(51, 51, 51);">给出面向不同作业类型的 slowstart 参数建议。</font>

**测试参数**

+ <font style="color:rgb(51, 51, 51);">作业类型：</font>**<font style="color:rgb(51, 51, 51);">WordCount</font>**<font style="color:rgb(51, 51, 51);">：解析行、分词、计数，Map 端计算量高，Shuffle 规模很小。</font>**<font style="color:rgb(51, 51, 51);">TeraSort</font>**<font style="color:rgb(51, 51, 51);">：需要大规模排序与分发，中间数据和 Shuffle 量显著更大。</font>
+ <font style="color:rgb(51, 51, 51);">Reduce数量：4</font>
+ Slowstart测试值：0.05,0.10,0.20,0.30,0.50,0.70,0.80,0.90,1.00
+ 每组运行次数：3次（取平均值）

##### **实验3步骤**

hadoop集群的启动（HDFS和YARN）在之前的实验步骤中已经详细的展示过了，不再赘述。下面直接从数据集的准备开始讲述实验步骤。

**步骤 1：生成 1GB WordCount 测试数据**

+ 运行 `generate_data.py` 脚本生成 WordCount 测试数据：

```plain
cd /root/Exp-hadoop/EXP/task3

# 确保已激活虚拟环境
source /root/Exp-hadoop/.venv/bin/activate

# 生成 1GB WordCount 测试数据
python3 scripts/generate_data.py
```

生成的数据文件位于 `data/input_1gb.txt`。

**步骤2：编译 WordCount 程序**

```markdown
cd /root/Exp-hadoop/EXP/task3

# 编译 Java 代码并创建 JAR 文件
chmod +x compile.sh
./compile.sh
```

![](https://cdn.nlark.com/yuque/0/2025/png/63078037/1764321251471-39785294-0e30-4a0c-a95a-24f605650f7e.png)

**步骤3：运行实验**

```markdown
cd /root/Exp-hadoop/EXP/task3

# 确保已激活虚拟环境
source /root/Exp-hadoop/.venv/bin/activate

# 运行完整实验（自动测试两种作业类型）
python3 scripts/run_experiment.py
```

实验脚本会：

1. 将 WordCount 数据上传到 HDFS
2. 使用 TeraGen 生成 TeraSort 数据（~1GB）
3. 运行 WordCount 实验（9个slowstart值 × 3次运行）
4. 运行 TeraSort 实验（9个slowstart值 × 3次运行）
5. 记录详细的时间指标
6. 保存结果到 `results/` 目录

**步骤4：查看结果**

实验完成后，结果保存在 `results/` 目录：

**步骤5：提取详细结果**

实验完成后，使用工具提取详细指标：

```markdown
cd /root/Exp-hadoop/EXP/tools
python3 extract_job_timing.py --batch ../task3/results/raw_results_YYYYMMDD_HHMMSS.json
```

#### Task4 

##### 实验4介绍

**目的**

探究在数据分布极度不均匀的场景下，Reduce 启动策略对长尾任务（straggler）的影响，验证早启动 Reduce 是否能缓解数据倾斜问题。

**核心问题**

+ 数据倾斜的长尾效应：当某个 key 占据大量数据时，对应的 Reduce 任务会成为瓶颈吗？
+ Slowstart 的缓解作用：早启动 Reduce（小 slowstart）能否通过提前 Shuffle 来缓解长尾效应？
+ 对比均匀场景：倾斜场景与均匀数据场景的性能差异有多大？

**数据集设计**

1. 倾斜数据集（Skewed Data）

+ 总大小：1GB
+ 热点单词 "hotkey"：占总词频的 60%
+ 其他单词：10,000 个不同单词，均匀分布
+ 目的：模拟生产环境中的数据倾斜场景

2. 均匀数据集（Uniform Data）

+ 总大小：1GB
+ 词汇分布：10,000 个单词均匀随机分布
+ 目的：作为基准对照，突出倾斜效应

**测试参数**

+ 作业类型: WordCount（便于控制 key 分布）
+ Reduce 数量: 4（多个 Reduce 能更好观察倾斜）
+ Slowstart 测试值: 0.05（早启动）、0.50（中间值）、1.00（完全等待）
+ 每组运行次数: 3次（取平均值）

##### 实验4步骤

hadoop集群的启动（HDFS和YARN）在之前的实验步骤中已经详细的展示过了，不再赘述。下面直接从数据集的准备开始讲述实验步骤。

**步骤 1：生成测试数据**

由于该实验

执行 Task4 的数据生成脚本：

```plain
cd /root/Exp-hadoop/EXP/task4
python3 scripts/generate_data.py
```

生成的数据文件位于：

```plain
data/input_skewed_1gb.txt
data/input_uniform_1gb.txt
```

**步骤 2**：编译wordcount.jar

![](https://cdn.nlark.com/yuque/0/2025/png/40512603/1764328650209-a51f22a6-f64d-4d5e-bdcd-5b0fa3c6dfd7.png)

**步骤 3**：启动测试

`python3 ./task4/scripts/run_experiment.py`

实验脚本会：

+ 上传倾斜数据和均匀数据到 HDFS
+ 对每种数据类型 × slowstart 组合运行 3 次
+ 记录详细执行时间和 Job ID
+ 保存结果到 results/ 目录

实验总数: 2 种数据 × 3 个 slowstart 值 × 3 次运行 = 18 个实验

**步骤 4**: 提取详细指标（自动）

使用自动化工具提取详细的 Reduce 任务统计信息，无需手动从 Web UI 提取：

```plain
cd /root/Exp-hadoop/EXP/tools
python3 extract_job_timing.py --batch ../task4/results/raw_results_YYYYMMDD_HHMMSS.json
```

工具会自动提取：

+ 每个 Reduce 任务的完成时间和耗时
+ Reduce 任务统计（min/max/avg/stddev）
+ 每个 Reduce 的详细阶段时间（Shuffle/Merge/Reduce）

所有数据都会保存在增强版 JSON 文件中，可直接用于分析。

本次实验测试的实验结果路径为：

`Exp/task4/results/raw_results_20251127_055904.json`

`Exp/task4/results/raw_results_20251127_055904_enhanced.json`

后一个文件为增强的实验结果，其中的详细字段内容请参考：

`Exp/tools/README.md - 工具使用指南和示例（包含数据倾斜分析代码示例）`

`Exp/tools/FIELD_SPECIFICATION.md - 完整字段规范文档（包含数据倾斜分析章节）`

### <font style="color:rgb(51, 51, 51);">实验结果与分析</font>

#### Task1 实验结果与分析

本实验在 1GB WordCount 作业上测试了 9 组 slowstart 值。综合所有运行结果，slowstart 对任务性能、Reduce 启动时机、阶段重叠与资源利用均具有显著影响。

**1. 总执行时间随 slowstart 变化**

 图 1：**不同 slowstart 的平均总执行时间折线图**  
![](https://cdn.nlark.com/yuque/0/2025/png/63078037/1764297440711-cffeaba2-f383-4873-a6d0-e06814d13882.png)

结果呈典型的 **U 型曲线**：

+ **slowstart = 0.20–0.30 区间性能最优**
+ **slowstart > 0.70 时性能显著下降**

从图中可以看到，整体性能呈现明显的 **U 型曲线**。当 slowstart 取值过小（如 0.05–0.10）时，Reduce 虽然被提早启动，但由于大部分 Map 尚未完成，Shuffle 阶段出现大量等待，Reduce 端无法及时获得数据开始计算，导致整体执行时间被拉长。而当 slowstart 过大（如 0.70 以上）时，Reduce 又会延迟到 Map 全部完成后才启动，使得 Map 和 Reduce 几乎呈串行运行，缺乏阶段重叠，整体运行时间显著变长。

只有在 slowstart=0.20–0.30 的中间区间时，Map 尾部与 Shuffle、Reduce 的重叠程度最佳，从而获得实验中的最优执行性能。  

**2. Map / Shuffle / Reduce 阶段时间分解**

 图 2：**Map、Shuffle、Reduce 各阶段的时间分布**  
![](https://cdn.nlark.com/yuque/0/2025/png/63078037/1764297604629-9435f5c7-8ec1-4745-82b8-86229bcd4125.png)

其中左图为各个阶段的绝对时间：

+ **Map阶段在Slowstart为0.20时用时最短（~65s）但总体时间都相对稳定**

右图为Reduce启动时机和Map的完成时间：

+ **绿色曲线：首个Reduce实际启动时间**
+ **橙色曲线：Map完成时间**
+ **曲线间隙：Reduce提前启动的时间窗口**

在对总执行时间拆分后可以看到，Map 阶段耗时在所有 slowstart 下都相对稳定，约为 65 秒，这说明 Map 阶段的性能主要由数据规模与计算逻辑决定，与 slowstart 几乎无关。

真正表现出显著变化的是 Shuffle 阶段：在 slowstart 过小的情况下，Shuffle 会由于 Reduce 过早启动而产生大量等待，使得 Shuffle 时长反而更长；在适中 slowstart（0.20–0.30）时，Shuffle 能充分利用 Map 阶段末尾的输出数据，实现最佳重叠从而使得如图1中的总时长变长；而 slowstart=1.00 时，Shuffle 完全顺延并集中发生在 Map 结束之后，无法享受任何并行带来的加速效果。

而右图也展现出Reduce 阶段的长度与其启动时机密切相关：启动越合理，等待越少，如果slowstart过大，则会接近串行处理，如slowstart在（0.90-1.0）时，初次reduce的时间就在map完成之后。

**3. Reduce 启动时机的理论与实际差异**

 图 3：Reduce 启动比例图（实际 vs 理论）  
![](https://cdn.nlark.com/yuque/0/2025/png/63078037/1764299649749-51ca4bb8-4d29-4021-b66a-39ccacbc28b4.png)理论上，Reduce 的启动时间应严格遵循 slowstart × Map 完成进度的关系,但实际上在slowstart较小的时候，第一次Reduce开始的时间都在50%到55%，这可能是因为当Map 阶段的输出量较小，Shuffle 阶段可能需要等待更多的数据才可以执行，从而消耗更多的时间来收集和排序数据，导致 Reduce 的启动被推迟。 

此外，YARN ResourceManager 的调度策略也可能是导致 Reduce 延迟启动的一个原因。YARN 在调度 Reduce 阶段时，可能优先考虑其他任务的资源分配，尤其是在 集群资源紧张时，这会导致 Reduce 启动时延迟。即使理论上 Reduce 应该在某个比例后启动，YARN 可能由于资源限制将其推迟到 Map 完成后。  

**4. 性能稳定性（方差分析）**

 图 4：**标准差 & 变异系数柱状图**  
![](https://cdn.nlark.com/yuque/0/2025/png/63078037/1764301049401-c071c4d6-ebdb-412d-ac28-62309498bcc3.png)

其中左图为同一个参数下3次实验的标准差：

+ slowstart=0.30最稳定（1.22s标准差） 
+ slowstart=0.50最不稳定（4.61s标准差） 

右图为同一个参数下3次实验的变异系数：

+ 变异系数在1.8%-6.6%之间
+ 最佳性能配置（0.20）的稳定性中等（CV=3.7%）

从稳定性角度看，slowstart 在不同取值下的执行结果波动差别较大。slowstart=0.30 的结果最稳定，方差与变异系数均为最低，说明其运行最受调度随机性、网络波动和系统负载变化的影响最小。

slowstart=0.20 虽然平均性能最优，但其稳定性中等，这表明它的性能依赖 Shuffle 与 Map 收尾阶段的精细重叠，稍有调度变化便会产生影响。

而 slowstart=0.50 附近的波动最大，说明此区间的阶段交互最为敏感，不同运行之间差异明显。稳定性指标与平均性能综合考虑，0.20–0.30 仍是最佳选取范围。

**5. 资源利用率分析（CPU & 内存）**

图 5：**资源利用率图（CPU/Memory）**  
![](https://cdn.nlark.com/yuque/0/2025/png/63078037/1764301460770-14efa4df-d677-4f19-bff3-86a9ac924544.png)

资源利用率图表显示，在不同 slowstart 值下，CPU 与内存使用均未出现显著差异。CPU 峰值、平均值以及内存消耗大致处于同一水平，说明 slowstart 参数不会改变 WordCount 作业的计算负载性质。

真正影响性能的不是 CPU 或 Memory 的瓶颈，而是 Shuffle 阶段数据拉取的节奏、Reduce 等待时间的多少，以及 Map/Reduce 的合适重叠。因此 slowstart 调优的核心价值在于 **调度优化** 而非 **资源优化**。

**6. 总结**

综上所述，slowstart 对 WordCount 作业的执行效率具有决定性影响，slowstart 过大或过小都会显著降低性能，而 **0.20–0.30 区间可实现性能、重叠度与稳定性的最佳平衡**。此区间下，Shuffle 能与 Map 阶段尾部适度重叠，Reduce 可以提前启动但不会过早等待，Map 与 Reduce 不会串行化。整体性能可比极端 slowstart 设置提升 **10% 以上**，并表现出良好的稳定性，是本实验环境下的最优设置。同时，整个实验的总执行时间与Map完成时间高度相关，且Shuffle时间的减少无法完全补偿Map阶段的延迟，因而适当提早开启Reduce操作对减少全局使用时间更为合理。

#### Task2 实验结果与分析

**1. 不同数据规模下耗时最短的slowstart配置及关键指标**

| **<font style="color:rgb(51, 51, 51);">数据规模</font>** | **<font style="color:rgb(51, 51, 51);">最优 slowstart</font>** | **<font style="color:rgb(51, 51, 51);">平均耗时 (s)</font>** | **<font style="color:rgb(51, 51, 51);">耗时标准差 (s)</font>** | **<font style="color:rgb(51, 51, 51);">Map总耗时 (s)</font>** | **<font style="color:rgb(51, 51, 51);">Reduce总耗时 (s)</font>** | **<font style="color:rgb(51, 51, 51);">CPU总时间 (s)</font>** | **<font style="color:rgb(51, 51, 51);">Reduce标准差 (s)</font>** |
| :------------------------------------------------------- | :----------------------------------------------------------- | :----------------------------------------------------------- | :----------------------------------------------------------- | :----------------------------------------------------------- | :----------------------------------------------------------- | :----------------------------------------------------------- | :----------------------------------------------------------- |
| <font style="color:rgb(51, 51, 51);">500 MB</font>       | <font style="color:rgb(51, 51, 51);">0.10</font>             | <font style="color:rgb(51, 51, 51);">50.52</font>            | <font style="color:rgb(51, 51, 51);">14.37</font>            | <font style="color:rgb(51, 51, 51);">268.77</font>           | <font style="color:rgb(51, 51, 51);">80.67</font>            | <font style="color:rgb(51, 51, 51);">222.17</font>           | <font style="color:rgb(51, 51, 51);">2.65</font>             |
| <font style="color:rgb(51, 51, 51);">1 GB</font>         | <font style="color:rgb(51, 51, 51);">0.50</font>             | <font style="color:rgb(51, 51, 51);">66.12</font>            | <font style="color:rgb(51, 51, 51);">0.53</font>             | <font style="color:rgb(51, 51, 51);">564.25</font>           | <font style="color:rgb(51, 51, 51);">91.96</font>            | <font style="color:rgb(51, 51, 51);">470.06</font>           | <font style="color:rgb(51, 51, 51);">8.09</font>             |
| <font style="color:rgb(51, 51, 51);">1500 MB</font>      | <font style="color:rgb(51, 51, 51);">0.90</font>             | <font style="color:rgb(51, 51, 51);">94.69</font>            | <font style="color:rgb(51, 51, 51);">1.07</font>             | <font style="color:rgb(51, 51, 51);">781.44</font>           | <font style="color:rgb(51, 51, 51);">21.87</font>            | <font style="color:rgb(51, 51, 51);">684.25</font>           | <font style="color:rgb(51, 51, 51);">1.08</font>             |


分析：

+ **<font style="color:rgb(51, 51, 51);">500 MB</font>**<font style="color:rgb(51, 51, 51);">：最佳 slowstart=0.10，Map/Reduce 分别耗时 268.77 s 与 80.67 s。Map 完成时间 44.27 s，Reduce 启动时间 27.29 s（早于 Map 完成），说明小数据规模下可以提前启动 Reduce。</font>
+ **<font style="color:rgb(51, 51, 51);">1 GB</font>**<font style="color:rgb(51, 51, 51);">：最佳 slowstart=0.50，Map 阶段平均 564.25 s，Reduce 91.96 s。Map 完成时间 64.58 s，Reduce 启动时间 36.43 s（在 Map 完成前启动）。</font>
+ **<font style="color:rgb(51, 51, 51);">1.5 GB</font>**<font style="color:rgb(51, 51, 51);">：最佳 slowstart=0.90，Map 781.44 s、Reduce 21.87 s，整体 CPU 时间 684.25 s。Map 完成时间 89.81 s，Reduce 启动时间 87.61 s（几乎与 Map 同时完成），说明让 Map 大幅完成后再启动 Reduce 可减少 shuffle 堵塞。</font>

**2. 不同数据规模的 slowstart-耗时曲线**

![](https://cdn.nlark.com/yuque/0/2025/png/32620802/1764421044198-d9232753-672f-445a-abe0-5f62c708475e.png)

随着数据规模增大，曲线整体抬升且最优 slowstart 向右移动；500 MB 在 0.1 附近达到最低点，而 1.5 GB 则在 0.9 左右最优，反映了数据越大可能越需要推迟slowstart。从图中可以清晰看到：

+ 500 MB 的最优点在 slowstart=0.1，曲线在 0.05~0.3 区间波动较大
+ 1 GB 的最优点在 slowstart=0.5，曲线相对平滑
+ 1.5 GB 的最优点在 slowstart=0.9，曲线在 0.7~1.0 区间表现最佳

**3. 关键阶段时间随 slowstart 变化图**

![](https://cdn.nlark.com/yuque/0/2025/png/32620802/1764338736653-9593d7a5-8829-4fd1-8745-b1ef976077a2.png)

+ **<font style="color:rgb(51, 51, 51);">500 MB</font>**<font style="color:rgb(51, 51, 51);">：当 slowstart≥0.8 时，Reduce 启动时间（约 45 s）早于 Map 完成时间（约 44~48 s），说明小数据规模下可以充分利用并行性</font>
+ **<font style="color:rgb(51, 51, 51);">1 GB</font>**<font style="color:rgb(51, 51, 51);">：Reduce 启动时间随 slowstart 增大而延迟，最优配置（slowstart=0.5）下 Reduce 在 Map 完成前约 28 s 启动</font>
+ **<font style="color:rgb(51, 51, 51);">1.5 GB</font>**<font style="color:rgb(51, 51, 51);">：最优配置（slowstart=0.9）下，Reduce 启动时间（87.61 s）几乎与 Map 完成时间（89.81 s）同步，避免了过早启动导致的资源浪费</font>

**总结**

最优 slowstart 随数据规模单调增大并发生漂移：500 MB→0.1，1 GB→0.5，1.5 GB→0.9。这一规律清晰地验证了数据量增大时，最佳 Reduce 启动时机（slowstart）确实发生了漂移，为不同数据规模下的参数调优提供了一定的指导。

#### Task3 实验结果与分析

**1. 总执行时间随 slowstart 的变化**

 **图 1**：不同 任务下slowstart 的平均总执行时间折线图

![](https://cdn.nlark.com/yuque/0/2025/png/63078037/1764322406414-ba662293-fcd0-4016-978b-097d1709a57c.png)

+ WordCount 在 slowstart=0.30 达到最低耗时（约 66s），0.05/0.10 过早启动会让 Reduce 等待，0.70 以上则因等待 Map 完成、阶段重叠度下降导致耗时上升（0.90/1.00 ≈ 71–73s）。
+ TeraSort 在 0.20 最优（约 40.6s），0.05/0.10 接近但略高，0.90/1.00 明显恶化到 48–50s，显示对过晚启动更敏感。

因此，WordCount 和 TeraSort 都有一个较优的 slowstart 范围，在 0.20 到 0.30 之间表现最佳，过小或过大的 slowstart 设置都对性能产生了负面影响，导致总执行时间延长。

**2. Map / Shuffle / Reduce 阶段时间分解**

**图 2**：Map、Shuffle、Reduce 各阶段的时间分布

![](https://cdn.nlark.com/yuque/0/2025/png/63078037/1764322735951-df97a2a1-2d69-4fc6-82d0-056c6dd047b2.png)

+ **WordCount**：当 **slowstart ≤ 0.5** 时，**Map** 阶段的完成时间较为稳定，约为 **64s 至 69s**，而 **Reduce** 的启动时间在 **33s 至 37s** 之间。这表明在较小的 **slowstart** 值下，**Reduce** 启动较早，能够及时处理来自 **Map** 的数据。

当 **slowstart > 0.7** 时，**Reduce** 启动时间明显推迟至 **60s** 左右，**Map** 和 **Reduce** 几乎串行执行，缺乏并行，导致总执行时间的显著增长。特别是 **Map** 和 **Reduce** 的重叠度降低，执行效率变差。

+ **TeraSort**：**TeraSort** 的 **Map** 阶段完成时间相对较短，大约 **29s 至 30s**，且 **slowstart** 值在 **0.05 至 0.5** 范围内时，**Reduce** 阶段能够较早启动，在 **15s 至 18s** 内启动，与 **Map** 阶段重叠，充分利用了资源。

然而，当 **slowstart > 0.7** 时，**Reduce** 启动时间推迟至 **26s 至 32s**，导致 **Map** 完成后 **Shuffle** 阶段才开始，从而使得总时间显著增加，且缺乏并行性。

从图中可以看出，适当的 **slowstart** 值（如 **0.20-0.30**）能够使得 **Map** 和 **Reduce** 阶段实现更好的重叠，提升资源利用率和任务并行性。但是两者的slowstart选择的时机来看，TeraSort启动slowstart的时间更早有助于其提高运行速度。这说明WordCount 需要一定等待以减少 Reduce 空转（slowstart启动稍晚），而TeraSort 需要更早启动以摊平大规模 Shuffle。

**3. 工作负载特征对比（计算量与 shuffle 规模）**

 **图 3**： 工作负载特征对比（计算量与 Shuffle 规模）  

![](https://cdn.nlark.com/yuque/0/2025/png/63078037/1764323541132-3bafe20f-f704-41b1-ba36-a7c596588871.png)

这张图对比了 **WordCount** 和 **TeraSort** 两个作业的 **CPU 时间** 和 **Shuffle 数据量**，呈现出明显的差异。

+ **CPU 时间**：**WordCount** 的 CPU 时间平均约为 **440-460s**，显著高于 **TeraSort** 的 **160s**。这表明 **WordCount** 任务的计算开销远高于 **TeraSort**，可能与其处理的数据量和计算逻辑复杂性较大有关。
+ **Shuffle 数据量**：**WordCount** 的 Shuffle 数据量约为 **23KB**，而 **TeraSort** 的 Shuffle 数据量则达到了 **1.04GB**，两者的差距极为显著。这意味着在 **TeraSort** 中，数据的 **Shuffle** 阶段对网络和磁盘 I/O 的压力远高于 **WordCount**，需要更长的时间来传输和处理数据。

结合这一实验结果，可以分析出：

+ **TeraSort** 的主要瓶颈在于 Shuffle 和 I/O，因此需要通过调整 `slowstart` 等参数，提前启动 Reduce 阶段，从而优化调度并缓解 Shuffle 阶段的等待压力。  
+ **WordCount** 受限于 **计算负载**，虽然其 **Shuffle** 阶段相对时间较短，但 <font style="color:rgb(51, 51, 51);">Reduce 过早启动只会增加等待，适合中等 slowstart。</font>

**4. Shuffle 时间随 slowstart 的变化**

**图 4**：Shuffle 时间随 slowstart 的变化图

![](https://cdn.nlark.com/yuque/0/2025/png/63078037/1764324737944-d19c6348-f308-4fd7-91a6-17a34e0a0811.png)

+ <font style="color:rgb(51, 51, 51);">WordCount：slowstart 从 </font>**<font style="color:rgb(51, 51, 51);">0.05/0.10 到 0.3/0.5 </font>**<font style="color:rgb(51, 51, 51);">时，每个 Reduce 的平均 Shuffle 时间由 </font>**<font style="color:rgb(51, 51, 51);">~34s</font>**<font style="color:rgb(51, 51, 51);"> 下降到 </font>**<font style="color:rgb(51, 51, 51);">~24–25s</font>**<font style="color:rgb(51, 51, 51);">，slowstart≥0.7 时进一步降到 ~4–6s。Shuffle 时间变短是因为 Reduce 等 Map 完成后才拉取数据、等待时间少，但此时 Map/Reduce 几乎顺序执行，总耗时反而上升（见图1）。</font>
+ <font style="color:rgb(51, 51, 51);">TeraSort：0.05–0.5 区间 Shuffle 时间保持 11–13s、在 slowstart=0.2 最优，说明适度提前 Reduce 可以边 Map 边拉取数据。slowstart≥0.7 时 Shuffle 时间虽降到 ~5s，但 Reduce 启动过晚（见图2），失去阶段重叠，Map和Reduce操作接近串行导致总耗时提升。</font>

总体而言，Shuffle 时间随 slowstart 增大会缩短，但并不等价于更快的作业；适度的 Shuffle 并行与阶段重叠才是更优折中。WordCount 在 0.3 左右兼顾较低 Shuffle 时间与总耗时，TeraSort 在 0.2 左右保持 Shuffle 负载可控且整体最快。

**5. 总结**

slowstart 推荐设置：

+ 对于 CPU 密集型作业（如 WordCount），推荐使用 0.2–0.5 之间的 slowstart 设置，具体建议为 0.3。此设置平衡了 Reduce 阶段 的启动时机，避免过早启动造成的空转，同时优化了整体执行时间。
+ 对于 **Shuffle 密集型作业**（如 TeraSort），建议使用 **0.1–0.3** 的 **slowstart** 设置，具体推荐 **0.2**。这种设置可以有效提前启动 **Reduce** 阶段，减轻shuffle阶段数据过多对网络和磁盘 I/O 造成的压力。

#### Task4 实验结果与分析

1. **有combiner时总执行时间对比**

![](https://cdn.nlark.com/yuque/0/2025/png/40512603/1764336700679-414b623d-eddb-4029-bde8-cb51b1994eb0.png)

1. 总体趋势：均匀数据集的执行时间普遍高于倾斜数据集
2. 最优slowstart：
   - 倾斜数据：slowstart=0.20时达到最优（52.32s）
   - 均匀数据：slowstart=0.50时达到最优（60.47s）
3. 性能差异：倾斜数据比均匀数据平均快约12-18%

意外发现：倾斜数据反而执行更快，这与传统认知相悖。

原因分析：

1. Combiner优化效应：
   - WordCount程序使用了Combiner进行Map端聚合
   - 倾斜数据中60%是相同的"hotkey"
   - Combiner可以在Map端将这些重复key聚合，显著减少Shuffle数据量
2. Shuffle数据量对比：
   - 查看实验数据中的`reduce_shuffle_bytes`字段
   - 倾斜数据的Shuffle数据量约为均匀数据的40-50%
3. Partitioner行为：
   - 使用默认HashPartitioner
   - 热点key被分配到固定Reducer
   - 但由于Combiner已经聚合，单个Reducer负载并未显著增加

可以看到有combiner时，倾斜数据的长尾属性无法展现，所以下面的实验都是根据无combiner时的结果进行分析的。

2. **无combiner总执行时间对比**

![](https://cdn.nlark.com/yuque/0/2025/png/40512603/1764350107801-f081ae80-cc57-44aa-a1e9-307095989172.png)

+ 展示内容：在不同 `slowstart` 值下，倾斜（skewed）与均匀（uniform）两类数据的平均总耗时对比。
+ 主要结论：
  - 无 Combiner 下，倾斜数据整体用时通常高于均匀数据；这是长尾拖慢总完工时间的直接表现。
  - 早启动能显著降低两类数据的总耗时，但对倾斜数据只能“缩短”而难以“反转”与均匀数据的差距。
  - 均匀数据的最优 `slowstart` 多落在中位（约 0.30–0.50）；倾斜数据的低点更偏向早启动（约 0.05–0.20）。
+ 原因与解释：
  - 无 Combiner 使热键在 Shuffle/Reduce 阶段完整暴露，最慢的分区显著拉长作业尾部，提升总耗时。
  - 早启动提高 Map/Shuffle/Reduce 的重叠度，但无法消除热键集中导致的负载不均；晚启动则降低重叠度，整体时间上升。

3. **Reduce 时间标准差**

![](https://cdn.nlark.com/yuque/0/2025/png/40512603/1764350151949-010fe003-8f82-44b3-8505-d8b03c07d28a.png)

+ 展示内容：不同 `slowstart` 值下，两类数据的 Reduce 完成时间标准差（越大表示任务间差异越明显）。
+ 主要结论：
  - 倾斜数据的标准差在各 `slowstart` 下普遍更高，且随早启动加剧时会出现明显峰值，反映真实倾斜的强度。
  - 均匀数据的标准差较低且更平稳，早启动时存在适度升高，但整体弱于倾斜数据的“真实长尾”。
+ 原因与解释：
  - 热键集中使单个分区聚合/合并成本上升，形成明显的时间分散与厚尾；该效应在无 Combiner 下被完整呈现。
  - 早启动引入的“等待窗口差异”会放大标准差，但与真实倾斜相比，均匀数据的增幅有限。

4. **长尾任务分析**

![](https://cdn.nlark.com/yuque/0/2025/png/40512603/1764350206464-282afe2b-5d1a-47cc-b188-56ad33f36e46.png)

+ 展示内容：
  - (a) 长尾延迟：`Max - Avg`（最慢任务相对平均值的延迟）。
  - (b) 长尾比率：`Max / Min`（最大/最小耗时比）。
+ 主要结论：
  - 无 Combiner 下，倾斜数据的长尾延迟与比率显著高于均匀数据，并在早启动区间形成峰值；比率常年 >2 属于明显长尾。
  - 均匀数据的比率在早启动时有轻微抬升，但更多是“等待差异”导致的伪倾斜，整体水平明显低于倾斜数据。
+ 原因与解释：
  - 真实倾斜：热键集中→单个 Reduce 分区的 Shuffle/合并/计算更重→最慢任务拖尾增加。
  - 伪倾斜：早启动导致不同 Reduce 的“就绪时间”差异，形成完成时间的结构性分散，但强度有限。

5. **早启动有效性**

![](https://cdn.nlark.com/yuque/0/2025/png/40512603/1764350296478-649f7e8c-91a8-4310-b979-ecaf55ed9c5b.png)

+ 展示内容：对比 `slowstart=0.05`（早启动）与 `slowstart=1.00`（晚启动）的总耗时差与节省比例。
+ 主要结论：
  - 早启动相较晚启动在两类数据上均有显著节省；倾斜数据的节省幅度更高，但仍存在不可忽视的长尾拖延。
  - 对倾斜数据，早启动的收益随真实倾斜强度而变化，收益上限受限于最慢分区的“尾部”。
+ 原因与解释：
  - 早启动提升阶段重叠度并压缩空等时间；真实倾斜源于键分布与分区策略，需要数据层与分区层治理，单靠启动时机无法根治。

6. **综合指标热力图**

![](https://cdn.nlark.com/yuque/0/2025/png/40512603/1764350339282-21016a9d-0728-4572-aa03-6e7ded3d51b1.png)

+ 展示内容：总耗时、标准差、长尾比率、Shuffle 时间等核心指标随 `slowstart` 与数据类型的整体表现。
+ 主要结论：
  - 倾斜数据在总耗时、标准差、长尾比率三项上形成显著高值区；热力图呈现与早启动相关的高温带。
  - 均匀数据的高值区主要集中在早启动的方差/比率维度，属于伪倾斜模式，总耗时指标相对温和。
+ 原因与解释：
  - 指标联动确认：真实倾斜在多个维度同步升高；伪倾斜仅在时间分散维度抬头。无 Combiner 强化真实倾斜的可见性与范围。

7. **最优 slowstart 对比**

![](https://cdn.nlark.com/yuque/0/2025/png/40512603/1764350430602-3ab5529e-dd59-467b-949e-4535cda355ed.png)

+ 展示内容：分别标注两类数据对应的最优 `slowstart`（总耗时最低点），并给出性能曲线。
+ 主要结论：
  - 倾斜数据的最优多偏向“更早的启动”（约 0.05–0.20），借助更强的阶段重叠降低总耗时；但曲线底部更浅，收益有限。
  - 均匀数据的最优更偏中位（约 0.30–0.50），两端（过早/过晚）均会恶化总耗时。
+ 原因与解释：
  - 无 Combiner 情况下，早启动提升阶段重叠，有助于减少空等；但真实倾斜主导的长尾使“最优点”收益有限，曲线更不敏感。

8. **总结**

+ Combiner 的关键作用：从“倾斜”到“优势”
  - 核心发现：当启用Combiner时，倾斜数据集的执行时间反而显著短于均匀数据集。 这与传统认知相悖，是本次实验最关键的发现。
  - 原因分析：
    * Combiner在Map阶段对输出进行了局部聚合。
    * 倾斜数据集中存在大量重复的“热点key”，Combiner极大地减少了这些key需要Shuffle的数据量（实验数据显示，Shuffle数据量降至均匀数据的40-50%）。
    * 因此，数据倾斜的负面效应（长尾任务）在Shuffle和Reduce阶段被成功“掩盖”，其执行效率甚至因Combiner的优化效果而反超均匀数据。
  - 启示：Combiner是应对数据倾斜的首选和最有效的工具，应优先考虑使用。
+ 无Combiner时：数据倾斜的“真实面貌”暴露无遗
  - 总执行时间更长：倾斜数据的整体耗时高于均匀数据，因为最慢的Reducer（处理热点key）成为拖慢整个作业的“长尾”。
  - 任务执行时间差异巨大：Reduce任务完成时间的标准差显著更高，表明任务间负载严重不均。
  - 明显的长尾效应：长尾延迟（Max - Avg） 和长尾比率（Max / Min） 两项指标远高于均匀数据，证实了“个别任务极大拖慢整体进度”的现象。
+ Slowstart 参数的优化效果与局限性
+ 优化作用：调整slowstart参数（让Reduce任务更早启动）确实能优化性能。早启动（如slowstart=0.05）相比晚启动（slowstart=1.00）能通过提高Map、Shuffle、Reduce阶段的并行重叠度，显著缩短总执行时间，对两类数据集均有效。
+ 优化局限性：
  - 无法根治倾斜：早启动可以“缩短”但无法“消除”倾斜数据与均匀数据之间的性能差距。它优化的是流程空闲时间，但无法解决由热点key导致的根本性负载不均。
  - 最优值差异：
    * 倾斜数据的最优slowstart值偏向更早的区间（如0.05-0.20），试图通过更强的并行度来缓解长尾压力。
    * 均匀数据的最优值则更居中（如0.30-0.50），过早启动反而会因资源竞争导致轻微性能下降。

### <font style="color:rgb(51, 51, 51);">结论</font>

本研究围绕 MapReduce 中 Reduce 任务的启动时机 展开，分析了不同启动时机对作业执行效率和资源利用率的影响。通过四个关键实验，研究了 slowstart 参数的调整如何影响 Reduce 阶段的启动时机，并进一步分析了其对整个 MapReduce 作业的性能表现。以下是主要的结论：

1. **Reduce 启动时机的分析**<font style="color:rgb(51, 51, 51);">：</font>
   - **Reduce 启动时机与 Map 完成的依赖关系**<font style="color:rgb(51, 51, 51);">：在不同的 slowstart  参数设置下，实验表明 Reduce 阶段不必等待 Map 阶段的完全结束即可开始处理数据。合理地提前启动 Reduce 可以有效地减小任务的总执行时间，尤其是当 slowstart 设置适中时，Map 和 Reduce 阶段能够更好地重叠执行，避免不必要的等待时间。</font>
2. **不同启动时机对执行效率和资源利用率的影响**<font style="color:rgb(51, 51, 51);">：</font>
   - **过早启动 Reduce 的影响**<font style="color:rgb(51, 51, 51);">： 当 slowstart  设置过低时，Reduce 阶段可能会在 Map 阶段尚未完成时启动。虽然此时 Reduce 阶段开始提前拉取数据，但由于 Map 阶段的输出数据尚未完全准备好，Reduce 阶段会出现 </font>**等待数据的空闲时间**<font style="color:rgb(51, 51, 51);">。这种等待时间导致了 </font>**资源利用的低效**<font style="color:rgb(51, 51, 51);">，尤其是在 CPU 和内存的使用上。此外，虽然 Map 和 Reduce 阶段可能有重叠，但如果 Reduce 阶段启动得太早，反而不能充分利用资源，导致 </font>**总执行时间的增加**<font style="color:rgb(51, 51, 51);">。  </font>
   - **适中启动时机的优势**<font style="color:rgb(51, 51, 51);">：在大多数情况下，slowstart  设置在 0.2 到 0.5 范围内时，任务执行时间最短。此时，Map 和 Reduce 阶段能有效重叠执行，Shuffle 阶段的时间得到缩短，资源利用率和任务执行效率都得到了优化。</font>
   - **过晚启动 Reduce 的问题**<font style="color:rgb(51, 51, 51);">：当 slowstart  超过 0.7 时，Shuffle 时间进一步减少，但 Reduce 启动过晚，导致阶段重叠不充分，最终增加了总执行时间。这表明，在某些任务中，适当的调度和阶段重叠对性能优化至关重要。</font>
3. **Reduce 启动策略的适用性**<font style="color:rgb(51, 51, 51);">：</font>
   - **不同数据规模和任务类型**<font style="color:rgb(51, 51, 51);">：实验结果显示，slowstart  参数对不同数据规模和任务类型的影响有所不同。对于大规模数据集或复杂的任务（如 TeraSort），适当提前启动 Reduce 阶段能显著提高性能，尤其是在数据倾斜严重时，优化启动时机可以减少等待和提升任务的总体效率。</font>
   - **数据倾斜的影响**<font style="color:rgb(51, 51, 51);">：在存在数据倾斜的场景下，合理的 slowstart  设置能够帮助平衡不同阶段的负载，减少由于数据倾斜带来的 Shuffle 阶段压力，从而优化整个任务的执行效率。同时，</font>当启用Combiner时，倾斜数据因Map端的局部聚合效应，Shuffle数据量大幅减少（降至均匀数据的40-50%），反而比均匀数据执行更快。这掩盖了数据倾斜的负面效应。

### <font style="color:rgb(51, 51, 51);">分工</font>

| <font style="color:rgb(51, 51, 51);">姓名</font>   | <font style="color:rgb(51, 51, 51);">学号</font>        | <font style="color:rgb(51, 51, 51);">分工</font>             | <font style="color:rgb(51, 51, 51);">排名</font> |
| -------------------------------------------------- | ------------------------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------ |
| <font style="color:rgb(51, 51, 51);">张桂晨</font> | <font style="color:rgb(51, 51, 51);">51285903131</font> | <font style="color:rgb(51, 51, 51);">主要代码撰写、Task2测试、撰写报告</font> | <font style="color:rgb(51, 51, 51);">1</font>    |
| <font style="color:rgb(51, 51, 51);">祝予晗</font> | <font style="color:rgb(51, 51, 51);">51285903139</font> | <font style="color:rgb(51, 51, 51);">代码撰写、Task1测试、撰写报告</font> | <font style="color:rgb(51, 51, 51);">2</font>    |
| <font style="color:rgb(51, 51, 51);">李泽朋</font> | <font style="color:rgb(51, 51, 51);">51285903125</font> | <font style="color:rgb(51, 51, 51);">Task4测试、撰写报告</font> | <font style="color:rgb(51, 51, 51);">3</font>    |
| <font style="color:rgb(51, 51, 51);">李涵一</font> | <font style="color:rgb(51, 51, 51);">51285903137</font> | <font style="color:rgb(51, 51, 51);">Task3测试</font>        | <font style="color:rgb(51, 51, 51);">4</font>    |

