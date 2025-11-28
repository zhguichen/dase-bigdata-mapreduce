# Python 环境配置说明

## 虚拟环境位置

Python 虚拟环境配置在主目录 `/root/Exp-hadoop/.venv`，所有实验任务共享同一个环境。

## 使用阿里云镜像源

uv 已配置使用阿里云 PyPI 镜像源，加速包下载：

```bash
uv pip install -i https://mirrors.aliyun.com/pypi/simple/ <package_name>
```

## 环境激活

在任何 task 目录下运行实验前，需要先激活虚拟环境：

```bash
source /root/Exp-hadoop/.venv/bin/activate
```

或者使用相对路径（在 Exp-hadoop 目录下）：

```bash
source .venv/bin/activate
```

## 已安装的依赖

当前环境已安装以下包（用于所有实验任务）：

- pandas >= 2.0.0
- matplotlib >= 3.7.0
- numpy >= 1.24.0

## 添加新依赖

如果需要为其他任务添加新的 Python 依赖：

```bash
cd /root/Exp-hadoop
source .venv/bin/activate
uv pip install -i https://mirrors.aliyun.com/pypi/simple/ <new_package>
```

## 验证环境

验证环境是否正确配置：

```bash
cd /root/Exp-hadoop
source .venv/bin/activate
python3 -c "import pandas, matplotlib, numpy; print('✓ Environment OK')"
```

