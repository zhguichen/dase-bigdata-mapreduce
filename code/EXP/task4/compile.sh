#!/bin/bash
# Compile WordCount.java and create JAR file

set -e

echo "============================================================"
echo "Compiling WordCount.java"
echo "============================================================"

# Check if HADOOP_HOME is set
if [ -z "$HADOOP_HOME" ]; then
    export HADOOP_HOME=/opt/hadoop
fi

echo "HADOOP_HOME: $HADOOP_HOME"

# Set Hadoop classpath
export HADOOP_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)

# Create build directory
mkdir -p /root/Exp-hadoop/EXP/task4/build

cd /root/Exp-hadoop/EXP/task4

echo ""
echo "Step 1: Compiling Java source..."
javac -classpath $HADOOP_CLASSPATH -d build src/WordCount.java

if [ $? -eq 0 ]; then
    echo "✓ Compilation successful"
else
    echo "✗ Compilation failed"
    exit 1
fi

echo ""
echo "Step 2: Creating JAR file..."
cd build
jar -cvf ../wordcount.jar *.class

if [ $? -eq 0 ]; then
    echo "✓ JAR file created: wordcount.jar"
else
    echo "✗ JAR creation failed"
    exit 1
fi

cd ..

echo ""
echo "============================================================"
echo "✓ Build Complete!"
echo "============================================================"
echo "JAR file location: /root/Exp-hadoop/EXP/task4/wordcount.jar"
echo ""
ls -lh wordcount.jar

