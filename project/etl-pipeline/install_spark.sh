#!/bin/bash
set -e

echo "==== [1/5] Cài dependencies (Java 17, Python, pip, wget...) ===="
apt-get update && apt-get install -y \
    openjdk-17-jdk \
    python3 python3-pip \
    wget curl tar

echo "==== [2/5] Tải Spark bản spark-3.5.1-bin-hadoop3 ===="
cd /opt
SPARK_VERSION="3.5.1"
SPARK_PACKAGE="spark-${SPARK_VERSION}-bin-hadoop3"
wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz

echo "==== [3/5] Giải nén Spark ===="
tar -xzf ${SPARK_PACKAGE}.tgz
mv ${SPARK_PACKAGE} spark
rm ${SPARK_PACKAGE}.tgz

echo "==== [4/5] Cài pyspark ===="
pip3 install --no-cache-dir pyspark

echo "==== [5/5] Thiết lập SPARK_HOME và PATH ====
export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$PATH
echo "export SPARK_HOME=/opt/spark" >> ~/.bashrc
echo "export PATH=\$SPARK_HOME/bin:\$PATH" >> ~/.bashrc

echo "✅ Spark đã được cài đặt tại: $SPARK_HOME"
spark-submit --version
