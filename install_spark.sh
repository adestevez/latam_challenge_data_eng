#!/bin/bash

sudo apt update
apt-get install openjdk-8-jdk-headless -qq > /dev/null
wget -q https://dlcdn.apache.org/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz
tar xf spark-3.2.1-bin-hadoop3.2.tgz
pip install -q findspark
pip install pyspark
pip install py4j
