#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Date    : 2022-06-23
# @Author  : tianyafu


from pyspark.sql import SparkSession
from pyspark import SparkConf

# 自定义分区数
SPARK_CUSTOM_NUM_PARTITIONS = "spark.custom.num.partition"
# 目标表
SPARK_TARGET_TABLE_NAME = "spark.target.table.name"
# spark任务的前置依赖  用于依赖检查
SPARK_PRE_DEPENDENCY_PATH = "spark.pre.dependency.path"
# spark任务的后置依赖  使其他下游任务的能够依赖检查
SPARK_OUTPUT_DEPENDENCY_PATH = "spark.output.dependency.path"
# spark任务的hive database
SPARK_HIVE_DATABASE = "spark.hive.db.name"


def get_spark_conf(warehouse_location, metastore_uris):
    conf = SparkConf()
    # 设置appName 和 master
    # conf.set("spark.app.name", conf.get("spark.app.name", "pyspark"))
    # conf.set("spark.master", conf.get("spark.master", "local[2]"))
    # pyspark 需要设置该变量
    conf.set("spark.sql.warehouse.dir", warehouse_location)
    # 设置Hive 的 metastore uris
    conf.set("spark.hadoop.hive.metastore.uris", metastore_uris)
    # 开启非严格模式和动态分区
    conf.set("spark.hadoop.hive.exec.dynamic.partition", "true")
    conf.set("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
    return conf


def get_spark_session(conf):
    # config方法中 必须是conf=conf这种形式 不能直接写conf  否则报错 'SparkConf' object has no attribute '_get_object_id'  参见 https://issues.apache.org/jira/browse/SPARK-2003
    return SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()


def stop(spark):
    if spark is not None:
        spark.stop()