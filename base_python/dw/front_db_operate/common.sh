#!/usr/bin/env bash

# 防止使用未定义过的变量
set -u

export SYS_TODAY=`date +%Y%m%d`
export PARTITION_DATE=`date -d "1 day ago ${SYS_TODAY}" +%Y%m%d`

# DataX相关的环境变量
export SYSTEM_DATAX_HOME=/application/data-center/datax


# 义乌矛调前置MySQL信息
export FRONT_MYSQL_HOST=xxxxxxx
export FRONT_MYSQL_PORT=3306
export FRONT_MYSQL_USERNAME=xxxxxxx
export FRONT_MYSQL_PASSWORD=xxxxxxx
export FRONT_MYSQL_DATABASE_NAME=issue

# 义乌矛调用来承接前置MySQL中的数据的MySQL信息 方便采集后直接整库导出
export BIG_DATA_OFFICE_MYSQL_HOST=xxxxxxx
export BIG_DATA_OFFICE_MYSQL_PORT=3306
export BIG_DATA_OFFICE_MYSQL_USERNAME=xxxxxxx
export BIG_DATA_OFFICE_MYSQL_PASSWORD=xxxxxxx
export BIG_DATA_OFFICE_MYSQL_DATABASE_NAME=issue