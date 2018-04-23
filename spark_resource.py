# coding: utf-8
from spark_sql import SparkSql


class SparkResource(object):
    def __init__(self):
        self.spark_sql = SparkSql()
