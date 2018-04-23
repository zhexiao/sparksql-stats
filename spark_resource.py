# coding: utf-8
from ccnu_resource.spark_sql import SparkSql


class SparkResource(object):
    def __init__(self):
        self.spark_sql = SparkSql()
