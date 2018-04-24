# coding: utf-8
from spark_sql import SparkSql


class SparkResource(object):
    def __init__(self):
        self.spark_sql = SparkSql()

        self.init_temp_view()

    def init_temp_view(self):
        # 读取表的dataframe
        question_df = self.spark_sql.load_table_df('question')
        question_df.createOrReplaceTempView("tmp_question")

        sub_q_df = self.spark_sql.load_table_df('paper_subtype_question')
        sub_q_df.createOrReplaceTempView("tmp_paper_subtype_question")

        cog_map_df = self.spark_sql.load_table_df('question_cognition_map')
        cog_map_df.createOrReplaceTempView("tmp_question_cognition_map")

