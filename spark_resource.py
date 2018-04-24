# coding: utf-8
from spark_sql import SparkSql


class SparkResource(object):
    def __init__(self):
        self.spark_sql = SparkSql()

    def init_temp_view(self):
        """
        SQL版本先加载表用的，但是SQL版本速度一般，先不调用
        :return:
        """
        # 读取表的dataframe
        question_df = self.spark_sql.load_table_df('question')
        question_df.createOrReplaceTempView("tmp_question")

        sub_q_df = self.spark_sql.load_table_df('paper_subtype_question')
        sub_q_df.createOrReplaceTempView("tmp_paper_subtype_question")

        cog_map_df = self.spark_sql.load_table_df('question_cognition_map')
        cog_map_df.createOrReplaceTempView("tmp_question_cognition_map")
