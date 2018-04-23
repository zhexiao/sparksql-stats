# coding: utf-8
from pyspark.sql.functions import broadcast
from spark_resource import SparkResource
from spark_exceptions import ResourceError


class SparkCognition(SparkResource):
    def __init__(self):
        super(SparkCognition, self).__init__()

    def get_knowledge_freq_top_n(self, n=20, faculty=None, subject=None):
        """
        得到某学科和学段下面试卷的知识点使用频繁度
        :param n:
        :param faculty:
        :param subject:
        :return:
        """
        if not faculty or not subject:
            raise ResourceError('缺少faculty或者subject')
        filter_str = "faculty = {0} and subject = {1}".format(faculty, subject)

        # 读取表的dataframe
        sub_q_df = self.spark_sql.load_table_df('paper_subtype_question')
        question_df = self.spark_sql.load_table_df('question')
        q_map_df = self.spark_sql.load_table_df('question_cognition_map')

        df = broadcast(sub_q_df).join(
            question_df, on=[
                question_df.qid == sub_q_df.question_id
            ], how='left'
        ).join(
            q_map_df, on=[
                q_map_df.question_id == sub_q_df.question_id
            ], how='left'
        ).select(q_map_df.cognition_map_num)

        # 统计排序
        res_df = df.filter(filter_str).groupBy(
            "cognition_map_num"
        ).count().sort('count', ascending=False).limit(n)

        return res_df

    def get_knowledge_top_n(self, n=20, faculty=None, subject=None):
        """
        获得某学科和学段下面的试题获得所绑定知识点的TOP数
        :param n:
        :param faculty:
        :param subject:
        :return:
        """
        if not faculty or not subject:
            raise ResourceError('缺少faculty或者subject')

        filter_str = "faculty = {0} and subject = {1}".format(
            faculty, subject
        )

        # 读取表的dataframe
        cog_map_df = self.spark_sql.load_table_df('question_cognition_map')
        question_df = self.spark_sql.load_table_df('question')

        df = broadcast(cog_map_df).join(
            question_df, on=[
                cog_map_df.question_id == question_df.qid
            ], how='left'
        ).select(cog_map_df.cognition_map_num)

        # 统计排序
        res_df = df.filter(filter_str).groupBy(
            "cognition_map_num"
        ).count().sort('count', ascending=False).limit(n)

        return res_df
