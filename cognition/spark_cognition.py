# coding: utf-8
from pyspark.sql.functions import broadcast
from spark_resource import SparkResource
from spark_exceptions import ResourceError


class SparkCognition(SparkResource):
    """
    DEMO:
    s_cognition = SparkCognition()
    knowledge_df = s_cognition.get_knowledge_top_n(faculty=3, subject=1)
    print(knowledge_df.toJSON().collect())
    """

    def __init__(self):
        super(SparkCognition, self).__init__()

    def get_knowledge_freq_top_n_sqlv(self, n=20, faculty=None, subject=None):
        """
        得到某学科和学段下面试卷的知识点使用频繁度 SQL版本
        :param n:
        :param faculty:
        :param subject:
        :return:
        """
        if not faculty or not subject:
            raise ResourceError('缺少faculty或者subject')

        sql_string = """
        SELECT `cognition_map_num`, COUNT(`cognition_map_num`) as `count`
        FROM tmp_paper_subtype_question tpsq
        LEFT JOIN tmp_question tq
        ON tpsq.question_id = tq.qid
        LEFT JOIN tmp_question_cognition_map tqcm
        ON tpsq.question_id = tqcm.question_id
        WHERE `faculty` = {0} AND `subject` = {1}
        GROUP BY `cognition_map_num`
        ORDER BY `count` DESC
        LIMIT {2}
        """.format(faculty, subject, n)

        res_df = self.spark_sql.spark.sql(sql_string)
        return res_df

    def get_knowledge_top_n_sqlv(self, n=20, faculty=None, subject=None):
        """
        获得某学科和学段下面的试题获得所绑定知识点的TOP数 SQL版本
        :param n:
        :param faculty:
        :param subject:
        :return:
        """
        if not faculty or not subject:
            raise ResourceError('缺少faculty或者subject')

        sql_string = """
        SELECT `cognition_map_num`, COUNT(`cognition_map_num`) as `count`
        FROM tmp_question_cognition_map tqcm
        LEFT JOIN tmp_question tq
        ON tqcm.question_id = tq.qid
        WHERE `faculty` = {0} AND `subject` = {1}
        GROUP BY `cognition_map_num`
        ORDER BY `count` DESC
        LIMIT {2}
        """.format(faculty, subject, n)

        res_df = self.spark_sql.spark.sql(sql_string)
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

        df = broadcast(self.question_cog_map_df).join(
            self.question_df, on=[
                self.question_cog_map_df.question_id == self.question_df.qid
            ], how='left'
        ).select(self.question_cog_map_df.cognition_map_num)

        # 统计排序
        res_df = df.filter(filter_str).groupBy(
            "cognition_map_num"
        ).count().sort('count', ascending=False).limit(n)

        return res_df

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

        df = broadcast(self.paper_sub_q_df).join(
            self.question_df, on=[
                self.question_df.qid == self.paper_sub_q_df.question_id
            ], how='left'
        ).join(
            self.question_cog_map_df, on=[
                self.question_cog_map_df.question_id == self.paper_sub_q_df.question_id
            ], how='left'
        ).select(self.question_cog_map_df.cognition_map_num)

        # 统计排序
        res_df = df.filter(filter_str).groupBy(
            "cognition_map_num"
        ).count().sort('count', ascending=False).limit(n)

        return res_df
