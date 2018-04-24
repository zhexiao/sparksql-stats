# coding: utf-8
from pyspark.sql.functions import broadcast
from spark_resource import SparkResource
from spark_exceptions import ResourceError


class SparkQuestion(SparkResource):
    """
    DEMO:
    s_question = SparkQuestion()
    diff_df = s_question.get_question_diff_distri(faculty=3, subject=1)
    print(diff_df.toJSON().collect())
    """

    def __init__(self):
        super(SparkQuestion, self).__init__()

    def get_question_diff_distri_sqlv(self, faculty=None, subject=None):
        """
        得到某学科和学段下面试题的困难度分布 SQL版本
        :param faculty:
        :param subject:
        :return:
        """
        if not faculty or not subject:
            raise ResourceError('缺少faculty或者subject')

        sql_string = """
        SELECT COUNT(`diff`) as `count`, `diff` 
        FROM tmp_question
        WHERE `faculty`={0} AND `subject`={1}
        GROUP BY `diff`
        """.format(faculty, subject)

        res_df = self.spark_sql.spark.sql(sql_string)
        return res_df

    def get_question_freq_top_n_sqlv(self, n=20, faculty=None, subject=None):
        """
        得到某学科和学段下面试卷的试题使用频繁度 SQL版本
        :param n:
        :param faculty:
        :param subject:
        :return:
        """
        if not faculty or not subject:
            raise ResourceError('缺少faculty或者subject')

        sql_string = """
        SELECT `question_id`, COUNT(`question_id`) as `count` 
        FROM tmp_paper_subtype_question tpsq
        LEFT JOIN tmp_question tq
        ON tpsq.question_id = tq.qid
        WHERE `faculty`={0}
        and `subject`={1}
        and `structure_string` IS NOT NULL
        GROUP BY `question_id`
        ORDER BY `count` DESC
        LIMIT {2}
        """.format(faculty, subject, n)

        res_df = self.spark_sql.spark.sql(sql_string)
        return res_df

    def get_question_diff_distri(self, faculty=None, subject=None):
        """
        得到某学科和学段下面试题的困难度分布
        :param faculty:
        :param subject:
        :return:
        """
        if not faculty or not subject:
            raise ResourceError('缺少faculty或者subject')

        filter_str = "faculty = {0} and subject = {1}".format(
            faculty, subject
        )

        # 统计排序
        res_df = self.question_df.filter(filter_str).groupBy(
            "diff"
        ).count()

        return res_df

    def get_question_freq_top_n(self, n=20, faculty=None, subject=None):
        """
        得到某学科和学段下面试卷的试题使用频繁度
        :param n:
        :param faculty:
        :param subject:
        :return:
        """
        if not faculty or not subject:
            raise ResourceError('缺少faculty或者subject')
        filter_str = (
            "faculty = {0} and subject = {1} and "
            "structure_string IS NOT NULL".format(faculty, subject)
        )

        df = broadcast(self.paper_sub_q_df).join(
            self.question_df, on=[
                self.paper_sub_q_df.question_id == self.question_df.qid
            ], how='left'
        ).select(self.paper_sub_q_df.question_id)

        # 统计排序
        res_df = df.filter(filter_str).groupBy(
            "question_id"
        ).count().sort('count', ascending=False).limit(n)

        return res_df
