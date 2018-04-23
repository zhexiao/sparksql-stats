# coding: utf-8
from pyspark.sql.functions import broadcast
from spark_resource import SparkResource
from spark_exceptions import ResourceError


class SparkPaper(SparkResource):
    def __init__(self):
        super(SparkPaper, self).__init__()

    def get_paper_info(self, paper_id=None):
        if not paper_id:
            raise ResourceError('缺少paper_id')

        # 读取表的dataframe
        sub_q_df = self.spark_sql.load_table_df('paper_subtype_question')
        q_map_df = self.spark_sql.load_table_df('question_cognition_map')

        df = broadcast(sub_q_df).filter(
            sub_q_df.paper_id == paper_id
        ).join(
            q_map_df, on=[
                sub_q_df.question_id == q_map_df.question_id
            ], how='left'
        ).select(q_map_df.cognition_map_num)

        # 统计排序
        res_df = df.groupBy(
            "cognition_map_num"
        ).count().sort('count', ascending=False)

        return res_df
