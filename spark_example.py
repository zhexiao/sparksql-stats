# coding: utf-8
from spark_resource import SparkResource


class SparkExample(SparkResource):
    DEMO_JSON_FILE = '/opt/spark/examples/src/main/resources/people.json'

    def __init__(self):
        super(SparkExample, self).__init__()

    def run(self):
        df = self.spark_sql.spark.read.json(
            '/opt/spark/examples/src/main/resources/people.json'
        )
        df.createOrReplaceTempView("people")

        sql_df = self.spark_sql.spark.sql("SELECT * FROM people")
        sql_df.show()

    def test_temp_view(self):
        sql_df = self.spark_sql.spark.sql("SELECT * FROM people")
        sql_df.show()


if __name__ == '__main__':
    se = SparkExample()
    se.run()

    se1 = SparkExample()
    se1.test_temp_view()
