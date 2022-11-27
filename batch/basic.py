from pyspark.sql import SparkSession


if __name__ == '__main__':
    print('teste')

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    df = spark.createDataFrame([(1,'User2'),(2,'User2')],['ID','Name'])
    df.show()

    spark.stop()