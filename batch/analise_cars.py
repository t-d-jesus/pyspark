from pyspark.sql import SparkSession


if __name__ == '__main__':

    spark = SparkSession.builder.config('spark.app.name','Cars Analises').enableHiveSupport().getOrCreate()

    cars_df = spark.read.option("multiline", "true") .json('../resources/cars.json')

    cars_df.show()

    
    print(cars_df.count())




    spark.stop()