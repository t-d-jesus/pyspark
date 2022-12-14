from pyspark.sql import SparkSession,functions as f
from logger import Log4j


if __name__ == '__main__':

    spark = SparkSession.builder.config('spark.app.name','XXX').enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    log = Log4j(spark)

    cars_df = spark.read.option("multiline", "true").json(f'resources/cars.json')
    

    log.warn('========================Show cars table========================')


    cars_df.show()

    log.info('========================Count rows========================')
    print(f'Total rows: ${cars_df.count()}')
    cars_df.groupBy('Origin').agg(f.count('Origin')).show()

    
    log.info('========================Save the result========================')
    cars_df.write.format('parquet').mode('overwrite').save('output/grouped_estates/')


    spark.stop()

