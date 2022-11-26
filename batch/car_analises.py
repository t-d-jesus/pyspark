from pyspark.sql import SparkSession,functions as f

import os
if __name__ == '__main__':

    spark = SparkSession.builder.config('spark.app.name','Cars Analises').enableHiveSupport().getOrCreate()
    print(os.getcwd())
    cars_df = spark.read.option("multiline", "true").json(f'resources/cars.json')
    
    cars_df.show()

    #Count rows
    print(f'Total rows: ${cars_df.count()}')
    cars_df.groupBy('Origin').agg(f.count('Origin')).show()

    #Save the result
    cars_df.write.format('parquet').mode('overwrite').save('output/grouped_estates/')


    spark.stop()

