from pyspark.sql import SparkSession
from pyspark.sql import functions as f


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()

    # d = {
    #     'value': {
    #         'a': 123,
    #         'b': 'asd',
    #         'c': {
    #             'd': None,
    #             'e': 'eee'
    #         }
    #     }
    # }


    json_schema_string = [("""{ "a": 123, "b": "asd", "c": { "d": "ddd", "e": "eee" } }""")]
    json_df = spark.read.json(spark.sparkContext.parallelize(json_schema_string))
    print(json_df)
    json_schema_rdd = json_df.schema
    print(json_schema_rdd)

    json_string = """ { "a": 123, "b": "asd", "c": { "d": "ddd", "e": "eee" } } """
    df = spark.createDataFrame([(1, json_string)], ['id', 'value'])

    df_with_schema = df.select(f.from_json(col='value', schema=json_schema_rdd).alias('value')).select('value.*')
    df_with_schema.show(truncate=False)
    df_with_schema.printSchema()


    print( type(spark.sparkContext.parallelize(json_schema_string) ) )
    print(spark.sparkContext.parallelize(json_schema_string).collect())

    print('----------------------------------------------------------------------------------------')

    json_schema_string = [("""{ "a": 123, "b": "asd", "c": { "d": "ddd", "e": "eee" } }""",1)]
    df_json = spark.createDataFrame(json_schema_string,['json','id']).drop('id')
    df_json.show()
    df_json.printSchema()

    print( spark.read.json(df_json.rdd.map(lambda row: row.json)).schema )
    spark.read.json(df_json.rdd.map(lambda row: row.json)).show()
    spark.read.json(df_json.rdd.map(lambda row: row.json)).printSchema()

    json_schema = spark.read.json(df_json.rdd.map(lambda row: row.json)).schema
    df_json.withColumn('new_json',f.from_json('json',json_schema)).show()
    df_json.withColumn('new_json',f.from_json('json',json_schema)).printSchema()
    df_json.withColumn('new_json',f.from_json('json',json_schema)).select('new_json.*').show()
    df_json.withColumn('new_json',f.from_json('json',json_schema)).select('new_json.*').printSchema()

    spark.stop()