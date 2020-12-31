from main import computeDF
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, lit

def are_dfs_equal(df1, df2):
    if df1.collect() != df2.collect():
        return False
    return True

def test():
    spark = SparkSession \
            .builder \
            .appName("tests-lab2").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    testData = [(1,1510670916247,10),
        (1,1510670916249,5),
        (1,1510600916251,10),
        (2,1510670916253,15),
        (1,1510600916255,5)
    ]

    expectedResult = [
        (1,1510670880000,15,'1m'),
        (1,1510600860000,15,'1m'),
        (2,1510670880000,15,'1m')
    ]

    columns1 = ["id","timestamp","value"]
    df = spark.createDataFrame(data=testData, schema = columns1)

    columns2 = ["id","timestamp","value","scale"]
    expectedDF = spark.createDataFrame(data=expectedResult, schema = columns2)


    computedDF = computeDF(df, 60, '1m')
    computedDF.show()
    computedDF.printSchema()
    expectedDF.show()
    expectedDF.printSchema()

    if are_dfs_equal(computedDF, expectedDF):
        print('TEST OK! Dataframes are equal!')
    else:
        print('BAD TEST! Dataframes are not equal!')

test()