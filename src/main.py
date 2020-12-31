from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, LongType, IntegerType
from pyspark.sql.functions import col, lit
import sys

def get_scale(seconds):
    """ 
    This method converts time in second to minuties or minuts plus seconds

    Attributes:
    __________
    seconds : int 
        amount of seconds

    Returns
    _______
    str
        a string which represents minutes and/or seconds
    """

    if seconds % 60 == 0:
        return str(seconds / 60) + 'm'
    else:
        return str(seconds // 60) + 'm' + str(seconds % 60) + 's'

def computeDF(df, interval, scale):
    """ 
    This method aggregate metrics in given interval by timestamp

    Attributes:
    __________
    df : Spark DataFrame
        initiat DataFrame
    interval : int
        time interval for aggregation
    scale : str
        time scale

    Returns
    _______
    df : Spark DataFrame
        resulting DataFrame
    """
    
    df = df.withColumn('timestamp', (col('timestamp') / 1000 / interval).cast('long') * interval * 1000)
    df = df.groupBy("id", "timestamp").sum("value").withColumn('scale', lit(scale))
    df = df.withColumnRenamed('sum(value)', 'value')
    return df

def main(argv):
    timeInterval = int(argv[0])
    scale = get_scale(timeInterval)
    fileFrom = argv[1]
    fileTo = argv[2]

    spark = SparkSession \
        .builder \
        .appName("lab2").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR") 

    schema = StructType() \
      .add("id",IntegerType(),True) \
      .add("timestamp",LongType(),True) \
      .add("value",IntegerType(),True)

    initDF = spark.read.format('csv').option('header', True).schema(schema).load(fileFrom)
    initDF.printSchema()
    initDF.show(truncate=False)

    resultDF = computeDF(initDF, timeInterval, scale)
    resultDF.show(truncate=False)
    resultDF.write.csv(fileTo)

if __name__ == "__main__":
    main(sys.argv[1:])
