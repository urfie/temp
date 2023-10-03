"""
# 
"""

# import modules
from pyspark.sql import SparkSession

AppName = "training01"

def main():
    # start spark code
    spark = SparkSession.builder.appName(AppName).getOrCreate()
    
    #do something here
    logger.info("Reading CSV File")
    #df_category = spark.read.option("delimiter","|").csv("hdfs:///var/data/category_pipe.txt")
    logger.info("Previewing CSV File Data")
    #df_category.show(truncate=False)

    logger.info("Ending spark application")
    # end spark code
    spark.stop()
    return None

# Starting point for PySpark
if __name__ == '__main__':
    main()
    sys.exit()