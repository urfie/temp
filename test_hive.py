"""
# 
"""

# import modules
from pyspark.sql import SparkSession

AppName = "training01"

def main():
	# start spark code

#	spark = SparkSession.builder.appName(AppName).config("hive.metastore.warehouse.dir","/home/hadoop/apache-hive-3.1.2-bin").getOrCreate()
	spark = SparkSession.builder \
		.appName(AppName) \
		.config("spark.sql.hive.metastore.jars","file:////home/hadoop/apache-hive-3.1.2-bin/lib/mysql-connector-java.jar") \
		.config("spark.sql.catalogImplementation","hive") \
		.getOrCreate()

#.config("spark.sql.hive.metastore.jars.path","file:////home/hadoop/apache-hive-3.1.2-bin/lib") \

	data = [['Agus','F',100,150,150],['Windy','F',200,150,180],
        	['Budi','B',200,100,150],['Dina','F',150,150,130],
        	['Bayu','F',50,150,100],['Dedi','B',50,100,100]]

	kolom = ["nama","kode_jurusan","nilai1","nilai2","nilai3"]
	df = spark.createDataFrame(data,kolom)
	df.show()

	spark.sql("show databases;").show()
	#spark.sql("create database test_submit;")
	#spark.sql("show databases;").show()
	spark.sql("show tables;")

	#df.write.mode('overwrite') \
        #  .saveAsTable("test_submit.mahasiswa")

	#spark.sql("select * from test_submit.mahasiswa").show()

	spark.stop()
	return None

# Starting point for PySpark
if __name__ == '__main__':
    main()
