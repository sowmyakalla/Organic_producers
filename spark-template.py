from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

USER = 'sowmya-kalla'
APP_NAME = 'content'
INPUT_FILE = 'hdfs:///data/content.parquet'
OUTPUT_FILE = 'sortedresult.csv'
OUTPUT_PATH = 'hdfs:///user/'+USER+'/'+OUTPUT_FILE

conf = SparkConf().setAppName(APP_NAME).setMaster('yarn')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

df = sqlContext.read.parquet(INPUT_FILE)

df1 = df.coalesce(1)
df1.write.csv(OUTPUT_PATH, mode='overwrite', header=True)
