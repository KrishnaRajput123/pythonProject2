from pyspark.sql import *
from pyspark.sql.functions import *
#
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
# sample_data = [('Ram','','Aggarwal','1981-06-02','M',4000),
#   ('Shyam','Gupta','','2005-04-02','M',5000),
#   ('Amit','','Jain','1988-07-02','M',5000),
#   ('Pooja','Raju','Bansal','1977-08-03','F',5000),
#   ('Mary','Yadav','Brown','1970-04-15','F',-2)
# ]
#
# sample_columns = ["firstname","middlename","lastname","dob","gender","salary"]
# dataframe = spark.createDataFrame(data = sample_data, schema = sample_columns)
# dataframe.printSchema()
# dataframe.show(truncate=False)
import re
data="E:/SPARK/datasets/venu.txt"
df=spark.read.format("csv").option("header","true").option("sep",",").option("inferSchema","true").load(data)
#df.show()
def fun(col,frmt=("dd-MM-yyyy","yyyy-MM-dd","dd-MMM-yyyy","ddMMMMyyyy","MMM/yyyy/dd")):
  return coalesce(*[to_date(col,i)for i in frmt])

cols=[re.sub('[^a-zA-Z0-9]',"",c) for c in df.columns]
ndf=df.toDF(*cols)
#ndf.show()
res=ndf.withColumn("birthdob",fun(col("birthdob")))
res.show()