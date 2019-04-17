from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import asc, desc, sum, count
import collections


spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("PharmaSparkSQL").getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(C_ID = int(fields[0]), C_FIRST_NAME = str(fields[1]), C_LAST_NAME = str(fields[2]), DRUG_NAME = str(fields[3]), DRUG_COST = float(fields[4]))
    
lines = spark.sparkContext.textFile("C:/Users/aagga/Documents/SmallProjects/SparkApps/parmacy-counting/input/itcont.txt")
#lines = spark.sparkContext.textFile("C:/Users/aagga/Documents/SmallProjects/SparkApps/parmacy-counting/input/de_cc_data.txt")

data = lines.map(mapper)
 
schemaData = spark.createDataFrame(data).cache()
schemaData.createOrReplaceTempView("data")
 
output = spark.sql("SELECT DRUG_NAME, sum(DRUG_COST) as TOTAL_COST, count(DRUG_NAME) as NO_OF_PRISCRIBERs FROM data group by DRUG_NAME")
counting = output.orderBy(desc("TOTAL_COST"))
final = counting.orderBy("DRUG_NAME")
final.show()

#final.rdd.map(lambda x: ",".join(map(str, x))).coalesce(1).saveAsTextFile("file.csv")





  
