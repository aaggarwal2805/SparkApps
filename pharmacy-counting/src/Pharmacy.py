from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster("local[*]").setAppName("PharmacyCounting")
sc = SparkContext(conf = conf)

def parselines(line):
    fields = line.split(',')
    drug_name = (fields[-2])
    drug_cost = float(fields[-1])
    return (drug_name, drug_cost)

lines = sc.textFile("C:/Users/aagga/Documents/SmallProjects/SparkApps/parmacy-counting/input/itcont.txt")
# lines = sc.textFile("C:/Users/aagga/Documents/SmallProjects/SparkApps/parmacy-counting/input/de_cc_data.txt")

rdd = lines.map(parselines)
catalog = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y : (x[0] +y[0], x[1] + y[1]))
#flipped = catalog.map(lambda x, y: (y, x))
sorted_catalog = catalog.sortBy(lambda x: (-1*x[1][0], x[0]))
final_catalog = sorted_catalog.collect()
counter = 0
for f in final_catalog:
    if counter > 10:
        break
    
    print(f)
    counter +=1
