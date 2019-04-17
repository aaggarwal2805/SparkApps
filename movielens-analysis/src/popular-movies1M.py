from pyspark import SparkConf, SparkContext

def movieNames():
    movieNames = {}
    with open("C:/SparkCourse/ml-100k/u.ITEM") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return(movieNames)
    
def parseLines(line):
    fields = line.split()
    movieID = int(fields[1])
    return (movieID)    

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)
    
nameDict = sc.broadcast(movieNames())

lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")
rdd = lines.map(parseLines)
movie = rdd.map(lambda x: (x, 1))
movieCount = movie.reduceByKey(lambda x, y: x + y)
flipped = movieCount.map(lambda x: (x[1], x[0]))
sortedMovie = flipped.sortByKey()
namedMovies = sortedMovie.map(lambda x : (nameDict.value[x[1]], x[0]))
result = namedMovies.collect()

for r in result:
    print(r)

            
