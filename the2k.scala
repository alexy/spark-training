import org.apache.spark.sql._ 
val ctx = new org.apache.spark.sql.SQLContext(sc) 
val tweets = sc.textFile("hdfs:/twitter") 
val tweetTable = JsonTable.fromRDD(sqlContext, tweets, Some(0.1)) 
tweetTable.registerAsTable("tweetTable") 

ctx.sql("SELECT text FROM tweetTable LIMIT 5").collect.foreach(println) 
ctx.sql("SELECT lang, COUNT(*) AS cnt FROM tweetTable \
  GROUP BY lang ORDER BY cnt DESC LIMIT 10").collect.foreach(println) 
val texts = sql("SELECT text FROM tweetTable").map(_.head.toString) 

def featurize(str: String): Vector = { ... } 
val vectors = texts.map(featurize).cache() 
val model = KMeans.train(vectors, 10, 10) 

sc.makeRDD(model.clusterCenters, 10).saveAsObjectFile("hdfs:/model")
val ssc = new StreamingContext(new SparkConf(), Seconds(1)) 
  
val model = new KMeansModel(
    ssc.sparkContext.objectFile(modelFile).collect())

// Streaming
val tweets = TwitterUtils.createStream(ssc, /* auth */) 
val statuses = tweets.map(_.getText) 
val filteredTweets = statuses.filter { 
    t => model.predict(featurize(t)) == clusterNumber 
} 
filteredTweets.print() 
  
ssc.start()
