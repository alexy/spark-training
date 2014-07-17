import org.apache.spark.sql._
import org.apache.spark.mllib.linalg.{Vector,Vectors}
import org.apache.spark.mllib.clustering.KMeans 

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val tweets = sqlContext.jsonFile("/data/tweets100K.json")
// tweets.printSchema()
tweets.registerAsTable("tweets") 

sqlContext.sql("SELECT text FROM tweets LIMIT 5").collect.foreach(println) 
sqlContext.sql("SELECT lang, COUNT(*) AS cnt FROM tweets \
  GROUP BY lang ORDER BY cnt DESC LIMIT 10").collect.foreach(println) 
val texts = sqlContext.sql("SELECT text FROM tweets WHERE text IS NOT NULL").map(_.head.toString) 

def featurize(str: String): Vector = {
    val n = 1000
    val result = new Array[Double](n)
    val bigrams = str.sliding(2).toArray
    for (h <- bigrams.map(_.hashCode % n)) {
        result(h) += 1.0 / bigrams.length
    }
    Vectors.sparse(n, result.zipWithIndex.filter(_._1 != 0).map(_.swap))
} 


val vectors = texts.map(featurize).cache() 
val model = KMeans.train(vectors, 10, 10) 

sc.makeRDD(model.clusterCenters, 10).saveAsObjectFile("/data/tweets/model100K.rdd")
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
