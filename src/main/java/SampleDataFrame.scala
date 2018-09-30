import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.reflect.internal.util.TableDef.Column



object SparkSessionExample {

  def main(args: Array[String]): Unit = {

    // Suppress unessesary log output
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)



    val jsonFile = "data/sampletweets.json"

    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("spark-sql-basic")
      .master("local[*]")
      .getOrCreate()

    //Read json file to DF
    val tweets = sparkSession.read.json(jsonFile)
    tweets.show(5)

    //Print the structure
//    tweets.printSchema()
//
//    //Select all tweets message
//    val tweet_messages = tweets.select("body")
//    tweet_messages.show(10)
//
//    //Count the most active languages
//    val most_active_languages = tweets.groupBy("twitter_lang").count().sort("Count")
//    most_active_languages.show(1000)
//
//
//    //Get earliest and latest tweet dates
//    var twitter_times = tweets
//      .filter("postedTime is not null")
//      .filter("body is not null")
//      .orderBy(asc("postedTime"))
//      .dropDuplicates("postedTime")
//    var earliest_tweet = twitter_times.select("body","postedTime").first()
//    var latest_tweet = twitter_times.select("body","postedTime").orderBy(desc("postedTime")).first()
//    println(earliest_tweet)
//    println(latest_tweet)


//    //Get top devices used amoung all Twitter users
//    var top_devices = tweets.select("generator").filter("generator is not null").orderBy("generator").groupBy("generator").count().sort(desc("count"))
//    top_devices.show()

//    //Get all tweets by user
//    val username = "Людмила Воронина"
//    var tweets_by_user = tweets
//
//    tweets_by_user.withColumn("username",col("actor.displayName")).select("username","body").filter("username == '%s'".format(username)).show()
//

//    //Count how many tweets each user has
//      var tweets_count_by_user = tweets.withColumn("username",col("actor.displayName")).groupBy("username").count()
//      tweets_count_by_user.show()

    //Show top 10 mentioned people
    var tweets_top_10 = tweets.withColumn("username",col("actor.displayName")).groupBy("username").count()
    //

  }
}
