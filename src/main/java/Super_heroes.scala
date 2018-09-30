import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level


object Super_heroes extends App{

  // Suppress unessesary log output
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)


  var conf = new SparkConf()
  conf.setAppName("Datasets Test")
  conf.setMaster("local[2]")

  val sc = new SparkContext(conf)
  val rows = sc.textFile("data/superheros.csv")

  //Superheroes
  var heros = rows.map(x => x.split(";")).map(x => (x(1),x(2) toInt))
  heros.foreach(println)
  println()

  //Total count of each heros
  var heros_count = heros.map(x=>(x._1,1)).reduceByKey(_+_)

  //Total count of killed enemies for each of hero
  var killed_count = heros.reduceByKey(_+_)



  println("Total count of each heros:")
  heros_count.foreach(println)
  println()

  println("Total count of killed enemies for each heros:")
  killed_count.foreach(println)
  println()

}
