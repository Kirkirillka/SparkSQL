import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.reflect.internal.util.TableDef.Column

import gdelt.utils.GDELTdata



object GDELT_analysis {

  def main(args: Array[String]): Unit = {

    // Suppress unessesary log output
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val csvFile = "data/gdelt.csv"

    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("spark-gdelt-analysis")
      .master("local[*]")
      .getOrCreate()


    val gdelt = new GDELTdata(sparkSession,csvFile)

    // All events in USA or RUSSIA
    gdelt.show_usa_or_russia()
    gdelt.join_with_event_desc()
    gdelt.get_top10_events()

  }
}