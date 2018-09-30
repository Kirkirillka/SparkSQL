import org.apache.spark.sql.types.{FloatType,_}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.functions._

import scala.reflect.internal.util.TableDef.Column

package gdelt.utils {

  class GDELTdata(sparkSession: SparkSession, input: String) {
    /*sql scheme of GDELT dataset*/
    var gdeltSchema = StructType(List(
      StructField("GLOBALEVENTID", IntegerType, true),
      StructField("SQLDATE", IntegerType, true),
      StructField("MonthYear", IntegerType, true),
      StructField("Year", IntegerType, true),
      StructField("FractionDate", DoubleType, true),
      StructField("Actor1Code", StringType, true),
      StructField("Actor1Name", StringType, true),
      StructField("Actor1CountryCode", StringType, true),
      StructField("Actor1KnownGroupCode", StringType, true),
      StructField("Actor1EthnicCode", StringType, true),
      StructField("Actor1Religion1Code", StringType, true),
      StructField("Actor1Religion2Code", StringType, true),
      StructField("Actor1Type1Code", StringType, true),
      StructField("Actor1Type2Code", StringType, true),
      StructField("Actor1Type3Code", StringType, true),
      StructField("Actor2Code", StringType, true),
      StructField("Actor2Name", StringType, true),
      StructField("Actor2CountryCode", StringType, true),
      StructField("Actor2KnownGroupCode", StringType, true),
      StructField("Actor2EthnicCode", StringType, true),
      StructField("Actor2Religion1Code", StringType, true),
      StructField("Actor2Religion2Code", StringType, true),
      StructField("Actor2Type1Code", StringType, true),
      StructField("Actor2Type2Code", StringType, true),
      StructField("Actor2Type3Code", StringType, true),
      StructField("IsRootEvent", StringType, true),
      StructField("EventCode", IntegerType, true),
      StructField("EventBaseCode", StringType, true),
      StructField("EventRootCode", StringType, true),
      StructField("QuadClass", StringType, true),
      StructField("GoldsteinScale", DoubleType, true),
      StructField("NumMentions", IntegerType, true),
      StructField("NumSources", IntegerType, true),
      StructField("NumArticles", IntegerType, true),
      StructField("AvgTone", DoubleType, true),
      StructField("Actor1Geo_Type", StringType, true),
      StructField("Actor1Geo_FullName", StringType, true),
      StructField("Actor1Geo_CountryCode", StringType, true),
      StructField("Actor1Geo_ADM1Code", StringType, true),
      StructField("Actor1Geo_Lat", FloatType, true),
      StructField("Actor1Geo_Long", FloatType, true),
      StructField("Actor1Geo_FeatureID", StringType, true),
      StructField("Actor2Geo_Type", StringType, true),
      StructField("Actor2Geo_FullName", StringType, true),
      StructField("Actor2Geo_CountryCode", StringType, true),
      StructField("Actor2Geo_ADM1Code", StringType, true),
      StructField("Actor2Geo_Lat", FloatType, true),
      StructField("Actor2Geo_Long", FloatType, true),
      StructField("Actor2Geo_FeatureID", StringType, true),
      StructField("ActionGeo_Type", StringType, true),
      StructField("ActionGeo_FullName", StringType, true),
      StructField("ActionGeo_CountryCode", StringType, true),
      StructField("ActionGeo_ADM1Code", StringType, true),
      StructField("ActionGeo_Lat", FloatType, true),
      StructField("ActionGeo_Long", FloatType, true),
      StructField("ActionGeo_FeatureID", StringType, true),
      StructField("DATEADDED", IntegerType, true),
      StructField("SOURCEURL", StringType, true)
    ))

    var eventSchema = StructType(List(
      StructField("id",IntegerType,false),
      StructField("EventDescription", StringType,true)
    ))
    /* initilize SQLContext of GDELT */
    var gdelt = sparkSession.read
      .option("header", "false")
      .option("delimiter", "\t")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .schema(this.gdeltSchema)
      .csv(input)

    gdelt.printSchema()
    gdelt.show(50)


    def show_usa_or_russia(): Unit ={
      gdelt.filter("Actor1CountryCode in ('USA','RUSSIA')").show()
    }

    def most_mentioned_users(): Unit ={
      return
    }

    def join_with_event_desc(): DataFrame ={
      val descCSV = "data/CAMEO_event_codes.csv"

      val events = sparkSession.read
          .option("header","true")
          .option("delimiter","\t")
          .schema(this.eventSchema)
          .csv(descCSV)

      return gdelt.join(events,col("EventCode").===(col("id"))).filter("EventDescription is not null")
    }

    def get_top10_events(): Unit ={
      val events = this.join_with_event_desc()

      events.groupBy("EventCode","EventDescription").count().orderBy(desc("count")).select("EventCode","EventDescription","count").show()
    }

  }
}