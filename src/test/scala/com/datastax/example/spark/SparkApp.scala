package com.datastax.example.spark

import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
/**
  * Created by angela on 5/2/16.
  */
object SparkApp  {


  def creatSparkContext(args:Array[String])= {
    val isLocal= args.length==0
    val jars = if (isLocal) List() else List(SparkContext.jarOfObject(this).get)

    val sc: SparkContext = {
      val conf = new SparkConf()
        .setAppName("CassandraSparkApp")
        .setJars(jars)
        .set("spark.driver.allowMultipleContexts", "true")
        .set("spark.cassandra.connection.host", "104.196.110.202")
      if (isLocal)
        conf.setMaster("local")
      new SparkContext(conf)
    }
    sc

  }
  def main(args: Array[String]): Unit = {
    val sc = creatSparkContext(args)
    val rdd =
      sc.cassandraTable("flight", "flights")
      .filter(f=>    f.get[String]("origin")=="BOS" || f.get[String]("dest")=="BOS"   )
       .map(f=> (
   f.get[Int]("id"),
   f.get[Int]("year"),
   f.get[Int]("day_of_month"),
   f.get[Date]("fl_date"),
   f.get[Int]("airline_id"),
   f.get[String]("carrier"),
   f.get[Int]("fl_num"),
   f.get[Int]("origin_airport_id"),
   "TST",
   f.get[String]("origin_city_name"),
   f.get[String]("origin_state_abr"),
   "TST",
   f.get[String]("dest_city_name"),
   f.get[String]("dest_state_abr"),
   f.get[Date]("dep_time"),
   f.get[Date]("arr_time"),
   f.get[Date]("actual_elapsed_time"),
   f.get[Date]("air_time"),
   f.get[Int]("distance")
       ))
        .saveToCassandra("flight", "flights", SomeColumns(
          "id",
          "year",
          "day_of_month",
          "fl_date",
          "airline_id",
          "carrier",
          "fl_num",
          "origin_airport_id",
          "origin",
          "origin_city_name",
          "origin_state_abr",
          "dest",
          "dest_city_name",
          "dest_state_abr",
          "dep_time",
          "arr_time",
          "actual_elapsed_time",
          "air_time",
          "distance"))

  }

}
