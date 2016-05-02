package com.datastax.example.spark

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
      sc.cassandraTable("sparktest", "kv")
      .filter(f=>    f.get[Int]("value")==10  )
        .map(f=>   (f.get[String]("key").toString , f.get[Int]("value") *100)  )
        .saveToCassandra("sparktest", "kv", SomeColumns("key", "value"))
  }

}
