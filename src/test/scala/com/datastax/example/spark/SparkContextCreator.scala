package com.datastax.example.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by angela on 5/2/16.
  */
trait SparkContextCreator {
  def creatSparkContext(args:Array[String], appName:String)= {
    val isLocal= args.length==0
    val jars = if (isLocal) List() else List(SparkContext.jarOfObject(this).get)

    val sc: SparkContext = {
      val conf = new SparkConf()
        .setAppName(appName)
        .setJars(jars)
        .set("spark.driver.allowMultipleContexts", "true")
        .set("spark.cassandra.connection.host", "104.196.110.202")
      if (isLocal)
        conf.setMaster("local")
      new SparkContext(conf)
    }
    sc

  }
}
