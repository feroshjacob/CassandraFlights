package com.datastax.example.spark

import java.util.Date

import com.datastax.spark.connector._

/**
  * Created by angela on 5/2/16.
  */
object CassandraUpdateCheck extends SparkContextCreator {



  def main(args: Array[String]): Unit = {
    val sc = creatSparkContext(args, "CassandraUpdateCheck")
    val rdd =
      sc.cassandraTable("flight", "flights")


      val bosCount=rdd
      .filter(f=>    f.get[String]("origin")=="BOS" || f.get[String]("dest")=="BOS"   )
      .count()

      val tstCount = rdd
        .filter(f=>    f.get[String]("origin")=="TST" || f.get[String]("dest")=="TST"   )
        .count()
    println(bosCount,tstCount)
  }

}
