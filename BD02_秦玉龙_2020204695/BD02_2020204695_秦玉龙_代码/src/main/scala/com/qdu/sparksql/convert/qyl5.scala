package com.qdu.sparksql.convert

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

case object qyl5{
  def main(args: Array[String]): Unit = {
    val sc1 = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val sc2 = SparkSession.builder().config(sc1).getOrCreate()
    val df1_user = sc2.read.option("header","true").option("inferSchema","true").csv("input/events.csv")
    df1_user.createOrReplaceTempView("events")

    val df2 = sc2.sql(
      """select shot_outcome,count(shot_outcome) as num from events
        |group by shot_outcome""".stripMargin)
    df2.show
    df2.write
      .format("jdbc")
      .option("url","jdbc:mysql://niit-master:3306/sem7")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("user","root")
      .option("password","root")
      .option("dbtable","qyl5")
      .mode(SaveMode.Append)
      .save()
  }
}

