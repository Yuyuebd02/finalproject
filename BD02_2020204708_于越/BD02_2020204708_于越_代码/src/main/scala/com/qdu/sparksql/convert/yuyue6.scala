package com.qdu.sparksql.convert

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object yuyue6 {
  def main(args: Array[String]): Unit = {
    val sc1 = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val sc2 = SparkSession.builder().config(sc1).getOrCreate()
    val df1_song = sc2.read.option("header", "true").option("inferSchema", "true").csv("input/athlete_events.csv")
    df1_song.createOrReplaceTempView("athlete_events")
    val df2 = sc2.sql(
    """SELECT Sport,count(*) as Medal_num  FROM athlete_events where team='China' and medal='Gold' GROUP BY Sport""".stripMargin)
    df2.show
    df2.write
      .format("jdbc")
      .option("url","jdbc:mysql://niit-master:3306/sem7")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("user","root")
      .option("password","root")
      .option("dbtable","yuyue6")
      .mode(SaveMode.Append)
      .save()
  }
}
