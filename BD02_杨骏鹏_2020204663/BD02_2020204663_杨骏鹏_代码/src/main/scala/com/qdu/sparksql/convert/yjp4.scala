package com.qdu.sparksql.convert

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object yjp4 {
  def main(args: Array[String]): Unit = {
    val sc1 = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val sc2 = SparkSession.builder().config(sc1).getOrCreate()
    val df1_song = sc2.read.option("header", "true").option("inferSchema", "true").csv("input/area_weather.csv")
    df1_song.createOrReplaceTempView("area_weather")
    val df2 = sc2.sql(
      """SELECT
        |    CASE
        |        WHEN humidity >= 0 AND humidity < 50 THEN 'Low Polution'
        |        WHEN humidity >= 50 AND humidity < 100 THEN 'Medium Polution'
        |        WHEN humidity >= 100 THEN 'Bad Polution'
        |    END AS Polution,
        |    COUNT(*) AS count
        |FROM
        |    area_weather
        |GROUP BY Polution
        |order by Polution asc""".stripMargin)
    df2.show
    df2.write
      .format("jdbc")
      .option("url","jdbc:mysql://niit-master:3306/sem7")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("user","root")
      .option("password","root")
      .option("dbtable","yjp4")
      .mode(SaveMode.Append)
      .save()

  }

}
