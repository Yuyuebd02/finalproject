package com.qdu.sparksql.convert

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object yuyue4 {
  def main(args: Array[String]): Unit = {
    val sc1 = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val sc2 = SparkSession.builder().config(sc1).getOrCreate()
    val df1_song = sc2.read.option("header", "true").option("inferSchema", "true").csv("input/athlete_events.csv")
    df1_song.createOrReplaceTempView("athlete_events")
    val df2 = sc2.sql(
      """select
        |    case
        |        when age >= 0 and age < 20 THEN '0-19'
        |        when age >= 20 and age < 30 THEN '20-29'
        |        when age >= 30 and age < 40 THEN '30-39'
        |        when age >= 40 and age < 50 THEN '40-49'
        |        when age >= 50 then '50+'
        |    end as age_level,
        |    count(*) as count
        |from
        |	athlete_events
        |group by age_level
        |order by age_level asc""".stripMargin)
    df2.show
    df2.write
      .format("jdbc")
      .option("url","jdbc:mysql://niit-master:3306/sem7")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("user","root")
      .option("password","root")
      .option("dbtable","yuyue4")
      .mode(SaveMode.Append)
      .save()

  }

}
