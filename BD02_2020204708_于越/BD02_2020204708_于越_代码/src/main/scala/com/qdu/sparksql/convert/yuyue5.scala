package com.qdu.sparksql.convert

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

case object yuyue5{
  def main(args: Array[String]): Unit = {
    val sc1 = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val sc2 = SparkSession.builder().config(sc1).getOrCreate()
    val df1_user = sc2.read.option("header","true").option("inferSchema","true").csv("input/athlete_events.csv")
    df1_user.createOrReplaceTempView("athlete_events")
    val df2 = sc2.sql(
      """select year,count(*) as event_num,count(DISTINCT id) as athlete_num from athlete_events
        |where team='China' and Games like '%Summer'
        |group by year order by year""".stripMargin)
    df2.show
    df2.write
      .format("jdbc")
      .option("url","jdbc:mysql://niit-master:3306/sem7")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("user","root")
      .option("password","root")
      .option("dbtable","yuyue5")
      .mode(SaveMode.Append)
      .save()
  }
}

