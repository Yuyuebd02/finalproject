package com.qdu.sparksql.convert

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object yuyue2 {
  def main(args: Array[String]): Unit = {
    val sc1 = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val sc2 = SparkSession.builder().config(sc1).getOrCreate()

    //导入数据
    val df1_user = sc2.read.option("header","true").option("inferSchema","true").csv("input/athlete_events.csv")
    df1_user.createOrReplaceTempView("athlete_events")
    val df2 = sc2.sql(
      """select team,count(*) as count from athlete_events where medal!='NA' and year=2008 group by team""".stripMargin)
    df2.show
    df2.write
      .format("jdbc")
      .option("url","jdbc:mysql://niit-master:3306/sem7")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("user","root")
      .option("password","root")
      .option("dbtable","yuyue2")
      .mode(SaveMode.Append)
      .save()
  }
}
