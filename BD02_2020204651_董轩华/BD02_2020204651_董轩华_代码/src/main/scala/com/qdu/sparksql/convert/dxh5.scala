package com.qdu.sparksql.convert

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

case object dxh5{
  def main(args: Array[String]): Unit = {
    val sc1 = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val sc2 = SparkSession.builder().config(sc1).getOrCreate()
    val df1_user = sc2.read.option("header","true").option("inferSchema","true").csv("input/laptops.csv")
    df1_user.createOrReplaceTempView("laptops")
    val df2 = sc2.sql(
      """select
        |    case
        |        when Weight >= 0 and Weight < 1 THEN 'lightest'
        |        when Weight >= 1 and Weight < 1.5 THEN 'lighter'
        |        when Weight >= 1.5 and Weight < 2 THEN 'heavier'
        |        when weight >= 2 then 'heaviest'
        |    end as weight_level,
        |    count(*) as count
        |from
        |	laptops
        |group by weight_level
        |order by weight_level asc""".stripMargin)
    df2.show
    df2.write
      .format("jdbc")
      .option("url","jdbc:mysql://niit-master:3306/sem7")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("user","root")
      .option("password","root")
      .option("dbtable","dxh5")
      .mode(SaveMode.Append)
      .save()
  }
}

