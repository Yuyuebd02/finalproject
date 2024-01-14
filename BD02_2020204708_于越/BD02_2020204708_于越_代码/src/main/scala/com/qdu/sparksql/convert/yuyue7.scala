package com.qdu.sparksql.convert

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object yuyue7 {
  def main(args: Array[String]): Unit = {
    val sc1 = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val sc2 = SparkSession.builder().config(sc1).getOrCreate()
    //导入数据
    val df1_user = sc2.read.option("header","true").option("inferSchema","true").csv("input/health.csv")
    df1_user.createOrReplaceTempView("health")
    val df2 = sc2.sql(
      """select NObeyesdad,count(*) from health
        |group by NObeyesdad order by NObeyesdad""".stripMargin)

    df2.write
      .format("jdbc")
      .option("url","jdbc:mysql://niit-master:3306/sem7")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("user","root")
      .option("password","root")
      .option("dbtable","yuyue7")
      .mode(SaveMode.Append)
      .save()


  }
}
