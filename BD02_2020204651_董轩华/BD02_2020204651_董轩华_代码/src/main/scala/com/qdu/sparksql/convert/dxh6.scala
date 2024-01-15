package com.qdu.sparksql.convert

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object dxh6 {
  def main(args: Array[String]): Unit = {
    val sc1 = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val sc2 = SparkSession.builder().config(sc1).getOrCreate()
    val df1_song = sc2.read.option("header", "true").option("inferSchema", "true").csv("input/electriccardata_clean.csv")
    df1_song.createOrReplaceTempView("electriccardata_clean")
    val df2 = sc2.sql(
    """select brand,avg(PriceEuro)avg,min(PriceEuro)from electriccardata_clean group by Brand""".stripMargin)
    df2.show
    df2.write
      .format("jdbc")
      .option("url","jdbc:mysql://niit-master:3306/sem7")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("user","root")
      .option("password","root")
      .option("dbtable","dxh6")
      .mode(SaveMode.Append)
      .save()
  }
}
