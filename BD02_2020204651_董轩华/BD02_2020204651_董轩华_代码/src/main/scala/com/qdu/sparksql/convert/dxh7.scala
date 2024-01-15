package com.qdu.sparksql.convert

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object dxh7 {
  def main(args: Array[String]): Unit = {
    val sc1 = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val sc2 = SparkSession.builder().config(sc1).getOrCreate()
    //导入数据
    val df1_user = sc2.read.option("header","true").option("inferSchema","true").csv("input/electriccardata_clean.csv")
    df1_user.createOrReplaceTempView("electriccardata_clean")
    val df2 = sc2.sql(
      """SELECT Brand, Model, COUNT(*) AS TotalCount
        |FROM electriccardata_clean
        |WHERE BodyStyle LIKE '%Sedan%' AND Range_Km > 200
        |GROUP BY Brand, Model""".stripMargin)

    df2.write
      .format("jdbc")
      .option("url","jdbc:mysql://niit-master:3306/sem7")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("user","root")
      .option("password","root")
      .option("dbtable","dxh7")
      .mode(SaveMode.Append)
      .save()


  }
}
