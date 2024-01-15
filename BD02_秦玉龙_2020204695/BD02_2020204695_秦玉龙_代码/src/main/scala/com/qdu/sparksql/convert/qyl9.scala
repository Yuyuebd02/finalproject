package com.qdu.sparksql.convert

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object qyl9 {
  def main(args: Array[String]): Unit = {
    val sc1 = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val sc2 = SparkSession.builder().config(sc1).getOrCreate()
    val df1_song = sc2.read.option("header", "true").option("inferSchema", "true").csv("input/air_data.csv")
    df1_song.createOrReplaceTempView("air_data")

    val df2 = sc2.sql(
      """SELECT
        |    CASE
        |        WHEN age >= 0 AND age < 18 THEN '0-17'
        |        WHEN age >= 18 AND age < 25 THEN '18-24'
        |        WHEN age >= 25 AND age < 35 THEN '25-34'
        |        WHEN age >= 35 AND age < 45 THEN '35-44'
        |        WHEN age >= 45 AND age < 55 THEN '45-54'
        |        WHEN age >= 55 THEN '55+'
        |        ELSE 'Unknown'
        |    END AS age_group,
        |    COUNT(*) AS count
        |FROM
        |   air_data
        |GROUP BY age_group
        |order by age_group asc""".stripMargin)
    df2.show
    df2.write
      .format("jdbc")
      .option("url","jdbc:mysql://niit-master:3306/sem7")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("user","root")
      .option("password","root")
      .option("dbtable","qyl9")
      .mode(SaveMode.Append)
      .save()

  }
}
