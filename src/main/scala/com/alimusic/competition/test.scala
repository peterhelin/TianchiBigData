package com.alimusic.competition

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import com.databricks.spark.csv._

/**
  *
  * Author: helin <helin199210@icloud.com>
  * Time: 16/4/8 下午12:43
  */
object test {
  val conf = new SparkConf().setAppName("csvDataFrame").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val sqlContext=new SQLContext(sc)
  val cars = sqlContext.csvFile("cars.csv")
  val students=sqlContext.csvFile(filePath="StudentData.csv", useHeader=true, delimiter='|')

}
