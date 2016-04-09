package com.alimusic.competition

import com.databricks.spark.csv._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * Author: helin <helin199210@icloud.com>
  * Time: 16/4/8 上午11:24
  */
object LoadScv {


  val conf = new SparkConf().setAppName("LoadTianchiBigDataCompetitionScvData").setMaster("local");
  val sc = new SparkContext(conf);
  val sqlContext = new SQLContext(sc);
  val tianchiData = sqlContext.csvFile(filePath = "/Users/helin/Documents/TianchiBigData/data/mars_tianchi_user_actions.csv", useHeader = true, delimiter = ',')
  val sortedTianchiBigData = tianchiData.sort(tianchiData("gmt_create").desc)
  sortedTianchiBigData.persist();
  sortedTianchiBigData.registerTempTable("sortedTianchiBigData")


  def main(args: Array[String]) {
    println("hello spark");

    //    val conf = new SparkConf().setAppName("LoadTianchiBigDataCompetitionScvData").setMaster("local");
    //    val sc = new SparkContext(conf);
    //    val sqlContext = new SQLContext(sc);

    //    val tianchiData = sqlContext.csvFile(filePath = "/Users/helin/Documents/TianchiBigData/data/mars_tianchi_user_actions_bak.csv", useHeader = true, delimiter = ',')
//        val tianchiData1 = sc.textFile("/Users/helin/Documents/TianchiBigData/data/mars_tianchi_user_actions_bak.csv")
    //    tianchiData.printSchema();
    //    tianchiData show ;
    //    tianchiData take 5 foreach println

    //    val song: DataFrame = tianchiData.select("song_id")
    //    song.show(3)
    //    val songAndaction_type = tianchiData.select("song_id", "action_type")
    //    songAndaction_type show
    //    val tianchiBigData1 = sc.textFile("/Users/helin/Documents/TianchiBigData/data/mars_tianchi_user_actions_bak.csv")
    //    var a:List[Any] = List()

    //    val s = "effb071415be51f11e845884e67c0f8c"
    //    val s1 = "f87ff481d85d2f95335ab602f38a7655"
    //    println (tianchiData.filter(s"song_id ='${s}' ").count())
    //    tianchiData.filter(s"song_id ='${s1}' and action_type = '3'").sort(tianchiData("gmt_create").desc).foreach(println)
    //    println (song.filter("song_id ='effb071415be51f11e845884e67c0f8c' OR action_type = '1'").count())
    //    println (song.filter(s"song_id ='${s1}' and action_type = 1").count())

    //    tianchiData.filter("action_type = 1").show()
//    tianchiData.registerTempTable("tianchiData")
        val sql =
        """
          |SELECT DISTINCT song_id,count(*) as clicks FROM sortedTianchiBigData
          |group by song_id
        """.stripMargin

    val songsWithUserActions = sqlContext.sql(sql)
    val songsWithUserActionsOrderByClicksInDescOrder = songsWithUserActions.sort(songsWithUserActions("clicks").desc)

//    songsWithUserActionsOrderByClicksInDescOrder.persist()

//    songsWithUserActionsOrderByClicksInDescOrder.show()
//    val n:Int = Integer.parseInt(songsWithUserActionsOrderByClicksInDescOrder.filter("clicks > '1000'").select("song_id").count().toString)
//    println(n)

    val songNameList = songsWithUserActionsOrderByClicksInDescOrder.select("song_id").takeAsList(100)
//    songsWithUserActionsOrderByClicksInDescOrder.persist()
    val songClicksCountList = songsWithUserActionsOrderByClicksInDescOrder.select("clicks").takeAsList(100)

    for(i <- 0 until songNameList.size()){
      val songName =  songNameList.get(i).toString()
      val sn = songName.substring(1,songName.length - 1)

      val songClick = songClicksCountList.get(i).toString()
      val fileName = songClick + sn

      val songWithClicks = sortedTianchiBigData.filter(s"song_id = '${sn}'")
      songWithClicks.saveAsCsvFile(s"/Users/helin/Documents/TianchiBigData/data/TableOfUserActionBySong_id/${fileName}")
    }

//    val songsWithUsers = sqlContext.sql(
//      """
//        |SELECT DISTINCT song_id,count(*) as clicks FROM tianchiData
//        |group by song_id
//      """.stripMargin)

//    println(songsWithUsers.filter("clicks > '30000'").count())

//    val clicks = "clicks > '30000'"
////    val theMostPopSongInTheCsv = songsWithUsers.filter(clicks)
//    tableOfUserActionBySongName(songsWithUsers.filter(clicks).select("song_id"),clicks)



//    tableOfUserActionBySongName("8a27d9a6c59628c991c154e8d93f412e",clicks)
//    theMostPopSongInTheCsv.foreach(
//      d => {
//
//        tableOfUserActionBySongName(d.toString, clicks)
//      }
//    )
    //    songsWithUsers.foreach(
    //      d => {
    //        tableOfUserActionBySongName(d)
    //      }
    //    )
    //    println(songsWithUsers.count())
  }


  //    songsWithUsers.saveAsCsvFile("/Users/helin/Documents/TianchiBigData/data/songsWithUsers.csv")
  //    songsWithUsers.registerTempTable("songsWithUsers")
  //    val sql1=
  //      """
  //        |SELECT count(*) as num FROM songsWithUsers
  //        |GROUP BY song_id
  //      """.stripMargin
  //    val checkDistinct = sqlContext.sql(sql1)
  //    println(checkDistinct.count())
  //    checkDistinct.foreach(println)
  //    val user_actions = sqlContext.csvFile(filePath="/Users/helin/Documents/TianchiBigData/data/mars_tianchi_user_actions_bak.csv", useHeader=true, delimiter=',')
  //    val songs = sqlContext.csvFile(filePath="/Users/helin/Documents/TianchiBigData/data/mars_tianchi_songs.csv", useHeader=true, delimiter=',')
  //    songs.registerTempTable("songs")
  //    val songSql =
  //      """
  //        |SELECT DISTINCT song_id FROM songs
  //      """.stripMargin
  //    val songsWithArtist = sqlContext.sql(s"${songSql}")
  //    println(songsWithArtist.count())
  //
  ////    songs.foreach(println)
  //    val join = user_actions.join(songs,user_actions("song_id") === songs("song_id"))
  ////    join show
  ////    val right_outer_join = user_actions.join(songs,user_actions("song_id") === songs("song_id"), "right_outer")
  ////    right_outer_join show
  ////
  ////    val options = Map("path" -> "/Users/helin/Documents/TianchiBigData/data/join.csv")
  ////    val joinOfTianchiBigData =
  //    join.saveAsCsvFile("/Users/helin/Documents/TianchiBigData/data/join.csv")


}

//}
//case class Employee(id:Int, name:String){
//  val conf = new SparkConf().setAppName("colRowDataFrame").setMaster("local")
//  val sc = new SparkContext(conf)
//  val sqlContext=new SQLContext(sc)
//  val listOfEmployees =List(Employee(1,"Arun"), Employee(2, "Jason"), Employee (3, "Abhi"))
//  val empFrame=sqlContext.createDataFrame(listOfEmployees)
//  empFrame.printSchema
//
//}

