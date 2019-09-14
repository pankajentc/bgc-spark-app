

package com.bgc

import org.apache.spark.sql.expressions.{Aggregator, Window}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SparkApp {



  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("BGCTest")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    processFiles(spark,args(0))

    spark.stop()
  }

  def processFiles(spark:SparkSession, path:String): Unit ={

    import spark.implicits._

    val principals =  spark.read.option("sep", "\t").option("header", "true").csv(path+"/title.principals.tsv.gz").as("principals")

    val nameBasic =  spark.read.option("sep", "\t").option("header", "true").csv(path+"/name.basics.tsv.gz").as("nameBasic")

    val titleBasic =  spark.read.option("sep", "\t").option("header", "true").csv(path+"/title.basics.tsv.gz").as("titleBasic")

    val titleRating = spark.read.option("sep", "\t").option("header", "true").csv(path+"/title.ratings.tsv.gz")

    val byTconst = Window.partitionBy("tconst");

    titleRating.withColumn("numVotes", col("numVotes").cast("int")).withColumn("averageRating", titleRating.col("averageRating").cast("double")).withColumn("avgNumVotes",avg("numVotes").over(byTconst)).createOrReplaceTempView("titleRating")

    spark.sql("select tconst as tconstRating ,numVotes , (numVotes/avgNumVotes)*averageRating as calc from titleRating where numVotes >= 50 order by calc desc limit 20").join(principals,$"tconstRating" === $"principals.tconst")
      .select($"tconstRating",$"nconst".as("nconstPrinc")).join(nameBasic,$"nconstPrinc"===$"nameBasic.nconst")
      .select($"tconstRating",$"nconstPrinc",$"primaryName").join(titleBasic,$"tconstRating"===$"titleBasic.tconst")
      .select($"tconstRating",$"nconstPrinc",$"primaryName",$"primaryTitle",$"originalTitle").
      coalesce(1).
      foreach(x => (println(x.getAs("tconstRating") ,x.getAs("nconstPrinc") , x.getAs("primaryName"), x.getAs("primaryTitle"), x.getAs("originalTitle"))))
  }
}
