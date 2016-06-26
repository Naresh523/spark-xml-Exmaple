package com.spark

import com.databricks.spark.xml.XmlInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, DataFrame}



/**
  * Created by n590368c on 6/20/2016.
  */
object TestExample {

  def main(args : Array[String]) {

    val conf = new SparkConf().setMaster("local[*]").setAppName("XML Parser Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val input = args(0)
    val output = args(1)
    System.out.println(input)
    System.out.println(output)

//    loadingXmlFile(sc, input, output)

    /*val nestedCircularItem = StructType(List(
      StructField("id", LongType, nullable = true),
      StructField("rank", StringType, nullable = true)
    ))

    val nestedCircularItems = StructType(Array(
      StructField("circularItem", nestedCircularItem, nullable = true)
    ))
*/
    /*val nestedWeekly = StructType(Array(
      StructField("circularId", StringType, nullable = true),
      StructField("entityId", StringType, nullable = true),
      StructField("divId", StringType, nullable = true),
      StructField("storeId", StringType, nullable = true),
      StructField("marketRepStore", StringType, nullable = true),
      StructField("TargetedDate", StringType, nullable = true),
      StructField("LastUpdatedDate", StringType, nullable = true),
      StructField("circularItems", nestedCircularItems, nullable = true)
//      StructField("", StringType, nullable = true)
    ))*/

   /* val nestedScoredOffer = StructType(Seq(
      StructField("offerId", LongType, nullable = true),
      StructField("score", DoubleType, nullable = true),
      StructField("startDate", StringType, nullable = true),
      StructField("endDate", StringType, nullable = true),
      StructField("rankSource", StringType, nullable = true)
    ))

    val nestedScoredOffers = StructType(Seq(
      StructField("scoredOffer", nestedScoredOffer, nullable = true)
    ))

    val nestedCoupons = StructType(Seq(
      StructField("entityId", LongType, nullable = true),
      StructField("TargetedDate", StringType, nullable = true),
      StructField("LastUpdatedDate", StringType, nullable = true),
      StructField("scoredOffers", nestedScoredOffers, nullable = true)
    ))

    val nestedDataServed = StructType(Seq(
      StructField("issuccessful", BooleanType, nullable = true),
      StructField("cachemaxage", LongType, nullable = true),
      StructField("SystemRequestId", StringType, nullable = true),
//      StructField("weeklyads", nestedWeekly, nullable = true),
      StructField("coupons",nestedCoupons,nullable = true),
      StructField("responsecode", LongType, nullable = true),
      StructField("responsemessage", StringType, nullable = true)
    ))

    val nestedResponse = StructType(Seq(
      StructField("Response", nestedDataServed, nullable = true)
    ))

    val nestedSchema =
    StructType(Seq(
      StructField("PersonaId", StringType, nullable = true),
      StructField("ApplicationRecordId", StringType, nullable = true),
      StructField("ScienceDataGrainId", LongType, nullable = true),
      StructField("ScienceDataGrain", StringType, nullable = true),
      StructField("EventCellId", LongType, nullable = true),
      StructField("ChannelName", StringType, nullable = true),
      StructField("EventId", StringType, nullable = true),
      StructField("EventStartDate", StringType, nullable = true),
      StructField("EventEndDate", StringType, nullable = true),
      StructField("DataServed", nestedResponse, nullable = true),
      StructField("Filters", StringType, nullable = true),
      StructField("BasketContents", StringType, nullable = true),
      StructField("Timestamp", StringType, nullable = true),
      StructField("ServiceName", StringType, nullable = true),
      StructField("ServiceId", LongType, nullable = true),
      StructField("ApiVersion", StringType, nullable = true),
      StructField("InputParams", LongType, nullable = true),
      StructField("ControlType", LongType, nullable = true),
      StructField("EligibleForSponsorship", BooleanType, nullable = true),
      StructField("SourceSystemEnvironment", StringType, nullable = true),
      StructField("DefaultValueServed", BooleanType, nullable = true),
      StructField("DefaultValueKeyServed", StringType, nullable = true),
      StructField("ChannelId", LongType, nullable = true),
      StructField("PageId", StringType, nullable = true),
      StructField("StoreId", StringType, nullable = true),
      StructField("OrderId", StringType, nullable = true),
      StructField("ShowMore", BooleanType, nullable = true),
      StructField("ResponseTime", LongType, nullable = true),
      StructField("PaginationLimit", StringType, nullable = true),
      StructField("PaginationOffset", StringType, nullable = true),
      StructField("IsPreAllocated", BooleanType, nullable = true),
      StructField("PreAllocatedGroupName", StringType, nullable = true),
      StructField("ControlContentType", StringType, nullable = true),
      StructField("IsCheckRecommendation", BooleanType, nullable = true)
//      StructField("", StringType, nullable = true)

          ))

    val customSchema =
      StructType(Seq(
        StructField("DocumentId", StringType, nullable = true),
        StructField("Contact", nestedSchema, nullable = true)
      ))*/

    val dfSchema = sqlContext.read
      .format("com.databricks.spark.xml")
//      .schema(customSchema)
      .option("rowTag", "Contact")
//      .option("rowTag", "Service")
//      .option("header", "false")
      .load(input)

//    dfSchema.registerTempTable("Xml_Temp")
    dfSchema.printSchema()

    dfSchema.write
      .format("com.databricks.spark.csv")
      .save(output)


    /*val dfSchema = sqlContext.read
                      .format("com.databricks.spark.xml")
                      .option("rowTag", "Service")
                      .load(input)

    dfSchema.registerTempTable("Xml_Temp")
        dfSchema.printSchema()

    dfSchema.write
        .format("com.databricks.spark.csv")
        .save(output)*/

  }
}
