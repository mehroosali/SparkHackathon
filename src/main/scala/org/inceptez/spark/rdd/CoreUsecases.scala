package org.inceptez.spark.rdd

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DateType

object CoreUsecases {

  case class insureclass( IssuerId : String, IssuerId2 : String, BusinessDate : String, StateCode : String,
    SourceName : String, NetworkName : String, NetworkURL : String, custnum : Int, MarketCoverage : String,
    DentalOnlyPlan : String )

  def main( args : Array[ String ] ) : Unit = {

    Logger.getLogger( "org" ).setLevel( Level.ERROR )

    val conf = new SparkConf()
      .set( "spark.hadoop.fs.defaultFS", "hdfs://localhost:54310" )
      .set( "spark.history.fs.logDirectory", "file:///tmp/spark-events" )
      .set( "spark.eventLog.dir", "file:////tmp/spark-events" )
      .set( "spark.eventLog.enabled", "true" )

    val spark = SparkSession.builder()
      .appName( "Spark Hackathon-1" )
      .master( "local[*]" )
      .config( conf )
      .getOrCreate()

    import spark.implicits._

    val sparkContext = spark.sparkContext
    val sqlContext = spark.sqlContext

    //1.1
    val insurancedata1 = sparkContext.textFile( "/user/hduser/sparkhack2/insuranceinfo1.csv" )
    println( "insuranceinfo1.csv loaded." )
    val initialRecordsData1 = insurancedata1.count
    println( s"No. of initial records of insuranceinfo1.csv = ${initialRecordsData1}" )
    //1.2
    val header1 = insurancedata1.first
    val insuranceRdd1 = insurancedata1.filter( _ != header1 )
    println( "insuranceinfo1.csv header removed." )
    //1.3
    println( s"No. of records after removing header = ${insuranceRdd1.count}" )
    println( "Printing few records after removing header: " )
    insuranceRdd1.take( 5 ).foreach( println )
    //1.4
    val insuranceRdd2 = insuranceRdd1.filter( _.length != 0 )
    println( "Blank lines in insuranceinfo1.csv removed." )
    //1.5
    val insuranceRdd3 = insuranceRdd2.map( _.split( ",", -1 ) )
    println( "Splited records based on , delimiter." )
    println( s"First record array after spliting: ${insuranceRdd3.first.mkString( "," )}" )
    //1.6
    val insuranceRdd4 = insuranceRdd3.filter( _.length == 10 )
    println( "Filtered records with only 10 columns." )
    //1.7
    val insuranceSchemaRdd1 = insuranceRdd4.map( row ⇒ insureclass( row( 0 ), row( 1 ), row( 2 ), row( 3 ),
      row( 4 ), row( 5 ), row( 6 ), row( 7 ).toInt, row( 8 ), row( 9 ) ) )
    println( "Applied case class schema on insuranceinfo1.csv " )
    //1.8
    val finalRecordsData1 = insuranceSchemaRdd1.count
    println( s"Final records after cleaning of insuranceinfo1.csv = ${finalRecordsData1}" )
    println( s"No. of rows removed in insuranceinfo1.csv = ${initialRecordsData1 - finalRecordsData1}" )
    //1.9
    val rejectData1 = insuranceRdd3.filter( _.length != 10 )
    println( "Rejected data with columns not equal to 10 seperated." )

    //1.10
    val insurancedata2 = sparkContext.textFile( "/user/hduser/sparkhack2/insuranceinfo2.csv" )
    println( "insuranceinfo2.csv loaded." )
    val initialRecordsData2 = insurancedata2.count
    println( s"No. of initial records of insuranceinfo2.csv = ${initialRecordsData2}" )
    //1.11
    //1.11.2s
    val header2 = insurancedata2.first
    val insuranceRdd5 = insurancedata2.filter( _ != header2 )
    println( "insuranceinfo2.csv header removed." )
    //1.11.3
    println( s"No. of records after removing header = ${insuranceRdd5.count}" )
    println( "Printing few records after removing header: " )
    insuranceRdd5.take( 5 ).foreach( println )
    //1.11.4
    val insuranceRdd6 = insuranceRdd5.filter( _.length != 0 )
    println( "Blank lines in insuranceinfo2.csv removed." )
    //1.11.5
    val insuranceRdd7 = insuranceRdd6.map( _.split( ",", -1 ) )
    println( "Splited records based on , delimiter." )
    println( s"First record array after spliting: ${insuranceRdd7.first.mkString( "," )}" )
    //1.11.6
    val insuranceRdd8 = insuranceRdd7.filter( _.length == 10 )
    println( "Filtered records with only 10 columns." )
    //1.11.7
    val insuranceSchemaRdd2 = insuranceRdd8.map( row ⇒ insureclass( row( 0 ), row( 1 ), row ( 2 ), row( 3 ),
      row( 4 ), row( 5 ), row( 6 ), row( 7 ).toInt, row( 8 ), row( 9 ) ) )
    println( "Applied case class schema." )
    //1.11.8
    val finalRecordsData2 = insuranceSchemaRdd2.count
    println( s"Final records after cleaning of insuranceinfo1.csv = ${finalRecordsData2}" )
    println( s"No. of rows removed in insuranceinfo1.csv = ${initialRecordsData2 - finalRecordsData2}" )
    //1.11.9
    val rejectData2 = insuranceRdd7.filter( _.length != 10 )
    println( "Rejected data with columns not equal to 10 seperated." )
    //1.11.10
    val insuranceSchemaRdd3 = insuranceSchemaRdd2.filter { x ⇒ x.IssuerId != null || x.IssuerId2 != null }
    println( s"Count of final RDD: ${insuranceSchemaRdd3.count}" )
    //2.12
    val mergeRDD = insuranceSchemaRdd1.union( insuranceSchemaRdd3 )
    //2.13
    mergeRDD.persist( StorageLevel.DISK_ONLY )
    println( "Merged RDD persisted in disk." )
    //2.14
    val file1FinalRddCount = insuranceSchemaRdd1.count
    val file2FinalRddCount = insuranceSchemaRdd3.count
    val mergeRddCount = mergeRDD.count
    println( s"Count check is ${file1FinalRddCount + file2FinalRddCount == mergeRddCount}" )
    //2.15
    val distinctMergeRdd = mergeRDD.distinct
    val distinctMergeCount = distinctMergeRdd.count
    print( s"Number of duplicate rows is ${mergeRddCount - distinctMergeCount}" )
    //2.16
    val insuredatarepart = distinctMergeRdd.repartition( 8 )
    println( "number of partition increased to 8" )
    //2.17
    val rdd_20191001 = insuredatarepart.filter( _.BusinessDate == "2019-10-01" )
    val rdd_20191002 = insuredatarepart.filter( _.BusinessDate == "2019-10-02" )
    println( "Rdd's splited based on dates" )
    //2.18
    rejectData1.saveAsTextFile( "/user/hduser/sparkhack2/results/rejectDataFile1" )
    mergeRDD.saveAsTextFile( "/user/hduser/sparkhack2/results/mergeDataFile1andFile2" )
    rdd_20191001.saveAsTextFile( "/user/hduser/sparkhack2/results/dateRdd1" )
    rdd_20191002.saveAsTextFile( "/user/hduser/sparkhack2/results/dateRdd2" )
    println( "Files saved to HDFS." )
    //3.20
    val insuranceSchema = StructType( Array(
      StructField( "IssuerId", IntegerType ),
      StructField( "IssuerId2", IntegerType ),
      StructField( "BusinessDate", DateType ),
      StructField( "StateCode", StringType ),
      StructField( "SourceName", StringType ),
      StructField( "NetworkName", StringType ),
      StructField( "NetworkURL", StringType ),
      StructField( "custnum", StringType ),
      StructField( "MarketCoverage", StringType ),
      StructField( "DentalOnlyPlan", StringType )
    ) )
    println( "insurance StructType defined." )
    //2.19
    val tempdf = insuredatarepart.toDF
    val insuredaterepartdf = spark.createDataFrame( tempdf.rdd, insuranceSchema )
    println( "insurance DF created using StructType." )

  }

}
