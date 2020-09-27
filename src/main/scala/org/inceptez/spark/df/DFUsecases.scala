package org.inceptez.spark.df

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.inceptez.hack.allmethods

object DFUsecases {
  def main( args : Array[ String ] ) : Unit = {

    Logger.getLogger( "org" ).setLevel( Level.ERROR )

    val conf = new SparkConf()
      .set( "spark.hadoop.fs.defaultFS", "hdfs://localhost:54310" )
      .set( "hive.metastore.uris", "thrift://localhost:9083" )
      .set( "spark.sql.warehouse.dir", "hdfs://localhost:54310/user/hive/warehouse" )
      .set( "spark.history.fs.logDirectory", "file:///tmp/spark-events" )
      .set( "spark.eventLog.dir", "file:////tmp/spark-events" )
      .set( "spark.eventLog.enabled", "true" )

    val spark = SparkSession.builder()
      .appName( "Spark Hackathon-2" )
      .master( "local[*]" )
      .config( conf )
      .enableHiveSupport()
      .getOrCreate()

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

    //3.21
    val insuranceDF1 = spark.read
      .option( "header", "true" )
      .option( "escape", "," )
      .schema( insuranceSchema )
      .csv( "/user/hduser/sparkhack2/insuranceinfo*.csv" )
    println( "insurance data's loaded." )
    //3.22.a
    val insuranceDF2 = insuranceDF1
      .withColumnRenamed( "StateCode", "stcd" )
      .withColumnRenamed( "SourceName", "srcnm" )
    println( "2 columns renamed for insurance DF" )
    //3.22.b
    val insuranceDF3 = insuranceDF2
      .withColumn( "issueridcomposite", concat( col( "IssuerId" ).cast( "String" ), col( "IssuerId2" ).cast( "String" ) ) )
    println( "new concated column added" )
    //3.22.c
    val insuranceDF4 = insuranceDF3.drop( "DentalOnlyPlan" )
    println( "DentalOnlyPlan column dropped" )
    //3.22.d
    val insuranceDF5 = insuranceDF4
      .withColumn( "sysdt", current_date() )
      .withColumn( "systs", current_timestamp() )
    println( "new columns with system date and timestamp added." )
    insuranceDF5.write.mode( "overwrite" ).saveAsTable( "sparkdb.test" )
    //3.23
    val insuranceDF6 = insuranceDF5.select( "*" ).na.drop()
    println( "null columns dropped" )
    println( s"count of non null columns: ${insuranceDF6.count}" )
    //3.25
    val allmethodinc = new allmethods()
    val udftrim = udf( allmethodinc.remspecialchar _ )
    println( "UDF created and registered" )
    //3.26
    val insuranceDFTrim = insuranceDF6.withColumn( "NetworkNameTrimmed", udftrim( col( "NetworkName" ) ) )
    println( "column NetworkName trimmed." )
    //3.27
    insuranceDFTrim.coalesce( 1 ).write.mode( "overwrite" ).json( "/user/hduser/sparkhack2/results/insurancejson" )
    println( "DF saved as JSON in HDFS location" )
    //3.28
    insuranceDFTrim.coalesce( 1 ).write.mode( "overwrite" )
      .option( "header", "true" )
      .option( "sep", "~" )
      .csv( "/user/hduser/sparkhack2/results/insurancecsv" )
    println( "DF saved as CSV with options in HDFS location" )
    //3.29
    insuranceDFTrim.write.mode( "append" ).saveAsTable( "sparkdb.insurancedata" )
    println( "DF saved to Hive Table" )

    //4.30
    val customerdata1 = spark.sparkContext.textFile( "/user/hduser/sparkhack2/custs_states.csv" )
    println( "custs_states.csv loaded." )
    //4.31
    val customerdata2 = customerdata1.map( _.split( ",", -1 ) )
    val custfilter = customerdata2.filter { _.length == 5 }
    val statesfilter = customerdata2.filter { _.length == 2 }
    println( "custs_states.csv RDD splitted into custfilter and statesfilter." )
    //4.32
    val custstatesdf = spark.read
      .csv( "/user/hduser/sparkhack2/custs_states.csv" )
    println( "custs_states.csv loaded as a DF" )
    //4.33
    val statedf = custstatesdf.filter( col( "_c2" ).isNull )
      .drop( "_c2", "_c3", "_c4" )
      .withColumnRenamed( "_c0", "Code" )
      .withColumnRenamed( "_c1", "State" )
    val custdf = custstatesdf.filter( col( "_c2" ).isNotNull )
      .withColumnRenamed( "_c0", "Id" )
      .withColumnRenamed( "_c1", "Firstname" )
      .withColumnRenamed( "_c2", "Lastname" )
      .withColumnRenamed( "_c3", "Age" )
      .withColumnRenamed( "_c4", "Profession" )
    println( "custstatesdf DF is splitted into custdf and statedf." )
    //4.34
    statedf.createOrReplaceTempView( "statesview" )
    custdf.createOrReplaceTempView( "custview" )
    println( "tempview for statedf and custdf created." )
    //4.35
    insuranceDF5.createOrReplaceTempView( "insuranceview" )
    println( "tempview for insuranceDF  created." )
    //4.36
    val allmethodinc2 = new allmethods()
    spark.udf.register( "remspecialcharudf", allmethodinc2.remspecialchar _ )
    println( "UDF for spark SQL is registered." )
    //4.37.a
    spark.sql( """create temporary view insuranceview1 as select *,
    remspecialcharudf(NetworkName) as cleannetworkname from insuranceview""" )
    println( "applied udf remspecialcharudf to get new column cleannetworkname." )
    //4.37.b
    spark.sql( """create temporary view insuranceview2 as select *,
      current_date() as curdt, current_timestamp() as curts from insuranceview""" )
    println( "added column with current date and timestamp." )
    //4.37.c
    spark.sql( """create temporary view insuranceview3 as select *,
      year(businessdate) as yr, month(businessdate) as mth from insuranceview2""" )
    println( "extracted year and month as new columns." )
    //4.37.d
    spark.sql( """create temporary view insuranceview4 as select *,
                  case
                  when parse_url(networkurl,'PROTOCOL') = 'http' then 'http'
                  when parse_url(networkurl,'PROTOCOL') = 'https' then 'https'
                  else 'noprotocal'
                  end as protocol from insuranceview3 """ )
    println( "extracted protocol as new column." )
    //4.37.e
    val joineddf = spark.sql( """
      select iv.*,sv.state, cv.age, cv.profession from insuranceview4 iv
      inner join statesview sv on iv.stcd=sv.code
      inner join custview cv on iv.custnum=cv.id
      """ )
    println( "joined insuranced view, statesview and custviews." )
    //4.38
    joineddf.repartition( 1 ).write
      .mode( "overwrite" ).parquet( "/user/hduser/sparkhack2/results/joinedparquet" )
    println( "joined iview written as parquet in hdfs location." )
    //4.39
    joineddf.createOrReplaceTempView( "joinedview" )
    val finalDF = spark.sql( """
      select avg(age) as avg_age,
      count(state) as count_state, state, protocol, profession from joinedview group by state, protocol, profession
      """ )
    val prop = new java.util.Properties();
    prop.put( "user", "root" )
    prop.put( "password", "root" )
    //4.40
    finalDF.write.mode( "overwrite" ).jdbc( "jdbc:mysql://localhost/custdb", "insureaggregated", prop )
    println( "final aggregated table stored in mysql" )
  }
}
