package org.inceptez.spark.streaming
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object KafkaUsecase {

  def main( args : Array[ String ] ) : Unit = {

    Logger.getLogger( "org" ).setLevel( Level.ERROR )

    val spark = SparkSession.builder
      .appName( "Spark Hackathon-4" )
      .master( "local[*]" )
      .config( "hive.metastore.uris", "thrift://localhost:9083" )
      .config( "spark.sql.warehouse.dir", "hdfs://localhost:54310/user/hive/warehouse" )
      .enableHiveSupport()
      .getOrCreate();

    import spark.implicits._
    val sparkcontext = spark.sparkContext;
    sparkcontext.setLogLevel( "ERROR" )
    val ssc = new StreamingContext( sparkcontext, Seconds( 50 ) )

    //Kafka Setup
    val kafkaParams = Map[ String, Object ](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[ StringDeserializer ],
      "value.deserializer" -> classOf[ StringDeserializer ],
      "group.id" -> "we33",
      "auto.offset.reset" -> "latest"
    )

    val topics = Array( "tk1" )

    val chatData = KafkaUtils.createDirectStream[ String, String ]( ssc,
      PreferConsistent,
      Subscribe[ String, String ]( topics, kafkaParams ) )

    chatData.foreachRDD( kafkastream ⇒ {
      if ( !kafkastream.isEmpty ) {
        println( java.time.LocalTime.now )
        //5.1, 5.2
        val chatDF = kafkastream.map( _.value() )
        val chatDFSplitted = chatDF.map( _.split( "~" ) )
          .filter( _.length == 3 )
          .map( x ⇒ ( x( 0 ).toInt, x( 1 ), x( 2 ) ) )
          .toDF( "id", "chat", "type" )
        println( "chat data loaded from kafkastream" )
        chatDFSplitted.show
        //5.3
        val custChatDF = chatDFSplitted.filter( col( "type" ) === "c" )
        println( "chat data filtered based on type" )
        //5.4
        val custChatDFDropped = custChatDF.drop( "type" )
        val custChatView = custChatDFDropped.createOrReplaceTempView( "custchat" )
        println( "type column dropped and view created" )
        //5.5
        spark.sql( """
          create temporary view chattokenized as
          select id,split(chat,'\\s') as chat_tokens from custchat
          """ )
        println( "split function on chat data applied" )
        //5.6
        spark.sql( """
          create temporary view chatexploded as
          select id, text from chattokenized lateral view explode(chat_tokens) as text
          """ )
        println( "exploded lateral view created" )
        //5.8
        val stopWordsRdd = sparkcontext.textFile( "file:///home/hduser/stopwordsdir/stopwords" )
        val stopWordsDF = stopWordsRdd.toDF( "stopword" )
        stopWordsDF.createTempView( "stopwords" )
        println( "stopwords data loaded and view created" )
        //5.9
        val stopWordsFiltered = spark.sql( """
            select * from chatexploded where text not in (select * from stopwords)
            """ )
        println( "stopwords data filtered" )
        stopWordsFiltered.show( 30 )
        //5.10
        stopWordsFiltered.write.mode( "append" ).saveAsTable( "sparkdb.stopwordsfiltered" )
        println( "filtered customer chats saved to hive table." )
        //5.11
        val finalResult = stopWordsFiltered.groupBy( col( "text" ) as "chatkeywords" )
          .agg( count( "text" ) as "occurance" )
        //5.12
        finalResult.show( 30 )
        finalResult.coalesce( 1 ).write.mode( "append" ).json( "file:///home/hduser/sparkdata" )
        println( "final result saved as json" )
        println( "cleaning up temp views.." )
        spark.catalog.dropTempView( "chattokenized" )
        spark.catalog.dropTempView( "chatexploded" )
        spark.catalog.dropTempView( "stopwords" )
      }
      else {
        println( java.time.LocalTime.now )
        println( "No chat data available" )
      }
    } )
    ssc.start()
    ssc.awaitTermination()
  }
}
