package com.boco.bigdata.etl

import org.apache.spark._
import org.apache.spark.SparkContext._
import collection.mutable.ArrayBuffer
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.hadoop.io._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

import scala.collection.mutable.LinkedHashSet

/**
 * Created by liziyao on 15/7/16.
 */


//spark-submit --name sf --class com.boco.bigdata.etl.Low2High --master spark://cloud142:7077 BigdataElement-assembly-1.0.jar
object Time {
  def main(args: Array[String]) {

    if (args.length == 0) {
      System.err.println("USAGE [name] [class] [master] [jar file path] id1 id2 time input output")
      System.exit(-1)
    }
    val sparkConf = new SparkConf().setAppName("TimeSequence")
    val sc = new SparkContext(sparkConf)
    sc.setLocalProperty("spark.rdd.compress","true")
    sc.setLocalProperty("spark.storage.memoryFraction","1")
    sc.setLocalProperty("spark.core.connection.ack.wait.timeout","6000")
    sc.setLocalProperty("spark.akka.frameSize","100")

    val ID1 = args(0)
    val ID2 = args(1)
    val INPUT_FILE = "/user/boco/dataplatform/guangdong/input/clr_p141104.utf8.txt"
    val OUTPUT_FILE = "/home/boco/lzy/Ne_com/result"
    val LINE = "\\ |\n"
    val SEP = "_\\|"
    val INT_ID = 13
    val EVENT_TIME = 17
    val OBJECT_CLASS = 14
    val STANDARD_ALARM_ID = 134

//    //cal.add(Calendar.MINUTE,10)
//    val result = cal.getTime
//    val a = result.toLocalString
//    START_TIME=2014-11-03 00:00:00
//

    def timeChange(time:String):String={
      return time.substring(0,time.size-1)+"0"
    }


    @transient val conf = new Configuration()
    conf.set("textinputformat.record.delimiter",LINE)
    val raw_data = sc.newAPIHadoopFile(INPUT_FILE,classOf[TextInputFormat],classOf[LongWritable],classOf[Text],conf).map(_._2.toString).map(_.split(SEP))
    val data_1 = raw_data.map( x => ((x(INT_ID)+"_"+x(OBJECT_CLASS),timeChange(x(EVENT_TIME))),x(STANDARD_ALARM_ID))).filter( x => x._1._1 == ID1 )
    val data_2 = data_1.map( x => (x._1._1+","+x._1._2,x._2)).distinct().groupByKey.map( x => (x._1,x._2.toList.mkString("+"))).collectAsMap()

    def timeSequence(input:String):List[String]={
      val cal = Calendar.getInstance()
      val format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
      var tmp = input
      var arr = ArrayBuffer[String]()
      for( i <- 0 to 143){
        cal.setTime(format.parse(tmp))
        cal.add(Calendar.MINUTE,10)
        val result = cal.getTime
        val a = result.toString
        arr = arr :+ a
        tmp = a
      }
      return arr.toList
    }

  }
}
