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
/**
 * Created by liziyao on 15/7/16.
 */


//spark-submit --name sf --class com.boco.bigdata.etl.NeCom --master spark://cloud142:7077 BigdataElement-assembly-1.0.jar
object NeCom {
  def main(args: Array[String]) {

    if (args.length == 0) {
      System.err.println("USAGE [name] [class] [master] [jar file path] input output")
      System.exit(-1)
    }
    val sparkConf = new SparkConf().setAppName("Net_Com")
    val sc = new SparkContext(sparkConf)
    sc.setLocalProperty("spark.rdd.compress","true")
    sc.setLocalProperty("spark.storage.memoryFraction","1")
    sc.setLocalProperty("spark.core.connection.ack.wait.timeout","6000")
    sc.setLocalProperty("spark.akka.frameSize","100")

    val INPUT_FILE = args(0)
    val OUTPUT_FILE = args(1)
    val LINE = "\\ |\n"
    val SEP = "_\\|"
    val START_TIME = "2014-11-03 00:00:00"
    val INT_ID = 13
    val EVENT_TIME = 17
    val OBJECT_CLASS = 14

    val date_format = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS")
    val calendar_start = Calendar.getInstance()
    calendar_start.setTime(date_format.parse(START_TIME))
    val date_start = calendar_start.getTime
    val sh = date_start.getHours
    val smin = date_start.getMinutes/10
    val start_day = calendar_start.get(Calendar.DAY_OF_YEAR)

    //change time to number
    def time2Num(time:String):Int={
      val calendar_end = Calendar.getInstance()
      calendar_end.setTime(date_format.parse(time))
      val date_end = calendar_end.getTime
      val day = calendar_end.get(Calendar.DAY_OF_YEAR) - start_day
      val hour = date_end.getHours
      val minute_get_head = date_end.getMinutes/10
      val num4Time = day*24*6+(hour-sh)*6+(minute_get_head-smin)
      num4Time
    }

    @transient val conf = new Configuration()
    conf.set("textinputformat.record.delimiter",LINE)
    val raw_data = sc.newAPIHadoopFile(INPUT_FILE,classOf[TextInputFormat],classOf[LongWritable],classOf[Text],conf).map(_._2.toString).map(_.split(SEP))
    println(raw_data.count+"------------------------------raw_data")
    val data_door = raw_data.filter( x => !x(INT_ID).contains("E") && x(OBJECT_CLASS)!="150111")
    val data_door1 = data_door.map( x => (x(INT_ID)+"_"+x(OBJECT_CLASS),time2Num(x(EVENT_TIME))))
    val data_group = data_door1.groupByKey().map( x => (x._1,x._2.toList.distinct.sorted))
    println(data_door.count+"------------------------------data_door")
    def reduceList(list:List[Int]):List[Int]={
      var arrbuff = ArrayBuffer[Boolean]()
      var arrResult = ArrayBuffer[Int]()
      arrbuff = arrbuff :+ true
      for( i <- 1 to list.size-1){
        if(list(i)-list(i-1)==1)
          arrbuff = arrbuff :+ false
        else
          arrbuff = arrbuff :+ true
      }
      for( i <- 0 to arrbuff.size-1){
        if(arrbuff(i))
          arrResult = arrResult :+ list(i)
      }
      return arrResult.toList
    }

    val group_reduce = data_group.map( x => (x._1,reduceList(x._2))).filter( x => x._2.size > 10 && x._2.size < 50)
    val refer = group_reduce.toArray

    def comput(input:(String, List[Int]),refer:Array[(String, List[Int])]):List[(String,String,Double)]={
      var arrResult = ArrayBuffer[(String,String,Double)]()
      for( i <- 0 to refer.size-1){
        val man = input._2.size
        val upMan = Math.abs( man - refer(i)._2.size).toDouble
        if ( upMan/man < 0.2){
          val intersectS = input._2.toSet & refer(i)._2.toSet
          val unionS = input._2.toSet | refer(i)._2.toSet
          arrResult = arrResult :+ (input._1,refer(i)._1,intersectS.size/unionS.size.toDouble)
        }
      }
      return arrResult.toList
    }

    val computCoe = group_reduce.map( x => comput(x,refer)).flatMap( x => x).filter( x => x._3 > 0.4 && x._1 != x._2)
    val result = computCoe.map{ x => {
      val a = x._1.replaceAll("_","").toLong
      val b = x._2.replaceAll("_","").toLong
      if ( a > b )
        (x._1,x._2,x._3)
      else
        (x._2,x._1,x._3)
    }
    }.distinct
    //result.saveAsTextFile(OUTPUT_FILE)
    //------------------------------------------上面是输出关联对------------------------
    val dictionary = data_door.map( x => (x(INT_ID)+"_"+x(OBJECT_CLASS),List(x(EVENT_TIME),x(58),x(18),x(63),x(134),x(133),x(156),x(157),x(174),x(56),x(37),x(153),x(154),x(125)))).groupByKey().map( x => (x._1,x._2.toList))
    val obj_net = result.map( x => (x._1,x._3))

    val wifi = obj_net join dictionary
    println(result.count+"------------------------------result")
    println(obj_net.count+"------------------------------obj_net")
    println(wifi.count+"------------------------------wifi")
//    wifi.distinct.saveAsTextFile(OUTPUT_FILE)
//    sc.stop()


    class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
      override def generateActualKey(key: Any, value: Any): Any =
        NullWritable.get()

      override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
        key.asInstanceOf[String]
    }

    wifi.distinct().partitionBy(new HashPartitioner(5)).saveAsHadoopFile(OUTPUT_FILE,classOf[String],classOf[String],classOf[RDDMultipleTextOutputFormat])
//    wifi.saveAsHadoopFile(OUTPUT_FILE,classOf[String],classOf[String],classOf[RDDMultipleTextOutputFormat])
  }
}
