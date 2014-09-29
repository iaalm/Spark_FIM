package fim

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions

object Apriori {

  def main(args: Array[String]): Unit = {
    val support_percent = 0.85
    val fim_c = 8
    val conf = new SparkConf().setAppName("fim").setMaster("local")
    val sc = new SparkContext(conf)
    val file = sc.textFile(args(0))
    val t_list = file.map(line => line.split(" ").drop(1).map(_.toInt))
    val m_list = t_list.map(t=>(Set(t),1)).reduceByKey(_+_)
    val support_num = t_list.count() * support_percent round
    var i_list = t_list.flatMap(t => t)
      .map(word => (List(word), 1))
      .reduceByKey(_ + _)
      .filter(_._2 >= support_num)
    i_list.saveAsTextFile(args(1)+"/result-1")
    
    for (count <- 2 to fim_c){
      i_list.groupBy(_._1.drop(1))
    }
  }

}