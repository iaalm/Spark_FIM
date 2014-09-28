package fim1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.collection.mutable.Map
import scala.tools.ant.sabbus.Break

object fp {
  def key_index(a: Array[(String, Int)], s: String): Int = {
    val t = a.find(_._1 == s)
    t match {
      case Some(x) => a.indexOf(x) + 1
      case None => -1
    }
  }
  def main(args: Array[String]) = {
    val support_percent = 0.85
    val pnum = 1;
    val conf = new SparkConf().setAppName("fim").setMaster("local")
    val sc = new SparkContext(conf)
    val file = sc.textFile(args(0))
    val g_list = file.flatMap(line => line.split(" ").drop(1))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .collect
      .toArray
    val g_count = g_list.length
    val g_size = (g_count + pnum - 1) / pnum
    val items = file.map(line => (
      line.split(" ")
      .drop(1)
      .toList.map(key_index(g_list, _))
      .sortWith(_ < _), 1))
    val item = items.reduceByKey(_ + _)
    val support_num: Int = items.count() * support_percent toInt

    val f_list = item.flatMap(t => {
      var pre = -1
      var i = t._1.length - 1
      var result = List[(Int, (List[Int], Int))]()
      while (i >= 0) {
        if (t._1(i) / g_size != pre) {
          pre = t._1(i) / g_size
          result = (pre, (t._1.dropRight(t._1.length - i - 1), t._2)) :: result
        }
        i -= 1
      }
      result
    })
      .groupByKey()
    val d_result = f_list.flatMap(t => {
      fp_growth(t._2, support_num, t._1 * g_size + 1 to (((t._1 + 1) * g_size) min g_count))
    })
    d_result.saveAsTextFile(args(1))
    sc.stop()
  }
  def fp_growth(v: Iterable[(List[Int], Int)], min_support: Int, target: Iterable[Int] = null): List[(List[Int], Int)] = {
    if(v.count(t => true) == 1){
      return List(v.head)
    }
    val root = new tree(null, null, 0)
    val tab = Map[Int, tree]()
    //mk tree
    for (i <- v) {
      var cur = root;
      var s: tree = null
      var list = i._1
      while (!list.isEmpty) {
        if (!tab.exists(_._1 == list(0))) {
          tab(list(0)) = null
        }
        if (!cur.son.exists(_._1 == list(0))) {
          s = new tree(cur, tab(list(0)), list(0))
          tab(list(0)) = s
          cur.son(list(0)) = s
        } else {
          s = cur.son(list(0))
        }
        s.support += i._2
        cur = s
        list = list.drop(1)

      }
    }
    //pre cut
    for (i <- tab.keys) {
      var count = 0
      var cur = tab(i)
      while (cur != null) {
        count += cur.support
        cur = cur.Gnext
      }
      if (count < min_support) {
        var cur = tab(i)
        while (cur != null) {
          var s = cur.Gnext
          cur.Gparent.son.remove(cur.Gv)
          cur = s
        }
      }
    }
    //single
    var cur = root
    var c = 1
    while (c < 2) {
      c = cur.son.count(t => true)
      if (c == 0) {
        val pcur = cur
        var res = List[(Int,Int)]((0,root.support))
        cur = root
        while (cur != pcur) {
          cur = cur.son.head._2
          res = (cur.Gv,cur.support) :: res
        }
        val a = gen(res)
        print(a)
        print("fsda")
        return gen(res)
        //TODO:
      }
      cur = cur.son.values.head
    }
    //process
    var r = List[(List[Int], Int)]()
    var tail: Iterable[Int] = null
    if (target == null)
      tail = tab.keys
    else {
      tail = target.filter(a => tab.exists(b => b._1 == a))
    }
    for (i <- tail) {
      var result = List[(List[Int], Int)]()
      var cur = tab(i)
      while (cur != null) {
        var item = List[Int]()
        var s = cur.Gparent
        while (s.Gparent != null) {
          item = s.Gv :: item
          s = s.Gparent
        }
        result = (item, cur.support) :: result
        cur = cur.Gnext
      }
      r = fp_growth(result, min_support).map(t => (i :: t._1, t._2)) ::: r

    }
    r
  }
  def gen(tab : List[(Int,Int)]): List[(List[Int], Int)]= {
    if(tab.length == 1){
       return List((List(),tab(0)._2))
    }
    val sp = tab(0)
    val t = gen(tab.drop(1))
    return t.map(s => (sp._1::s._1,sp._2)):::t
  }
}

class tree(parent: tree, next: tree, v: Int) {
  val son = Map[Int, tree]()
  var support = 0
  def Gparent = parent
  def Gv = v
  def Gnext = next
}