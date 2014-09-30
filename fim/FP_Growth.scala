package fim

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.collection.mutable.Map
import scala.tools.ant.sabbus.Break

object FP_Growth {
  def key_index(a: Array[(String, Int)], s: String): Int = {
    val t = a.find(_._1 == s)
    t match {
      case Some(x) => a.indexOf(x) + 1
      case None => -1
    }
  }
  def main(args: Array[String]) = {
    val support_percent = 0.85
    //    val pnum = 16;
    //    val conf = new SparkConf()
    val pnum = 2;
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
        if ((t._1(i) - 1) / g_size != pre) {
          pre = (t._1(i) - 1) / g_size
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
    if (v.count(t => true) == 1) {
      return List(v.head)
    }
    val root = new tree(null, null, 0)
    val tab = Map[Int, tree]()
    val tabc = Map[Int, Int]()
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
      tabc(i) = count
      if (count < min_support) {
        var cur = tab(i)
        while (cur != null) {
          var s = cur.Gnext
          cur.Gparent.son.remove(cur.Gv)
          cur = s
        }
        tab.remove(i)
      }
    }
    //deal with target
    var r = List[(List[Int], Int)]()
    var tail: Iterable[Int] = null
    if (target == null)
      tail = tab.keys
    else {
      tail = target.filter(a => tab.exists(b => b._1 == a))
    }
    if (tail.count(t => true) == 0)
      return r
    //single
    var cur = root
    var c = 1
    while (c < 2) {
      c = cur.son.count(t => true)
      if (c == 0) {
        var res = List[(Int, Int)]()
        while (cur != root) {
          res = (cur.Gv, cur.support) :: res
          cur = cur.Gparent
        }

        val part = res.partition(t1 => tail.exists(t2 => t1._1 == t2))
        val p1 = gen(part._1)

        if (part._2.length == 0)
          return p1
        else
          return decare(p1, gen(part._2)) ::: p1
      }
      cur = cur.son.values.head
    }
    //process
    for (i <- tail) {
      var result = List[(List[Int], Int)]()
      var cur = tab(i)
      while (cur != null) {
        var item = List[Int]()
        var s = cur.Gparent
        while (s != root) {
          item = s.Gv :: item
          s = s.Gparent
        }
        result = (item, cur.support) :: result
        cur = cur.Gnext
      }
      r = (List(i), tabc(i)) :: fp_growth(result, min_support).map(t => (i :: t._1, t._2)) ::: r

    }
    r
  }
  def gen(tab: List[(Int, Int)]): List[(List[Int], Int)] = {
    if (tab.length == 1) {
      return List((List(tab(0)._1), tab(0)._2))
    }
    val sp = tab(0)
    val t = gen(tab.drop(1))
    return (List(sp._1), sp._2) :: t.map(s => (sp._1 :: s._1, s._2 min sp._2)) ::: t
    //TODO: sp._2 may not be min
  }
  def decare[T](a: List[(List[T], Int)], b: List[(List[T], Int)]): List[(List[T], Int)] = {
    var res = List[(List[T], Int)]()
    for (i <- a)
      for (j <- b)
        res = (i._1 ::: j._1, i._2 min j._2) :: res
    res
  }
}

class tree(parent: tree, next: tree, v: Int) {
  val son = Map[Int, tree]()
  var support = 0
  def Gparent = parent
  def Gv = v
  def Gnext = next
}