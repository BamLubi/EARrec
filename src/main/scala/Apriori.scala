
import org.apache.spark.{SparkConf, SparkContext}
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

class Rule(_his: ArrayBuffer[String], _item: String) {
  var item: String = _item
  val his: ArrayBuffer[String] = _his
  var conf: Double = 0

  def calculate(D: Array[Array[String]]): Unit ={
    val pre: Double = Apriori.get_frequency(his, D)
    his.append(item)
    val all: Double = Apriori.get_frequency(his, D)
    conf = (all / pre).formatted("%.2f").toDouble
  }
}

object Apriori {
  var min_sup: Double = 0.092 // 最小支持度阈值
  var D_size: Long = 0 // 购物车大小
  var pattern: ArrayBuffer[ArrayBuffer[String]] = new ArrayBuffer[ArrayBuffer[String]]() // 模式表
  var Rules: ArrayBuffer[Rule] = new ArrayBuffer[Rule]() // 关联规则

  val basket_path = "hdfs://127.0.0.1:9000/earrec/sample/trans_10W.txt"
  val user_path = "hdfs://127.0.0.1:9000/earrec/sample/test_2W.txt"
  val pattern_path = "hdfs://127.0.0.1:9000/earrec/out/pattern"
  val res_path = "hdfs://127.0.0.1:9000/earrec/out/result"

  def main(args: Array[String]): Unit ={

    println("Main Start ...")

    val conf = new SparkConf() // 创建SparkConf对象
    conf.setAppName("EARrec App")
    conf.setMaster("local[*]")

    val sc = new SparkContext(conf)


    // 1. 加载购物篮数据集
    println("Loading Basket Datasets from " + basket_path + " ...")
    val basket = sc.textFile(basket_path).map(x => x.split(" "))
    val basket_col = basket.collect()
    D_size = basket.count()
    min_sup = (min_sup * D_size).toLong // 只要模式的频次大于这个阈值就可取

    println("购物篮大小:" + basket.count())
    println("最小支持度阈值:" + min_sup)


    // 2. 挖掘频繁模式
    println("Generating Frequency Pattern ...")
    // 2.1 将原有数据拆分成单个词
    val words = basket.flatMap(x => x)
    val pairs = words.map(x => (x, 1)).reduceByKey(_ + _).filter(x => x._2 >= min_sup)
    val C1 = pairs.map(x => ArrayBuffer(x._1)).collect().toBuffer.asInstanceOf[ArrayBuffer[ArrayBuffer[String]]]
    pattern ++= C1 // 只有一个商品的，无法生成关联规则，但是给的模式表中有长度为1的项，因此保留。后面计算关联规则的是很好过滤
    // 2.2 递归计算所有的pattern
    pattern_mining(C1, basket_col)
    // 2.3 保存pattern模式到hdfs
    sc.parallelize(pattern).saveAsTextFile(pattern_path)


    // 3. 关联规则生成
    println("Generating Association Rules ...")
    // 3.1 读取模式表
    if (pattern.isEmpty) {
      println("Loading Frequency Pattern from " + pattern_path + " ...")
      sc.textFile(pattern_path).collect().foreach(x => {
        pattern ++= ArrayBuffer(x.split(" ").toBuffer.asInstanceOf[ArrayBuffer[String]])
      })
    }
    println("模式表: " + pattern.size)
    // 3.2 生成关联规则，需要同时计算置信度
    var cnt_pattern = 0
    pattern.foreach(x => {
      // 打印速度
      cnt_pattern += 1
      if(cnt_pattern % 1000 == 0) println(cnt_pattern + "/" + pattern.size)
      //
      if(x.size != 1){
        x.foreach(y => {
          val rule: Rule = new Rule(x - y, y)
          rule.calculate(basket_col)
          Rules.append(rule)
//          println(rule.his, rule.item, rule.conf)
        })
      }
    })


    // 4. 关联规则匹配
    println("Matching Association Rules ...")
    // 4.1 读取用户数据
    val user = sc.textFile(user_path)
    var cnt_user = 0
    // 4.2 匹配
    val ans = user.map(x => {
        // 打印速度
        cnt_user += 1
        if(cnt_user % 1000 == 0) println(cnt_user + "/" + user.count())
        val his = x.split(" ")
        var max_conf: Double = 0
        var item: String = ""
        // 遍历Rules查找所有的规则，并保留符合条件的最大置信度的商品
        Rules.foreach(rule => {
          // 不包含被推荐物品
          if(his.contains(rule.item)) return
          // 前置商品都存在
          var flg = 0
          rule.his.foreach(y => {
            if(!his.contains(y)) flg = 1
            if (flg == 1) return
          })
          if(flg == 1) return
          if(rule.conf >= max_conf){
            max_conf = rule.conf
            item = rule.item
          }
        })
        println(item)
        item
      })
    ans.saveAsTextFile(res_path)

    println("Main End ============================")

    sc.stop()
  }

  /**
   * 获取一个模式的频次
   *
   * @param x 模式
   * @param D 原始数据集
   * @return 次数
   */
  def get_frequency(x: ArrayBuffer[String], D: Array[Array[String]]): Int ={
    var cnt = 0
    D.foreach(line => {
      var flg = 0
      for(i <- x.indices){
        if(!line.contains(x(i))) flg = 1
      }
      if (flg == 0) cnt += 1
    })
    cnt
  }

  @tailrec
  def pattern_mining(x: ArrayBuffer[ArrayBuffer[String]], D: Array[Array[String]]): Unit ={
    // 根据传入的x，构造新的模式表
    var C_now: ArrayBuffer[ArrayBuffer[String]] = new ArrayBuffer[ArrayBuffer[String]]()
    val C_nxt: ArrayBuffer[ArrayBuffer[String]] = new ArrayBuffer[ArrayBuffer[String]]()
    for (i <- 0 to x.size - 2; j <- i + 1 until x.size) {
      //      C_now ++= ArrayBuffer(ArrayBuffer(x(i)(0), x(j)(0)).distinct)
      C_now ++= ArrayBuffer(x(i).union(x(j)).distinct.sorted)
    }
    // 去重
    C_now = C_now.distinct
    // 计算支持度并去除不满足最低支持度要求的
    C_now.foreach(x => {
      val freq = get_frequency(x, D)
      if (freq >= min_sup) {
        C_nxt ++= ArrayBuffer(x)
        pattern ++= ArrayBuffer(x)
        println(x)
      }
    })
    println(pattern.size)
    // 递归下一次
    if (C_nxt.nonEmpty) pattern_mining(C_nxt, D)
  }

}