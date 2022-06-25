
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.{SparkConf, SparkContext}

import java.net.URI
import scala.collection.mutable.ArrayBuffer

object Main {

  var basket_path = "hdfs://127.0.0.1:9000/earrec/sample/trans_2W.txt"
  var user_path = "hdfs://127.0.0.1:9000/earrec/sample/test_2W.txt"
  var pattern_path = "hdfs://127.0.0.1:9000/earrec/out/pattern"
  var res_path = "hdfs://127.0.0.1:9000/earrec/out/result"

  var min_sup: Double = 0.2 // 最小支持度阈值
  val min_conf = 0.6 // 最小置信度

  def main(args: Array[String]): Unit ={
    println("输入参数：" + args.mkString("[", ",", "]"))
    if(args.length == 4){
      basket_path = args(0)
      user_path = args(1)
      pattern_path = args(2)
      res_path = args(3)
    }
    println("购物篮数据输入目录: " + basket_path)
    println("用户数据集输入目录: " + user_path)
    println("频繁模式输出目录: " + pattern_path)
    println("推荐结果输出目录: " + res_path)
    println("最小支持度阈值: " + min_sup)
    println("最小置信度值: " + min_conf)

    println("Main Start ...")

    // 创建SparkConf对象
    val conf = new SparkConf()
    conf.setAppName("EARrec App")
    conf.setMaster("local[*]")
    conf.set("spark.driver.cores", "8")
    conf.set("spark.default.parallelism","8")
    conf.set("spark.dynamicAllocation.initialExecutors","4")
    conf.set("spark.dynamicAllocation.maxExecutors","32")
    val sc = new SparkContext(conf)

//     创建hdfs对象
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new URI("hdfs://127.0.0.1:9000"), new Configuration())

    // 1. 加载购物篮数据集
    println("Loading Basket Datasets from " + basket_path + " ...")
    val basket = sc.textFile(basket_path).map(x => x.split(" "))

    // 2. 使用FP-Growth算法生成频繁模式
    println("Generating Frequency Pattern ...")
    val fpg = new FPGrowth().setMinSupport(min_sup).setNumPartitions(10)
    val model = fpg.run(basket)
    // 2.1 生成频繁模式
//    model.freqItemsets.collect().foreach{item => println(s"${item.items.mkString("[", ",", "]")},${item.freq}")}
    if(hdfs.exists(new Path(pattern_path))) hdfs.delete(new Path(pattern_path), true)
    val pattern = model.freqItemsets
      .coalesce(1, false)
      .sortBy(_.freq)
      .map(x => x.items.mkString("", " ", ""))
    println("频繁规则数目: " + pattern.count())
    pattern.saveAsTextFile(pattern_path)
    // 2.2 生成关联规则
    println("Generating Association Rules ...")
    val Rules = model.generateAssociationRules(min_conf).collect()
    println("关联规则数目: " + Rules.length)

    // 3. 关联规则匹配
    println("Matching Association Rules ...")
    // 3.1 读取用户数据
    val user = sc.textFile(user_path).repartition(4)
    // 3.2 匹配
    // 通过mapPartitions优化速度
    def get_rec(iter: Iterator[String]): Iterator[String] ={
      var ans = ArrayBuffer[String]()
      while(iter.hasNext){
        val cur = iter.next.split(" ")
        var max_conf: Double = 0
        var item: String = "0"
        // 遍历Rules查找所有的规则，并保留符合条件的最大置信度的商品
        Rules.foreach(rule => {
          // 不包含被推荐物品
          if(!cur.contains(rule.consequent(0))){
            // 前置商品都存在
            var flg = 0
            for(i <- rule.antecedent.indices){
              if(!cur.contains(rule.antecedent(i))) flg = 1
            }
            if(flg == 0) {
              if(rule.confidence >= max_conf){
                max_conf = rule.confidence
                item = rule.consequent(0)
              }
            }
          }
        })
        ans.append(item)
      }
      ans.iterator
    }
    // 结果
    val res = user.mapPartitions(get_rec)
    if(hdfs.exists(new Path(res_path))) hdfs.delete(new Path(res_path), true)
    res.coalesce(1, false).saveAsTextFile(res_path)

    println("Main End ============================")

    sc.stop()
  }
}