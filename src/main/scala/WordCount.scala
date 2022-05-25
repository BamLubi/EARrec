
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit ={

    println("WordCount Start ============================")

    val conf = new SparkConf() // 创建SparkConf对象
    conf.setAppName("WordCount App")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("src/main/resources/test.txt", 1)

    val words = lines.flatMap(line => line.split(" "))

    val pairs = words.map(word => (word, 1))

    val wordCounts = pairs.reduceByKey(_+_)

    wordCounts.collect().foreach(wordCountsPair => println(wordCountsPair._1 + " : " + wordCountsPair._2))

    println("WordCount End ============================")

    sc.stop()
  }

}
