# EARrec
Efficient association rule Recommendation System for big data - The Homework of Advanced operating system

## 基本步骤

- 频繁模式挖掘
  - PFP-Growth算法
- 关联规则生成
- 关联规则匹配
- 推荐分值计算
  - 使用最高置信度作为候选项分值

### 频繁模式挖掘

> 这里使用最简单的Apriori算法，很慢，15min才跑到C2

https://pic1.zhimg.com/80/v2-bea6c42ce1e630b6bb2fe6b0f68679c0_720w.jpg

![](https://pic1.zhimg.com/80/v2-bea6c42ce1e630b6bb2fe6b0f68679c0_720w.jpg)

## 运行环境

![](https://img.shields.io/badge/hadoop-2.7.1-brightgreen)
![](https://img.shields.io/badge/spark-3.2.1-brightgreen)
![](https://img.shields.io/badge/scala-2.12.15-brightgreen)

## 数据集说明

```
trans_10W.txt // 购物车数据
pattern_1W.txt // 频繁模式
test_2W.txt // 用户数据
result.txt // 更具频繁模式推荐的top1
```

## 提交要求

打包成jar包，使用spark-submit运行

```shell
$ spark-submit --master <test spark cluster master uri> --class AR.Main --executor.memory 20G --driver.memory 20G <your jar file path> hdfs://<输入文件路径> hdfs://<输出文件路径> hdfs://<临时文件路径>
```

## 参考

- [Linux环境Spark安装配置及使用](https://juejin.cn/post/6844903839506792462)
- [Spark 2.2.0中文文档](http://spark.apachecn.org/#/docs/3)
- [开发工具之Spark程序开发详解](http://t.zoukankan.com/frankdeng-p-9092512.html)
- [linux怎么安装hadoop](https://m.php.cn/article/486140.html)
- [[Scala FPGrowth类代码示例](https://vimsky.com/examples/detail/scala-class-org.apache.spark.mllib.fpm.FPGrowth.html)](https://vimsky.com/examples/detail/scala-class-org.apache.spark.mllib.fpm.FPGrowth.html)
- [关联规则之FpGrowth算法以及Spark实现](https://blog.csdn.net/u013771019/article/details/107244180/)
