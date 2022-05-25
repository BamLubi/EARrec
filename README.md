# EARrec
 Efficient association rule Recommendation System for big data - The Homework of Advanced operating system

## 基本步骤

- 频繁模式挖掘
  - PFP-Growth算法
- 关联规则生成
- 关联规则匹配
- 推荐分值计算
  - 使用最高置信度作为候选项分值

## 运行环境

![](https://img.shields.io/badge/hadoop-2.7.1-brightgreen)
![](https://img.shields.io/badge/spark-3.2.1-brightgreen)
![](https://img.shields.io/badge/scala-2.12.15-brightgreen)

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
