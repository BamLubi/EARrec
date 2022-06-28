# EARrec
Efficient association rule Recommendation System for big data - The Homework of Advanced operating system

> 本赛题是面向大数据的高效关联规则推荐系统，具有数据量大的特点。 因此，使用基于 Spark 的大数据处理框架。本赛题不关注推荐的准确率，因 此使用最高值置信度作为推荐分值，并且只推荐 Top-1 的商品。

## 主要流程

主要流程包括以下四个步骤：

1. 频繁模式挖掘
2. 关联规则生成
3. 关联规则匹配
4. 推荐分值 计算

### 频繁模式挖掘

> 频繁模式是指在数据集中频繁出现的模式，常见的算法有`Apriori`、`FP-Growth`和`PFP-Growth`算法。主要任务是在购物篮数据集$D$中挖掘出频繁模式集合$P={P_1,P_2,...,P_s}$，其中对于每一个频繁模式需要满足大于最小支持度$min\_supp$，即：
> $$
> supprot(P_i)=|t_i|P_i\in t, t \in D| \ge min\_supp, 0 \le i \le s,t代表事务
> $$

### 关联规则生成

> 假设关联规则的后项仅包含一个项目，关联规则的前项包含该频繁模式剩下的所有的项目。则任意频繁模式$P_j = \{ i_{j1}, i_{j2}, ... ,i_{j|P_j|} \}$可以生成$|P_j|$条关联规则：
> $$
> R_{jk}:P_j - \{ i_{jk} \} \Rightarrow i_{jk}, i\le k\le |P_j|
> $$

### 关联规则匹配

> 给定等待推荐的用户数据集$U=\{T_1,T_2,...,T_{|U|}\}$，其中每个用户数据为历史浏览商品的集合$T_u = \{ i_1, i_2,...,i_{|T_u|} \}$。一条关联规则$R_{jk}$可以为$T_u$产生候选项，需要满足如下条件：
> $$
> s.t. \ P_j - \{ i_{jk} \} \sqsubseteq T_u,i_{jk} \notin T_u
> $$
> 则对用户$T_u$有用的关联规则集合如下：
> $$
> R_u = \{ R_{jk}:P_j - \{ i_{jk} \} \Rightarrow i_{jk} | 
> P_j - \{ i_{jk} \} \sqsubseteq T_u,i_{jk} \notin T_u,
> 1 \le k \le |P_j|
> \}
> $$

 ### 推荐分值计算

> 对于用户$T_u$的关联规则集合$R_u$，其中每一条关联规则都会带来一个推荐候选项$i_k$。这里对于候选推荐项使用最大置信度作为推荐分值，将所有推荐项的推荐分值从大到小排序，取Top-N的项作为推荐结果，这里选择Top-1作为结果。最大置信度计算如下：
> $$
> conf(R_{jk}) = \frac{support(P_j)}{support(P_j-\{ i_{jk} \})}
> $$
> 

 ## 频繁模式挖掘算法选择

### Apriori算法

该算法的核心思想是使用迭代循环的方法来减小搜索空间。首先从购物篮集合$D$中推导出大小为1的项集集合$C_1={i_1,i_2,...,i_m}$。因此，可以推出所有的频繁模式一定在$C_1$的子集当中产生，共有$2^m-1$种可能，即：
$$
\binom{m}{1} + \binom{m}{2} + ... + \binom{m}{m} = 2^m -1
$$
如果搜索所有的可能，则会耗时过长，因此引入最小支持度剪枝。Apriori算法的做法是从候选集$C_1$开始，通过筛选出满足最小支持度的1-频繁项集$L_1$。随后，在生成的项集$L_1$中生成大小为2的候选集$C_2$，即
$$
C_2 = \{ (i_1,i_2),(i_1,i_3),...,(i_m,i_n)\}, i_j \in L_1
$$
再根据候选集$C_2$生成2-频繁项集$L_2$，直到候选集筛选后为空的时候，退出迭代。则集合$\{L_1,L_2,...,L_n\}$为最终的频繁模式集合。虽然该方法实现起来简单，但是实际运行时发现耗费时间依旧过长，并且候选集合数据庞大，很容易堆栈溢出。

![](https://pic1.zhimg.com/80/v2-bea6c42ce1e630b6bb2fe6b0f68679c0_720w.jpg)

### FP-Growth算法

该算法的核心思想是分治的策略，通过不产生候选集和的方式生成频繁模式。首先压缩数据集并建立频繁模式树，统计每一个项在原始数据集中出现的次数，与最小支持度比较，去除低频项。然后根据出现次数对剩余项目进行升序排序。从第一位项开始扫描频繁模式树的叶子节点，通过回溯得到每一个项对应的条件模式基。根据条件模式基构建条件频繁项集树。根据这棵树和最小支持度得到最终的频繁项集。该算法实现起来复杂，并且需要构建大量的树，空间复杂度较高。

## 算法优化

- 对于多次使用的数据，提前`collect()`。Spark对于RDD数据不会立即计算，而是记录下数据的有向无环图DAG，当需要使用的时候就从头开始计算，这无疑会增加计算量。因此对于频繁使用的数据，可以提前收集并存储。比如，关联规则可以提前收集，在关联规则匹配阶段，需要多次使用到生成的关联规则。即调用`pattern.collect()`。
- 对于大文件数据，可以自定义分区个数，每个分区将由不同的task完成，以此可以提高计算速度。比如，在关联规则匹配阶段，用户概貌数据较大，并且对于每一个用户数据都需要与关联规则进行匹配，耗费时间较长。因此可以设置多个分区，即`sparkContext.textFile(userFile).repartition(10)`。
- 当数据经过过滤之后，数据变得稀疏，并不需要那么多的分区，因此可以将分区数据合并，即`coalesce(numPartition, isShuffle)`。
- 使用`mapPartition`算子替代map算子以提高性能。map算子是对RDD中的每一个元素进行操作，而`mapPartition`算子是对RDD的每一个分区的迭代器进行操作，与repartition()一起使用可以大大提高速度。但是一个分区如果有很多数据，会造成OOM，但是map就没有这样的问题。

## 运行环境

![](https://img.shields.io/badge/hadoop-2.7.1-brightgreen)
![](https://img.shields.io/badge/spark-3.2.1-brightgreen)
![](https://img.shields.io/badge/scala-2.12.15-brightgreen)

在1W的数据集上，选择最小支持度0.2，最小置信度0.6，选择2W的用户数据集进行测试，与样例结果相比ACC=0.93。

## 提交要求

打包成jar包，使用spark-submit运行

```shell
$ spark-submit --master local[*] --class Main --executor.memory 20G --driver.memory 20G EARrec.jar hdfs://<输入购物篮文件路径> hdfs://<输入用户文件路径> hdfs://<输出频繁模式路径> hdfs://<输出推荐结果路径>
```

```
hdfs://127.0.0.1:9000/earrec/sample/trans_3W.txt
hdfs://127.0.0.1:9000/earrec/sample/test_2W.txt
hdfs://127.0.0.1:9000/earrec/out/pattern
hdfs://127.0.0.1:9000/earrec/out/result
```

## 参考

- [Linux环境Spark安装配置及使用](https://juejin.cn/post/6844903839506792462)
- [Spark 2.2.0中文文档](http://spark.apachecn.org/#/docs/3)
- [开发工具之Spark程序开发详解](http://t.zoukankan.com/frankdeng-p-9092512.html)
- [linux怎么安装hadoop](https://m.php.cn/article/486140.html)
- [[Scala FPGrowth类代码示例](https://vimsky.com/examples/detail/scala-class-org.apache.spark.mllib.fpm.FPGrowth.html)](https://vimsky.com/examples/detail/scala-class-org.apache.spark.mllib.fpm.FPGrowth.html)
- [关联规则之FpGrowth算法以及Spark实现](https://blog.csdn.net/u013771019/article/details/107244180/)
- [SparkConf常见参数设置](https://blog.csdn.net/yu7888/article/details/122666184)
- [数据挖掘随笔（一）频繁模式挖掘与关联规则挖掘以及Apriori算法（python实现）](https://zhuanlan.zhihu.com/p/410019734)
- [数据挖掘随笔（二）FP-growth算法——一种用于频繁模式挖掘的模式增长方式(Python实现)](https://zhuanlan.zhihu.com/p/411594391)
