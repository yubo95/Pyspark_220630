# -*- coding:utf-8 -*-
"""
内容：
日期：2022年06月28日
"""
from pyspark import SparkContext, SparkConf
if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    rdd1 = sc.parallelize([1,3,5,2,4,9,7,6], 2)
    result = rdd1.foreach(lambda x : x + 5)
    print('rdd1:',rdd1.collect(),'re:',result)

    rdd2 = sc.parallelize([1,3,5,2,4,9,7,6], 2)
    result2 = rdd2.map(lambda x : x + 5)
    print('rdd2:',rdd2.collect(),'re2:',result2.collect())