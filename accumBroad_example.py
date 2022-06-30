# -*- coding:utf-8 -*-
"""
内容：累加器和广播变量的案例
日期：2022年06月29日
"""
import re

from pyspark import SparkContext, SparkConf
if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 1.读取
    fileRdd = sc.textFile("../data/accumulator_broadcast_data.txt")
    # 特殊字符定义
    abnormalChar = [",", ".", "!", "#", "%"]

    # 2.本地driver列表定义广播变量
    broadcast = sc.broadcast(abnormalChar)

    # 3.统计特殊字符数量，定义累加器
    accumulator = sc.accumulator(0)

    # 4.处理数据，清洗空行
    #去除空行，保留非空行
    rdd = fileRdd.filter(lambda x: x.strip())
    #去除保留下来的非空行的前后(首尾)的空格
    line_rdd = rdd.map(lambda line: line.strip())

    # 5.对数据进行切分，按照正则切分(多个空格处理方法)
    words_rdd = line_rdd.flatMap(lambda line: re.split("\s+", line))

    # 6.过滤特殊符号数据，保留正常单词, 过滤过程中统计
    def filter_func(data):
        global accumulator
        # 取出广播变量特殊符号List
        abnormal_char = broadcast.value
        if data in abnormal_char:
            accumulator += 1
            return False # 特殊字符不要了
        else:
            return True

    normal = words_rdd.filter(filter_func)
    result = normal.map(lambda x:(x, 1)).\
        reduceBykey(lambda a,b :a+b)

    print("正常单词结果：",result)

    print("特殊字符数量：", accumulator)