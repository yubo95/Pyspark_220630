# coding:utf8
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("WordCountHelloWorld")

    #通过SparkConf对象创建SparkContext对象
    sc = SparkContext(conf = conf)

    #需求：wordcount单词计数，读取HDFS上的words.txt文件，并统计单词出现的数量
    #读取文件

    #读取文件 "hdfs://node1:8020/input/words.txt"
    file_rdd = sc.textFile("hdfs://node1:8020/input/words.txt")

    #切割单词
    words_rdd = file_rdd.flatMap(lambda line:line.split(" "))

    #将单词转换为元组对象
    words_with_one_rdd = words_rdd.map(lambda x: (x,1))

    #聚合
    result_rdd = words_with_one_rdd.reduceByKey(lambda  a, b:a + b)
    print(result_rdd.collect())