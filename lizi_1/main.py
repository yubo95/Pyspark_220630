from  pyspark import SparkContext, SparkConf
from pyspark.storagelevel import StorageLevel
from defs import context_jieba, count_word
if __name__ == '__main__':
    conf = SparkConf().setAppName("test")
    sc = SparkContext(conf = conf)

    #1.读
    file_rdd = sc.textFile("hdfs://node1:8020/input/SogouQ_test.txt")
    #2.分
    split_rdd = file_rdd.map(lambda x:x.split("\t"))
    #3.缓存
    split_rdd.persist(StorageLevel.DISK_ONLY)

    #业务需求
    context_rdd = split_rdd.map(lambda x:x[2])
    words_rdd = context_rdd.flatMap(context_jieba)
    print(words_rdd.collect())
    count_rdd = words_rdd.map(count_word)
    result1 = count_rdd.reduceByKey(lambda a,b:a +b).\
        sortBy(lambda x:x[1], ascending=False, numPartitions=1).\
        take(5)

    print("需求1结果：", result1)