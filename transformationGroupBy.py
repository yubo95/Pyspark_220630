# reduceByKey、groupByKey和groupBy 对比
from pyspark import SparkContext, SparkConf
if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    rdd1 = sc.parallelize([('yubo',27),('huangshan',26),('ligouke',27),('caijichen',30),('ligouke','yunP')])
    groupRdd1 = rdd1.groupByKey()
    print(groupRdd1.map(lambda x: (x[0],list(x[1]))).collect())

    rdd2 = sc.parallelize([('yubo',27),('huangshan',26),('ligouke',27),('caijichen',30),('ligouke','yunP')])
    groupRdd2 = rdd2.groupBy(lambda x: x[0])
    print(groupRdd2.map(lambda x: (x[0],list(x[1]))).collect())

    rdd3 = sc.parallelize([('yubo',27),('huangshan',26),('ligouke',27),('caijichen',30),('ligouke',666)])
    reduceRdd3 = rdd3.reduceByKey(lambda a,b: a + b)
    print(reduceRdd3.collect())

    # [('yubo', [27]), ('huangshan', [26]), ('ligouke', [27, 'yunP']), ('caijichen', [30])]
    # [('yubo', [('yubo', 27)]), ('huangshan', [('huangshan', 26)]), ('ligouke', [('ligouke', 27), ('ligouke', 'yunP')]), ('caijichen', [('caijichen', 30)])]
    # [('yubo', 27), ('huangshan', 26), ('ligouke', 693), ('caijichen', 30)]