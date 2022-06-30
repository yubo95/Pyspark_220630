# coding:utf8
from pyspark import SparkContext, SparkConf
if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    stu_info = [(1,'yubo',22),(2,'like',21)]
    #1.将本地对象标记成广播变量
    broadcast = sc.broadcast(stu_info)

    score_info = sc.parallelize([(1,'cn',99),(2,'en',60),(1,'cn',88)])

    def map_func(data):
        id = data[0]
        name = ""
        # 2.使用本地集合的地方，取广播变量
        for info in broadcast.value:
            stu_id = info[0]
            if id == stu_id:
                name = info[1]
        return (name,data[1],data[2])


    print(score_info.map(map_func).collect())