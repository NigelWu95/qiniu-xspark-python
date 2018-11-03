# coding=utf-8


from pyspark.sql.types import StructField, StringType, StructType, LongType, FloatType

# 时间日期和请求次数的 Scheme
scheme_datetimecount = StructType([
    StructField('datetime', StringType()),
    StructField('count', LongType())])

# 时间日期和下载速度的 Scheme
scheme_datetimespeed = StructType([
    StructField('datetime', StringType()),
    StructField('speed', FloatType())])

# 从 row log 中拿出 datetime、resptime 和 fsize 字段，将 resptime 和 fsize 计算速度（B/ms ≈ KB/s），返回（datetime, speed）的 map，可以时间日期进行转换来改变统计粒度，这里取日期时间的前 15 位得到 "2018-09-01 00:0"，相当于 10 分钟作为粒度，因此每 10 分钟内的所有记录得到的 map 中的 key 均一致，可以进行聚合计算。
def computeGroupedTimeSpeed(log):
    groupDatetime = log.datetime[0:15]
    resptime = float(log.resptime)
    fsize = float(log.fsize)
    return (groupDatetime, fsize / resptime)

# reduce 操作，(v, v) -> v，迭代计算可得到同一个 datetime 下载速度的总和
def computeSpeedSum(speed1, speed2):
    return speed1 + speed2

# 将一个字典（dict）转换成列表（list），用于将计算得到的 [datetime]=count（dataframe 经过 countByKey() 得到的结果）结果转换成 (datetime, count) 的 list，list 可用于转换成 rdd 进一步转换成包含 datetime 和 count 字段的 dataframe
def dictToList(dic):
    keys = dic.keys()
    to_list = list()
    for key in keys:
        to_list.append((str(key), dic[key]))
    return to_list

# 筛选 200/206 状态码并且大小大于 100000 B 的记录取出划分粒度之后的 datetime 和 speed 字段
rdd_timespeed = df_io.filter("method == \"GET\"").filter("respcode == 200 or respcode == 206").filter(
    "fsize > 100000").rdd.map(lambda log: computeGroupedTimeSpeed(log))
# 对包含（datetime, speed）的记录进行 reduce 计算，得到同一 datetime 的 speed 总和，可以将（datetime, speed）和（datetime, speedSum）的结果注册成临时表进行 SQL 查询
rdd_speedsum = rdd_timespeed.reduceByKey(computeSpeedSum)
spark.createDataFrame(rdd_timespeed, scheme_datetimespeed).createOrReplaceTempView("result_timespeed")
spark.createDataFrame(rdd_speedsum, scheme_datetimespeed).createOrReplaceTempView("result_speedsum")
# 将（datetime, speed）的记录按照 datetime 分组计数，得到的 dict 转换成 list 后可通过 sc.parallelize() 得到 dataframe，再注册成结果表
rdd_timecount = sc.parallelize(dictToList(rdd_timespeed.countByKey()))
spark.createDataFrame(rdd_timecount, scheme_datetimecount).createOrReplaceTempView("result_timecount")
