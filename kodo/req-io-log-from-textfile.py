# coding=utf-8

# 时间日期和请求次数的 Scheme
from types import FloatType, LongType

from pyspark.sql.types import StructField, StringType, StructType

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

# 从保存结果的文件中解析还原所有字段
def parseRowLog(log):
    index1 = log.find("uid=u")
    index2 = log.find(", datetime=u")
    index3 = log.find(", timestamp=")
    index4 = log.find(", respcode=")
    index5 = log.find(", resptime=")
    index6 = log.find(", method=u")
    index7 = log.find(", scheme=u")
    index8 = log.find(", host=u")
    index9 = log.find(", path=u")
    index10 = log.find(", content_type=u")
    index11 = log.find(", file_type=u")
    index12 = log.find(", fsize=")
    index13 = log.find(", key=u")
    index14 = log.find(", user_agent=u")
    index15 = log.find(", x_forward_ip=u")
    index16 = log.find(", x_from_cdn=u")
    index17 = len(log) - 1
    uid = log[index1 + 6:index2 - 1]
    datetime = log[index2 + 13:index3 - 1]
    timestamp = log[index3 + 12:index4]
    respcode = log[index4 + 11:index5]
    resptime = log[index5 + 11:index6]
    method = log[index6 + 11:index7 - 1]
    scheme = log[index7 + 11:index8 - 1]
    host = log[index8 + 9:index9 - 1]
    path = log[index9 + 9:index10 - 1]
    content_type = log[index10 + 17:index11 - 1]
    file_type = log[index11 + 14:index12 - 1]
    fsize = log[index12 + 8:index13]
    key = log[index13 + 8:index14 - 1]
    user_agent = log[index14 + 15:index15 - 1]
    x_forward_ip = log[index15 + 17:index16 - 1]
    x_from_cdn = log[index16 + 15:index17 - 1]
    return uid, datetime, long(timestamp), long(respcode), long(
        resptime), method, scheme, host, path, content_type, file_type, long(
        fsize), key, user_agent, x_forward_ip, x_from_cdn

# 从 tslog 空间中读取上述步骤产生的结果文件读取记录，并重新进行解析得到所有字段，便于后续进行多次计算和查询，同时将读取的日志信息通过 cache() 缓存下来。
rdd_origin = sc.textFile("qiniu://tslog/1380359203-0901/part-00000*").cache()
rdd_tslog = rdd_origin.map(lambda log: parseRowLog(log))
df_tslog = spark.createDataFrame(rdd_tslog, schema_io)
print "schemes:", df_tslog.dtypes
df_tslog.createOrReplaceTempView("parsedtslog")
rdd_timespeed = df_tslog.filter("method == \"GET\"").filter("respcode == 200 or respcode == 206").filter(
    "fsize > 100000").rdd.map(lambda log: computeGroupedTimeSpeed(log))
rdd_speedsum = rdd_timespeed.reduceByKey(computeSpeedSum)
spark.createDataFrame(rdd_timespeed, scheme_datetimespeed).createOrReplaceTempView("result_timespeed")
spark.createDataFrame(rdd_speedsum, scheme_datetimespeed).createOrReplaceTempView("result_speedsum")
rdd_timecount = sc.parallelize(dictToList(rdd_timespeed.countByKey()))
spark.createDataFrame(rdd_timecount, scheme_datetimecount).createOrReplaceTempView("result_timecount")