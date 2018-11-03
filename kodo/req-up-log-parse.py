# coding=utf-8

from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql import Row

# 解析后的日志字段
scheme = StructType([
    StructField('timestamp', StringType()),
    StructField('respcode', LongType()),
    StructField('resptime', LongType()),
    StructField('path', StringType()),
    StructField('host', StringType()),
    StructField('user_agent', StringType()),
    StructField('x_forward_for', StringType()),
    StructField('x_real_ip', StringType()),
    StructField('uid', StringType()),
    StructField('bucket', StringType()),
    StructField('key', StringType())])


# 从一个 json 字符串中取出 key 字段对应的 json 字符串，用于从原始 header 中得到 Token 信息
def getJsonStrByFindIndex(key, total_str):
    json_start = total_str.find("\"" + key + "\"")
    if json_start > -1:
        json_end = total_str.find("},\"", json_start)
        if json_end == -1:
            json_end = total_str.find("}}", json_start)
        json_str = total_str[json_start + len(key) + 3: json_end + 1]
        return json_str
    else:
        return ""


# 从一个 json 字符串中取出 key 对应的 value 值，用于从原始 header 中得到各字段信息
def getStrByFindIndex(key, total_str):
    str_start = total_str.find("\"" + key + "\"")
    if str_start > -1:
        str_end = total_str.find(",\"", str_start)
        if str_end == -1:
            str_end = total_str.find("}", str_start)
        target_str = total_str[str_start + len(key) + 3: str_end]
        return target_str.strip("\"")
    else:
        return ""


# 将原始日志（log 为通过 rdd 传递过来的每一行）解析得到目标字段，并返回一行 Row(...) 包含常用的字段
def parseLogItem(log):
    timestamp = log.timestamp
    respcode = log.respcode
    resptime = log.resptime
    method = log.method
    path = log.path

    host = getStrByFindIndex("Host", log.reqheader)
    user_agent = getStrByFindIndex("User-Agent", log.reqheader)
    x_forward_for = getStrByFindIndex("X-Forwarded-For", log.reqheader)
    x_real_ip = getStrByFindIndex("X-Real-Ip", log.reqheader)
    rs_info = getJsonStrByFindIndex("rs-info", log.respheader)
    bucket = getStrByFindIndex("bucket", rs_info)
    key = getStrByFindIndex("key", rs_info)

    token = ""
    if path == "/":
        token = getJsonStrByFindIndex("Token", log.respheader)
    else:
        token = getJsonStrByFindIndex("Token", log.reqheader)
    uid = getStrByFindIndex("uid", token)

    return Row(timestamp, respcode, resptime, path, host, user_agent, x_forward_for, x_real_ip, uid, bucket, key)


rdd = df.rdd.map(lambda log: parseLogItem(log))
df1 = spark.createDataFrame(rdd, scheme)
df1.createOrReplaceTempView('parsed_up_log')