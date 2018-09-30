% pyspark

from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType
from pyspark.sql import Row
import time, datetime

# 解析后的 IO 日志包含的字段和对应数据类型
schema_io = StructType([
    StructField('uid', StringType()),
    StructField('datetime', StringType()),
    StructField('timestamp', LongType()),
    StructField('respcode', LongType()),
    StructField('resptime', LongType()),
    StructField('method', StringType()),
    StructField('scheme', StringType()),
    StructField('host', StringType()),
    StructField('path', StringType()),
    StructField('content_type', StringType()),
    StructField('file_type', StringType()),
    StructField('fsize', LongType()),
    StructField('key', StringType()),
    StructField('user_agent', StringType()),
    StructField('x_forward_ip', StringType()),
    StructField('x_from_cdn', StringType())])

# 从一个 json 字符串中取出 key 字段对应的 json 字符串，用于从原始 header 中得到 Token 信息
def getJsonStrByFindIndex(key, total_str):
    if total_str == None:
        return ""
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
    if total_str == None:
        return ""
    str_start = total_str.find("\"" + key + "\"")
    if str_start > -1:
        str_end = total_str.find(",\"", str_start)
        if str_end == -1:
            str_end = total_str.find("}", str_start)
        target_str = total_str[str_start + len(key) + 3: str_end]
        return target_str.strip("\"")
    else:
        return ""

# 转换日期（格式：2018-09-01T00:00:00.792725Z）成为时间戳值
def parseDatetime(datetime):
    timestamp = 0
    if datetime == None:
        times = ""
    else:
        times = datetime.replace("T", " ")[0:19]
        timeArray = time.strptime(times, "%Y-%m-%d %H:%M:%S")
        timestamp = int(time.mktime(timeArray))
    return timestamp

# 将原始日志（log 为通过 rdd 传递过来的每一行）解析得到目标字段，并返回一行 Row(...) 包含所有字段
def parseIOLogItem(log):
    datetime = log.timestamp
    timestamp = parseDatetime(datetime)
    respcode = log.respcode
    resptime = log.resptime
    method = log.method
    scheme = ""
    host = ""
    path = log.path
    x_forwarded_ip = ""
    uid = ""
    scheme = ""
    user_agent = ""
    x_from_cdn = ""
    content_type = ""
    file_type = ""
    fsize = long(0)

    scheme = getStrByFindIndex("X-Scheme", log.reqheader)
    host = getStrByFindIndex("Host", log.reqheader)
    x_forward_ip = getStrByFindIndex("X-Forwarded-For", log.reqheader)
    user_agent = getStrByFindIndex("User-Agent", log.reqheader)
    x_from_cdn = getStrByFindIndex("X-From-Cdn", log.reqheader)
    token = getJsonStrByFindIndex("Token", log.respheader)
    uid = getStrByFindIndex("uid", token)
    content_type = getStrByFindIndex("Content-Type", log.respheader)
    file_type = getStrByFindIndex("FileType", log.respheader)
    size = getStrByFindIndex("Fsize", log.respheader)
    if size != "":
        fsize = long(size)
    key = getStrByFindIndex("Key", log.respheader)

    return Row(uid, datetime, timestamp, respcode, resptime, method, scheme, host, path, content_type, file_type, fsize,
               key, user_agent, x_forward_ip, x_from_cdn)

# 将获取原始日志得到的 df 对象转换成 rdd 后进行 map 操作得到新的 rdd 对象，使用定义好的 scheme 将其转换成 dataframe 后可以注册成一张临时表
rdd_io = df.rdd.map(lambda log: parseIOLogItem(log))
df_io = spark.createDataFrame(rdd_io, schema_io)
df_io.createOrReplaceTempView('parsed_req_io_log')