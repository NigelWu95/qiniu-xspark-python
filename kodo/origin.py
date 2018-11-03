# coding=utf-8

import datetime
import sys
from qiniu import http
from qiniu import Auth, BucketManager

# kodo-log 账号的 ak、sk
access_key = ""
secret_key = ""
auth = Auth(access_key, secret_key)
bucketManager = BucketManager(auth)

# 获取到账号下的空间列表
buckets, err = bucketManager.buckets()
if err.status_code is not 200:
    print err
    sys.exit(-1)

# 通过 Zepplin 的单选框来选择日志源空间
bucket_options = []
for bucket in sorted(buckets):
    bucket_options.append((bucket, bucket))
bucket_selected = z.select('bucket_selected', bucket_options)
bucket_custom = z.input("bucket_custom", "")
bucket = bucket_selected
if bucket_custom != "":
    bucket = bucket_custom

# get domain 使用空间域名的列举接口拿到一个域名
domains, err = http._post_with_auth('http://api.qiniu.com/v6/domain/list?tbl=' + bucket, None, auth)
if err.status_code is not 200:
    print err
    sys.exit(-1)
if len(domains) is 0:
    print 'no domain in bucket'
    sys.exit(-1)

# 通过 Zepplin 初始化对象设置 hadoop 的参数配置（用来从空间中下载文件)
sc._jsc.hadoopConfiguration().set("fs.qiniu.access.key", access_key)
sc._jsc.hadoopConfiguration().set("fs.qiniu.secret.key", secret_key)
sc._jsc.hadoopConfiguration().set("fs.qiniu.bucket.domain", "http://" + domains[0])
sc._jsc.hadoopConfiguration().set("fs.qiniu.file.retention", "55")

# 输入框拿到起止日期和小时的值
dateFormat = "%Y-%m-%d"
today = datetime.date.today().strftime(dateFormat)
dateFrom = z.input("dateFrom", today)
hourFrom = z.input("hourFrom", "00")
dateTo = z.input("dateTo", dateFrom)
hourTo = z.input("hourTo", hourFrom)
dFrom = datetime.datetime.strptime(dateFrom, dateFormat)
dTo = datetime.datetime.strptime(dateTo, dateFormat)
if dFrom > dTo:
    print 'dateFrom should be earlier than dateTo'
    sys.exit(-1)

readFile = "qiniu://" + bucket + "/export-parquet/date=%s/hour=%02d"
readFiles = []

# 遍历出所有天数每个小时分割的日志文件
d = dFrom
while d <= dTo:
    hFrom = 0
    hTo = 23
    if d == dFrom:
        hFrom = hourFrom
    if d == dTo:
        hTo = hourTo
    for hour in range(int(hFrom), int(hTo) + 1):
        readFiles.append(readFile % (d.strftime(dateFormat), hour))
    d += datetime.timedelta(1)

# 将路径列表作为参数传递给 readFiles 来读取 parquet 文件
df = sqlContext.read.parquet(*readFiles)
print "schemes:", df.dtypes
print "files:", readFiles

# 注册 SQL 临时表（视图），表名为 log
df.createOrReplaceTempView("log")