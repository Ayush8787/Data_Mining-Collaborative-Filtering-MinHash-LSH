from pyspark import SparkContext, SparkConf
import sys
import json, time, string, math
from collections import Counter

st = time.time()

conf = SparkConf().setMaster("local").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
sc = SparkContext(conf=conf)

sc.setLogLevel("ERROR")

start = time.time()
input = sys.argv[1]
model = sys.argv[2]
output = sys.argv[3]


def result(xx, yy):
    
    if xx is not None and yy is not None:
        aa = len(set(xx).intersection(set(yy)))
        bb = math.sqrt(len(set(xx))) * math.sqrt(len(set(yy)))
        return aa / bb
    else:
        return 0


model_file_lines = sc.textFile(model).map(lambda row: json.loads(row))


user_profile_1 = model_file_lines.filter(lambda kv: kv["info"] == "user_profile").map(
    lambda x: (x["key"], x["value"])).collect()
user_profile_dic = {}
for i in user_profile_1:
    user_profile_dic[i[0]] = i[1]

business_profile_1 = model_file_lines.filter(lambda kv: kv["info"] == "busi_profile").map(
    lambda x: (x["key"], x["value"])).collect()
business_profile_dic = {}
for i in business_profile_1:
    business_profile_dic[i[0]] = i[1]

temp = []

#####Have to use get method in dictionary due to key error may occures

take_data = sc.textFile(input).map(lambda row: json.loads(row))


result_predict = take_data.map(lambda x: (x["user_id"], x["business_id"])).map(
    lambda x: ((x), result(user_profile_dic.get(x[0]), business_profile_dic.get(x[1])))).filter(lambda x: x[1] > 0.01)

for i in result_predict.collect():
    temp.append({"user_id": i[0][0],
                 "business_id": i[0][1],
                 "sim": i[1]})

with open(output, 'w+') as output_file:
    for item in temp:
        output_file.writelines(json.dumps(item) + "\n")
    output_file.close()

print((time.time() - start))
