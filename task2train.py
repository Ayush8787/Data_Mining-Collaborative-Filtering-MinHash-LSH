from pyspark import SparkContext, SparkConf
import sys
import json, time, string, math
from collections import Counter

st = time.time()

conf = SparkConf().setMaster("local").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
sc = SparkContext(conf=conf)

sc.setLogLevel("ERROR")

input = sys.argv[1]
output = sys.argv[2]
stop = sys.argv[3]

linestop = sc.textFile(stop).collect()

dd = {}
for i in linestop:
    dd[i] = 1

user_dic = {}
business_dic = {}

input_1 = sc.textFile(input).map(json.loads)
input_1 = input_1.map(lambda kv: (kv["user_id"], kv["business_id"], kv["text"]))

user_li = input_1.map(lambda x: x[0]).distinct().sortBy(lambda x: x).collect()
business_li = input_1.map(lambda x: x[1]).sortBy(lambda y: y).collect()

inc = 0
for i in user_li:
    if i not in user_dic.keys():
        user_dic[i] = inc
        inc += 1
    else:
        pass

business_dic = {}
inc = 0
for i in business_li:
    if i not in business_dic.keys():
        business_dic[i] = inc
        inc += 1
    else:
        pass


def remove_punch(x):
    # sets: 19.8566138744
    # regex: 6.86155414581
    # translate: 2.12455511093
    # replace: 28.4436721802
    # x = x.lower()
    # x = re.sub('\,|\[|\(|\.|\!|\?|\:|\;|\]|\)|\\n', ' ', x)   #from first assignment but takes lot of time as per analysis on stackoverflow
    # instead suggested to use translate
    # print(x[1])
    temp = []
   
    removed_number_punc = x[1].translate(str.maketrans('', '', string.digits + string.punctuation))
    ll = removed_number_punc.split()
    for i in ll:
        if i not in dd.keys() and i != '' and i not in string.ascii_lowercase:
            temp.append(i)

    return (x[0], temp)


def getfrq(x):
    checklist = x[1]
    counter = Counter(checklist)
    maximum_freq = max(counter.values())
    # for i,v in counter.items():
    #     if v < 2:                     #error due to dictionary changes its size
    #         del counter[i]
    # print(counter)
    counter = dict(filter(lambda x: x[1] > 3, counter.items())) #0.00001% of all words ~ 3 times
    temp = []
    for i, v in counter.items():
        tt = v / maximum_freq
        temp.append(((i, x[0]), tt))
    return temp


def getvalue(x):
    toiterate = set(x[1])
    Ni = len(toiterate)
    N = len(business_dic)
    temp = []
    for i in toiterate:
        rr = math.log(N / Ni, 2)
        to = ((x[0], i), rr)
        temp.append(to)
    return temp


def func(x):
    temp = []
    count = 0
    for i in x:
        if count == 200:
            break
        temp.append(i[0])
        count += 1
    return temp


getwords = input_1.map(lambda x: (x[1], str(x[2].encode('utf-8')).lower())).reduceByKey(
    lambda x, y: x + " " + y).map(lambda x: remove_punch(x))
Get_Frequency = getwords.flatMap(lambda x: getfrq(x))



Find_Freq_in_all = Get_Frequency.map(lambda x: (x[0][0], [x[0][1]])).reduceByKey(lambda x, y: x + y).flatMap(
    lambda x: getvalue(x))

final = Get_Frequency.join(Find_Freq_in_all).map(
    lambda x: (x[0][1], (x[0][0], x[1][0] * x[1][1]))).groupByKey().mapValues(lambda x: func(list(x)))

mapping = {}
mapping_li = final.flatMap(lambda x: x[1]).distinct().collect()

inc = 0
for i in mapping_li:
    mapping[i] = inc
    inc += 1

bus_profile = final.mapValues(lambda x: [mapping[i] for i in x])



business_profile = {}
for i in bus_profile.collect():
    business_profile[i[0]] = i[1]


def mixit(x):
   
    toit = list(set(x[1]))
    temp_1 = []
    for i in toit:
      
        # temp = business_dic[i]
        try:
            temp_1.append(business_profile[i])
        except:
            print("here")
            pass
  
    return (x[0], temp_1)


#
user = input_1.map(lambda kv: (kv[0], [kv[1]])).reduceByKey(lambda x, y: x + y).map(lambda x: mixit(x)).flatMapValues(
    lambda x: x).reduceByKey(lambda x, y: list(set(x + y)))

user_profile = {}
for i in user.collect():
    user_profile[i[0]] = i[1]

li = []


for i, v in business_profile.items():
    res = {
        "info": "busi_profile",
        "key": i,
        "value": v
    }
    li.append(res)

for i, v in user_profile.items():
    res = {
        "info": "user_profile",
        "key": i,
        "value": v
    }
    li.append(res)

with open(output, 'w') as file:
    for item in li:
        file.writelines(json.dumps(item) + "\n")
    file.close()

end = time.time() - st
print(end)
