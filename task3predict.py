from pyspark import SparkContext, SparkConf
import sys
import json, time

st = time.time()

conf = SparkConf().setMaster("local").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
sc = SparkContext(conf=conf)

sc.setLogLevel("ERROR")

# train = "C:\\Users\\acp87\\PycharmProjects\\untitled1\\train_review.json"
# test = "C:\\Users\\acp87\\PycharmProjects\\untitled1\\test_review.json"
# model = "C:\\Users\\acp87\\PycharmProjects\\untitled1\\task3_11"
# output = "CHeckthisAyushghghNonea"
# model_ = "item_based"
# business_avg = "C:\\Users\\acp87\\PycharmProjects\\untitled1\\business_avg.json"
# user_avg_file_path = "C:\\Users\\acp87\\PycharmProjects\\untitled1\\user_avg.json"

train = sys.argv[1]
test = sys.argv[2]
model = sys.argv[3]
output = sys.argv[4]
model_ = sys.argv[5]
business_avg = "../resource/asnlib/publicdata/business_avg.json"
user_avg_file_path = "../resource/asnlib/publicdata/user_avg.json"

input_1 = sc.textFile(train).map(lambda x: json.loads(x))
final_input = input_1.map(lambda x: (x["user_id"], x["business_id"], x["stars"]))

user_li = final_input.map(lambda x: x[0]).distinct().sortBy(lambda x: x).collect()
business_li = final_input.map(lambda x: x[1]).distinct().sortBy(lambda y: y).collect()
user_dic = {}
user_dic_rev = {}
inc = 0
for i in user_li:
    if i not in user_dic.keys():
        user_dic[i] = inc
        user_dic_rev[inc] = i
        inc += 1
    else:
        pass

business_dic = {}
business_dic_rev = {}
inc = 0
for i in business_li:
    if i not in business_dic.keys():
        business_dic[i] = inc
        business_dic_rev[inc] = i
        inc += 1
    else:
        pass

if model_ == "item_based":

    def getresult(data):
        xx = data[0]

        temp = []
        for i in list(data[1]):
            temp.append(tuple((i[1], model_file_dic.get(tuple((i[0], xx)), 0))))
        temp_11 = sorted(temp, key=lambda x: x[1], reverse=True)
        c = 0
        temp_1 = []
        for k in temp_11:
            if c != 3:
                temp_1.append(k)
                c += 1
            else:
                break
        temp_2 = []
        for k in temp_1:
            temp_2.append(k[0] * k[1])
        n = sum(temp_2)
        ToC = business_dic_rev.get(xx)
        if n != 0:
            temp_3 = []
            for kk in temp_1:
                temp_3.append(abs(kk[1]))
            d = sum(temp_3)
            if d == 0:
                return tuple((xx, bus_avg_dict.get(ToC, 0)))   # piazza suggestion to use avg from avg files
            ans = n / d
            return tuple((xx, ans))
        else:
            return tuple((xx, bus_avg_dict.get(ToC, 0)))


    model_file = sc.textFile(model).map(lambda x: json.loads(x)) \
        .map(lambda x: (business_dic[x["b1"]], business_dic[x["b2"]], x["sim"]))

    model_file_dic = {}
    for i in model_file.collect():
        model_file_dic[(i[0], i[1])] = i[2]
        model_file_dic[(i[1], i[0])] = i[2]  # reverse order bcz of key error

    bus_avg_dict_111 = sc.textFile(business_avg).map(lambda x: json.loads(x)) \
        .map(lambda x: dict(x)).flatMap(lambda x: x.items())
    bus_avg_dict = bus_avg_dict_111.collectAsMap()

    test_1 = sc.textFile(test).map(lambda x: json.loads(x)).map(
        lambda x: (user_dic.get(x["user_id"]), business_dic.get(x["business_id"])))
    test_1_final = test_1.filter(lambda x: x[0] is not None and x[1] is not None)

    train_1 = final_input.map(lambda x: (user_dic[x[0]], [(business_dic[x[1]], x[2])])).reduceByKey(
        lambda x, y: list(set(x + y)))

    final_output_1 = test_1_final.leftOuterJoin(train_1)
    final_output = final_output_1.mapValues(
        lambda x: getresult(tuple(x))).filter(lambda x: x[1][1] != 0).collect()

    temp22 = []

    for ii in final_output:
        res = {
            "user_id": user_dic_rev[ii[0]],
            "business_id": business_dic_rev[ii[1][0]],
            "stars": ii[1][1]
        }
        temp22.append(res)

    with open(output, 'w') as file:
        for item in temp22:
            file.writelines(json.dumps(item) + "\n")
        file.close()


elif model_ == "user_based":

    def getresult(data):
        xx = data[0]
        temp = []
        for i in list(data[1]):
            this = user_dic_rev.get(i[0])
            avg = user_avg_dict.get(this, 0)
            temp.append(tuple((i[1], avg, model_file_dic.get(tuple((i[0], xx)), 0))))
        temp_2 = []
        for k in temp:
            temp_2.append((k[0] - k[1]) * k[2])
        temp_111 = sorted(temp, key=lambda x: x[1], reverse=True)
        c = 0
        temp_1111 = []
        for k in temp_111:
            if c != 3:
                temp_1111.append(k)
                c += 1
            else:
                break
        n = sum(temp_2)
        ToC = user_dic_rev.get(xx)
        if n != 0:
            temp_3 = []
            for kk in temp:
                temp_3.append(abs(kk[2]))
            d = sum(temp_3)
            if d == 0:
                return tuple((xx, user_avg_dict.get(ToC, 0)))   # piazza suggestion to use avg from avg files
            ans = n / d
            final_ans = user_avg_dict.get(ToC, 0) + ans
            return tuple((xx, final_ans))
        else:
            return tuple((xx, user_avg_dict.get(ToC, 0)))


    model_file = sc.textFile(model).map(lambda x: json.loads(x)) \
        .map(lambda x: (user_dic[x["u1"]], user_dic[x["u2"]], x["sim"]))

    model_file_dic = {}
    for i in model_file.collect():
        model_file_dic[(i[0], i[1])] = i[2]
        model_file_dic[(i[1], i[0])] = i[2]

    train_1 = final_input.map(lambda x: (business_dic[x[1]], [(user_dic[x[0]], x[2])])).reduceByKey(
        lambda x, y: list(set(x + y)))

    user_avg_dict_111 = sc.textFile(user_avg_file_path).map(lambda x: json.loads(x)).map(lambda x: dict(x)).flatMap(
        lambda x: x.items())
    user_avg_dict = user_avg_dict_111.collectAsMap()

    test_1 = sc.textFile(test).map(lambda x: json.loads(x)).map(
        lambda x: (business_dic.get(x["business_id"]), user_dic.get(x["user_id"])))
    test_1_final = test_1.filter(lambda x: x[0] is not None and x[1] is not None)

    final_output_1 = test_1_final.leftOuterJoin(train_1)
    final_output = final_output_1.mapValues(
        lambda x: getresult(tuple(x))).collect()

    temp23 = []
    for ii in final_output:
        res = {
            "user_id": user_dic_rev[ii[1][0]],
            "business_id": business_dic_rev[ii[0]],
            "stars": ii[1][1]
        }
        temp23.append(res)

    with open(output, 'w') as file:
        for item in temp23:
            file.writelines(json.dumps(item) + "\n")
        file.close()
