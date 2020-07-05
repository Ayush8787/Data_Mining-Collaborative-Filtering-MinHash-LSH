from pyspark import SparkContext, SparkConf
import sys
import json, time, string, math, random
from collections import Counter
from pyspark.sql import SparkSession

st = time.time()

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

start = time.time()
input = sys.argv[1]
output = sys.argv[2]
mode = sys.argv[3]

input_1 = sc.textFile(input).map(json.loads)
input_1 = input_1.map(lambda x: (x["user_id"], x["business_id"], x["stars"]))

user_li = input_1.map(lambda x: x[0]).distinct().sortBy(lambda x: x)
user_li_ = user_li.collect()

business_li = input_1.map(lambda x: x[1]).distinct().sortBy(lambda y: y)
business_li_ = business_li.collect()

# m = len(user_li)
inc = 0
user_dic = {}
user_dic_re = {}
for i in user_li_:
    if i not in user_dic.keys():
        user_dic[i] = inc
        user_dic_re[inc] = i
        inc += 1

inc = 0
business_dic = {}
business_dic_re = {}
for i in business_li_:
    if i not in business_dic.keys():
        business_dic[i] = inc
        business_dic_re[inc] = i
        inc += 1


def checkit_22(x):
    p1 = set(mapping_dic[x[0]].keys())
    p2 = set(mapping_dic[x[1]].keys())
    xx = len(p1.intersection(p2))
    if xx >= 3:
        x1 = []
        x2 = []
        for i in list(p1.intersection(p2)):
            x1.append(mapping_dic[x[0]][i])
            x2.append(mapping_dic[x[1]][i])

        avg1 = sum(x1) / len(x1)
        avg2 = sum(x2) / len(x2)
        aa = []
        for ii, jj in zip(x1, x2):
            aa.append((ii - avg1) * (jj - avg2))
        n = sum(aa)
        d1 = []
        d2 = []
        if n != 0:
            for pp, qq in zip(x1, x2):
                d1.append((pp - avg1) ** 2)
                d2.append((qq - avg2) ** 2)
            d = math.sqrt(sum(d1)) * math.sqrt(sum(d2))
            if d != 0:
                ans = n / d
                if ans > 0:
                    return (x[0], x[1], ans)


def get_pairs_22(can):
    pairs = []
    for i in range(len(can)):
        for j in range(i + 1, len(can)):
            pairs.append((can[i], can[j]))

    return pairs


if mode == "item_based":
    item_based_input = input_1.map(lambda x: (x[1], [(user_dic[x[0]], x[2])])).reduceByKey(lambda x, y: x + y).filter(
        lambda x: len(x[1]) >= 3)

    mapping_dic = {}
    for i in item_based_input.collect():
        a = i[0]
        user_id = []
        stars = []
        dd = {}
        for j in i[1]:
            dd[j[0]] = j[1]
        mapping_dic[a] = dd

    to_make_pair_Pool = item_based_input.map(lambda x: (1, [x[0]])).reduceByKey(lambda x, y: x + y)
    pairs = to_make_pair_Pool.flatMap(lambda x: get_pairs_22(x[1])).map(
        lambda x: checkit_22(x)).filter(lambda x: x is not None).collect()  # Total # of pairs  ---->  (n*(n-1))/2

    li = []
    for i in pairs:
        res = {
            "b1": i[0],
            "b2": i[1],
            "sim": i[2]
        }
        li.append(res)

    with open(output, 'w') as file:
        for item in li:
            file.writelines(json.dumps(item) + "\n")
        file.close()

    end = time.time() - st
    print(end)

else:

    def dividebands(x, ban):
        temp1 = []
        for i in range(ban):
            thiss = tuple(x[1][i * r:(i + 1) * r])
            temp1.append(((i, tuple(sorted(thiss))), [x[0]]))  # need k to be a tuple coz list arent hashable
        return temp1


    p = 1710711741
    ab_val = []

    numk = 45
    list_a = [6383777788, 8189580011, 2538693988, 7425076232, 5097867820, 8009593258, 2438559140, 4453214645,
              3320639863, 2883987868, 4445666501, 5649451162, 9354922952, 5399185838, 4464886692, 6863919033,
              2910308964, 6483746980, 6553413196, 4296039188, 3449571386, 1549471493, 5554575630, 3383574969,
              7201240094, 6666524841, 4528372629, 7078589071, 2978723210, 1642046119, 6811027205, 3764947109,
              5490460361, 5167614792, 4285276616, 5286033057, 6281910103, 7127908639, 4135746128, 5431773516,
              2433342381, 3509990093, 9186035294, 5016093060, 2120171700, 6039993304, 7991237996, 6954800230,
              1903451028, 6476001290, 6562268092, 7772135538, 2362549642, 6988865575, 6386991515]

    list_b = [9086320966, 1831247637, 4084289813, 4317353021, 6423257930, 8101120749, 5897679258, 1850381232,
              8279075384, 7385899354, 1722054685, 8676404255, 5157987520, 2893271444, 4777630142, 7861048072,
              8058938804, 1877393135, 7217738322, 5871113862, 8430876679, 8894647165, 7684086563, 6698077648,
              4673475296, 2999318486, 7458608345, 7647347112, 2481713990, 8238141148, 3980085040, 6920973187,
              5700952884, 6944873753, 6966214079, 8078473969, 7177631464, 6063822529, 8126688502, 4817575610,
              6299201932, 7716234183, 2595374108, 5515334407, 4490062818, 3170592174, 7978764573, 3117344817,
              6678338214, 6009207581, 8802448280, 1768125729, 7233984239, 2104506893, 6930484268]

    m = len(user_li_)


    # print("lenth", m)

    def hash_it(x):
        index = business_dic[x]
        # print(type(index))
        # print(user_dic[x])
        temp = []
        for ii in range(45):
            a = list_a[ii]
            b = list_b[ii]
            # print("here", index)
            ee = ((index * (((a * index) + b) % m) + index * ((((a * index) + b) % p) % m) + index * index) % p) % m
            # print(ee)
            temp.append(ee)
        return (index, temp)


    def get_jacard(data):
        # print("herere")
        x = data[0][0]
        y = data[0][1]

        aa = set(mapping_dic[x].keys())
        bb = set(mapping_dic[y].keys())
        if len(aa.intersection(bb)) >= 3:
            business1 = aa
            business2 = bb
            similarity = len(business1 & business2) / len(business1 | business2)
            if similarity >= 0.01:
                x1 = []
                x2 = []
                for i in list(aa.intersection(bb)):
                    x1.append(mapping_dic[x][i])
                    x2.append(mapping_dic[y][i])
                avg1 = sum(x1) / len(x1)
                avg2 = sum(x2) / len(x2)
                aa = []
                for ii, jj in zip(x1, x2):
                    aa.append((ii - avg1) * (jj - avg2))
                n = sum(aa)
                d1 = []
                d2 = []
                if n != 0:
                    for pp, qq in zip(x1, x2):
                        d1.append((pp - avg1) ** 2)
                        d2.append((qq - avg2) ** 2)
                    d = math.sqrt(sum(d1)) * math.sqrt(sum(d2))
                    if d != 0:
                        ans = n / d
                        if ans > 0:
                            return (x, y, ans)


    def get_pairs(data):
        pairs = []
        # print(data[1])
        cc = list(data[1])
        cc.sort()
        for i in range(len(cc)):
            for j in range(i + 1, len(cc)):
                pairs.append(((cc[i], cc[j]), 1))
        return pairs


    final_input_user_bus_1 = input_1.map(lambda x: (user_dic[x[0]], [(business_dic[x[1]], x[2])])).reduceByKey(
        lambda x, y: x + y)

    final_input_bus_user = input_1.map(lambda x: (business_dic[x[1]], [user_dic[x[0]]])).reduceByKey(lambda x, y: x + y)

    mapping_dic = {}
    for i in final_input_user_bus_1.collect():
        a = i[0]
        user_id = []
        stars = []
        dd = {}
        for j in i[1]:
            dd[j[0]] = j[1]
        mapping_dic[a] = dd

    thisrdd = business_li.map(lambda x: hash_it(x))


    def get_flat(l1, l2):
        temp = []
        for i, j in zip(l1, l2):
            if i > j:
                temp.append(j)
            else:
                temp.append(i)
        return temp


    signature_matrix_rdd = final_input_bus_user.join(thisrdd) \
        .map(lambda x: x[1]) \
        .flatMap(lambda x: [(yy, x[1]) for yy in x[0]]) \
        .reduceByKey(get_flat).coalesce(2)

    bcc = 45  # total has function by rows per band
    r = 1  # br = n as per mentioned

    final_candidate_pool = signature_matrix_rdd.flatMap(lambda x: dividebands(x, bcc)).reduceByKey(lambda x, y: x + y)
    final_candidate_pool_filter = final_candidate_pool.filter(lambda x: len(x[1]) > 1)

    candidate_Pairs = final_candidate_pool_filter.flatMap(lambda x: get_pairs(x)).reduceByKey(lambda x, y: x + y)

    jacardSim = candidate_Pairs.map(lambda x: get_jacard(x)).filter(lambda x: x is not None).collect()

    # result11 = []
    #
    result11 = []
    for i in jacardSim:
        result11.append({"u1": user_dic_re[i[0]],
                         "u2": user_dic_re[i[1]],
                         "sim": i[2]})
    #
    with open(output, 'w') as file:
        for item in result11:
            file.writelines(json.dumps(item) + "\n")
        file.close()

    end = time.time()
    print(end - st)
