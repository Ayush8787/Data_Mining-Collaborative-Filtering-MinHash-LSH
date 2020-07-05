from pyspark import SparkContext, SparkConf, StorageLevel
import sys
import json, time
import random
from itertools import combinations

sc = SparkContext()

sc.setLogLevel("ERROR")

input = sys.argv[1]
output = sys.argv[2]

user_dic = {}
business_dic = {}

st = time.time()
input_1 = sc.textFile(input).map(json.loads)
input_1 = input_1.map(lambda kv: (kv["user_id"], kv["business_id"]))

user_li = input_1.map(lambda x: x[0]).distinct().sortBy(lambda y: y)
user_li_ = user_li.collect()
# m = len(user_li)
inc = 0
for i in user_li_:
    if i not in user_dic.keys():
        user_dic[i] = inc
        inc += 1

business_li = input_1.map(lambda x: x[1]).distinct().sortBy(lambda y: y)
business_li_ = business_li.collect()

inc = 0
business_dic = {}
business_dic_re = {}
for i in business_li_:
    if i not in business_dic.keys():
        business_dic[i] = inc
        business_dic_re[inc] = i
        inc += 1


def dividebands(x, ban):
    temp1 = []
    for i in range(ban):
        thiss = tuple(x[1][i * r:(i + 1) * r])
        temp1.append(((i, tuple(sorted(thiss))), [x[0]]))  # need k to be a tuple coz list arent hashable
    return temp1


p = 1711613751
ab_val = []

numk = 50
list_a = [9602301451, 1810371054, 1711880287, 2990197860, 5448149766, 7548485074, 8612973198, 6018534074, 7746976584,
          6814980327, 8037182131, 2830205057, 8821351759, 6148284045, 8723209519, 3356955718, 2735455070, 4952041158,
          4756956144, 2146313803, 5944502126, 3973006687, 5993418413, 2332278306, 4563445419, 6882488068, 6316428765,
          1937315339, 9108334079, 8399085493, 4506418782, 1640674574, 8461181292, 7710321322, 5332418740, 1565486451,
          5853374848, 3558948034, 4664902168, 8692674982, 8019130550, 2508702208, 7390264375, 4905929224, 2184306900,
          2127519372, 2770441918, 3788185881, 4176346890, 8954329547]
list_b = [8311632461, 2777916847, 3483824021, 2531308441, 6817264843, 5509776728, 9185288511, 5039069464, 2719841303,
          7476629567, 4885838895, 8251515531, 7497571300, 8631409279, 1966945220, 6782909734, 4008845630, 2530391092,
          3137036609, 2197317137, 2378848992, 4568927107, 8076624066, 3880890766, 5209405668, 2257391659, 8216026932,
          2493071514, 8265964087, 7886991535, 3741595328, 5292715768, 7064315212, 5103240663, 6148279886, 2769580113,
          2626341793, 8170864460, 9264618798, 6784137418, 5405991257, 7132884256, 8967883277, 4002237104, 8772074767,
          7115429151, 4612235322, 5996074170, 7466226215, 6777037007]

m = len(user_li_)


# print("length", m)


def hash_it(x):
    index = user_dic[x]
    # print(type(index))
    # print(user_dic[x])
    temp = []
    for ii in range(50):
        a = list_a[ii]
        b = list_b[ii]
        # print("here", index)
        ee = ((index * (((a * index) + b) % m) + index * ((((a * index) + b) % p) % m) + index * index) % p) % m
        # print(ee)
        temp.append(ee)
    return (index, temp)


def getJacard(data):
    # print("herere")
    x = data[0][0]
    y = data[0][1]
    business1 = set(use_this_dictionary[x])
    business2 = set(use_this_dictionary[y])
    similarity = len(business1 & business2) / len(business1 | business2)
    return ((x, y), similarity)


def get_pairs(data):
    pairs = []
    # print(data[1])
    cc = list(data[1])
    cc.sort()
    for i in range(len(cc)):
        for j in range(i + 1, len(cc)):
            pairs.append(((cc[i], cc[j]), 1))
    return pairs


final_input_user_bus = input_1.map(lambda x: (user_dic[x[0]], [business_dic[x[1]]])).reduceByKey(lambda x, y: x + y)

final_input_bus_user = input_1.map(lambda x: (business_dic[x[1]], [user_dic[x[0]]])).reduceByKey(lambda x, y: x + y)

use_this_dictionary = {}
for ii in final_input_bus_user.collect():
    use_this_dictionary[ii[0]] = ii[1]

thisrdd = user_li.map(lambda x: hash_it(x))


def get_flat(l1, l2):
    temp = []
    for i, j in zip(l1, l2):
        if i > j:
            temp.append(j)
        else:
            temp.append(i)
    return temp


matrix = final_input_user_bus.leftOuterJoin(thisrdd).map(lambda x: x[1]).flatMap(
    lambda x: [(yy, x[1]) for yy in x[0]]).reduceByKey(get_flat).coalesce(2)

bcc = 50  # total has function by rows per band
r = 1  # br = n as per mentioned

final_candidate_pool = matrix.flatMap(lambda x: dividebands(x, bcc)).reduceByKey(lambda x, y: x + y)
final_candidate_pool_filter = final_candidate_pool.filter(lambda x: len(x[1]) > 1)

pairs = final_candidate_pool_filter.flatMap(lambda x: get_pairs(x)).reduceByKey(lambda x, y: x + y)

jacardSim = pairs.map(lambda x: getJacard(x)).filter(lambda x: float(x[1]) >= 0.05).collect()
result11 = []

for i in jacardSim:
    result11.append({"b1": business_dic_re[i[0][0]],
                     "b2": business_dic_re[i[0][1]],
                     "sim": i[1]})

with open(output, 'w') as file:
    for item in result11:
        file.writelines(json.dumps(item) + "\n")
    file.close()

end = time.time()
print(end - st)
