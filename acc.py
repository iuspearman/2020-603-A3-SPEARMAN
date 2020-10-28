act_class, pred_class = list(), list()
with open("shuttle.txt") as ds:
    for row in ds:
        act_class.append(row.split(",")[-1])
with open("shuttle_results.txt") as pred:
    for row in pred:
        pred_class.append(row.split()[1])

count = 0
for i in range(len(pred_class)):
    # print(pred_class[i], act_class[i])
    if int(pred_class[i]) == int(act_class[i]):
        count += 1

print(count/len(pred_class)*100)
