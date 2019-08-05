import para
import itertools
import time

memo = {
 1: [4096, 2, 24.265],
 2: [6144, 3, 25.606],
 3: [8192, 4, 26.87],
 4: [4096, 2, 60.321],
 5: [6144, 3, 45.45],
 6: [8192, 4, 43.916],
 7: [4096, 2, 29.396],
 8: [6144, 3, 30.407],
 9: [8192, 4, 36.122],
 10: [4096, 2, 98.7356],
 11: [6144, 3, 69.874],
 12: [8192, 4, 43.916]
}

# 提取json的应用信息
def accept(info, history):
    appInfos = info["app_info"]
    app_infos = []
    for app in appInfos:
        if app["id"] not in history:
            type = str(app["appName"])[:2]
            type = int(type)
            id = app["id"]
            clusterTimestamp = app["clusterTimestamp"]
            if memo.__contains__(type):
                memory = memo[type][0]
                cpu = memo[type][1]
                brust_time = memo[type][2]
                app_info = [id, type, memory, cpu, brust_time, clusterTimestamp]
                app_infos.append(app_info)
        # print(app_infos)
    # 按短作业排序
    app_infos.sort(key=lambda x: x[4], reverse=False)
    print(app_infos)
    return app_infos

def wrap_action(app_infos, history, avail_memory, avail_cpu):
    return_action = []
    if len(app_infos) != 0:
        if app_infos[0][2] <= avail_memory and app_infos[0][3] <= avail_cpu:
            tmp = {}
            tmp["id"] = app_infos[0][0]
            tmp["memory"] = app_infos[0][2]
            tmp["cpu"] = app_infos[0][3]
            tmp["clusterTimestamp"] = app_infos[0][5]
            tmp["predictTime"] = None
            if app_infos[0][0] not in history:
                return_action.append(tmp)
                history.append(tmp["id"])
            with open('/var/lib/hadoop-yarn/sjf_action.txt', 'a+') as f:
                f.write(str(app_infos[0][0])+" "+str(app_infos[0][1])+"\n")
    return return_action

def sjf(io_interface):
    print("SJF start!")
    history = []
    for t in itertools.count():
        time_started = time.time()/1000
        info = io_interface.get_current_state()
        while not info.__contains__("avail_memory"):
            info = io_interface.get_current_state()
        avail_memory = info["avail_memory"]
        avail_cpu = info["avail_cpu"]
        app_info = accept(info, history)
        action = wrap_action(app_info, history, avail_memory, avail_cpu)
        io_interface.update_action(action)
        print("Action: ", action)
        time_ended = time.time()/1000
        time.sleep(para.DecisionInterval-(time_ended-time_started))

