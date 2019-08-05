import para
import itertools
import time
from prediction import simulation

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


def prediction_time(type, used_memory, used_cpu):
    cpu_rate = used_cpu / para.TotalCore
    memory_rate = used_memory / para.TotalMemory
    if type == 1:
        pre_time = 31.89 * pow(cpu_rate, 3.141) + 17.92
        return pre_time

    elif type == 2:
        pre_time = 73.32 * pow(cpu_rate, 5.513) + 19
        return pre_time

    elif type == 3:
        pre_time = 42.42 * pow(cpu_rate, 3.52) + 20.12
        return pre_time

    elif type == 4:
        pre_time = 71.34 * pow(cpu_rate, 2.689) + 46.71
        return pre_time

    elif type == 5:
        pre_time = 49.23 * pow(cpu_rate, 2.632) + 36.46
        return pre_time

    elif type == 6:
        pre_time = 55.5 * pow(cpu_rate, 2.785) + 33.46
        return pre_time

    elif type == 7:
        pre_time = 1.119 * pow(cpu_rate, 4.239) + 0.5888
        return pre_time

    elif type == 8:
        pre_time = 51.58 * pow(cpu_rate, 4.002) + 24.19
        return pre_time

    elif type == 9:
        pre_time = 134.4 * pow(cpu_rate, 5.049) + 27.19
        return pre_time

    elif type == 10:
        pre_time = 356.5 * pow(cpu_rate, 6.283) + 83.67
        return pre_time

    elif type == 11:
        pre_time = 306.7 * pow(cpu_rate, 5.977) + 56.72
        return pre_time

    elif type == 12:
        pre_time = 55.5 * pow(cpu_rate, 2.785) + 33.46
        return pre_time

    else:
        print("There is no this type")


# 提取json的应用信息
def accept(info, app_time):
    used_memory = para.TotalMemory - info["avail_memory"]
    used_cpu = para.TotalCore - info["avail_cpu"]
    # used_memory = 0
    # used_cpu = 0
    appInfos = info["app_info"]
    app_infos = []
    runningAppInfos = info["running_app_info"]
    running_app_infos = []
    app_timeout_order = []
    for app in appInfos:
        # if app["id"] not in history:
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

    # if len(app_timeout_order) != 0:
    #     time_out = simulation(app_timeout_order)[1]
    #     # 新到的应用需要初始化到达时间和超时时间，key是id，value是[到达时间， 超时时间]
    #     for i in range(len(app_infos)):
    #         if app_infos[i][0] not in app_time:
    #             arriving_time = time.time() / 1000
    #             app_time[app_infos[i][0]] = [arriving_time, time_out]

    short_app_infos = app_infos.copy()
    small_app_infos = app_infos.copy()
    # 短作业（运行时间短）
    short_app_infos.sort(key=lambda x: x[4], reverse=False)
    # 小作业(资源少)
    small_app_infos.sort(key=lambda x: x[3], reverse=False)

    for app in runningAppInfos:
        type = str(app["appName"])[:2]
        type = int(type)
        id = app["id"]
        clusterTimestamp = app["clusterTimestamp"]
        if memo.__contains__(type):
            memory = memo[type][0]
            cpu = memo[type][1]
            brust_time = memo[type][2]
            app_info = [id, type, memory, cpu, brust_time, clusterTimestamp]
            running_app_infos.append(app_info)
            running_app_infos.sort(key=lambda x: x[4], reverse=False)

    return app_infos, small_app_infos, short_app_infos, running_app_infos, used_memory, used_cpu, app_time

def wrap_action(app_info, history, used_memory, used_cpu, small_app_info, short_app_info, app_time):
    return_action = []
    tmp = {}
    min_time = 100000000
    best = []
    num = 0
    action = []
    multi_action = []
    time_out_action = []
    # 实际的可用资源为
    avail_memory1 = para.TotalMemory * para.threshold - used_memory
    avail_cpu1 = para.TotalCore * para.threshold - used_cpu
    if len(app_info) != 0:
        if avail_memory1 > 0 and avail_cpu1 > 0:
            # # 先看看有没有应用等待时间超过阈值，如果有则提交
            # for i in range(len(app_info)):
            #     if time.time() / 1000 - app_time[app_info[i][0]][0] > app_time[app_info[i][0]][1] * 10:
            #         print("wrong")
            #         avail_cpu1 = avail_cpu1 - app_info[i][3]
            #         time_out_action.append(app_info[i])
            #         app_info.pop(i)
            # avail_cpu = avail_cpu1
            # short_app_info = app_info.copy()
            # small_app_info = app_info.copy()
            # # 短作业（运行时间短）
            # short_app_info.sort(key=lambda x: x[4], reverse=False)
            # # 小作业(资源少)
            # small_app_info.sort(key=lambda x: x[3], reverse=False)


            # 先按小作业优先尽可能的填
            for i in range(len(small_app_info)):
                if avail_cpu1 - small_app_info[i][3] >= 0:
                    action.append(small_app_info[i])
                    avail_cpu1 = avail_cpu1 - small_app_info[i][3]
                    num = num + 1
                if i == len(small_app_info) - 1 and len(action) != 0:
                    multi_action.append(action.copy())

            # 再找看看有没有应用数目形同的情况下，资源也相同的
            if num >= 1:
                for i in range(1, len(small_app_info)+1-num):
                    avail_cpu2 = para.TotalCore * para.threshold - used_cpu
                    # avail_cpu2 = avail_cpu
                    action.clear()
                    for j in range(num):
                        action.append(small_app_info[i+j])
                        avail_cpu2 = avail_cpu2 - small_app_info[i+j][3]
                    if avail_cpu2 == avail_cpu1:
                        multi_action.append(action.copy())

    # 进入下一个阶段决策
    if len(multi_action) != 0:
        if len(multi_action) > 1:
            min_id = 0
            for i in range(len(multi_action)):
                app_order = []
                app_order_id = []
                for j in range(len(multi_action[i])):
                    # 记录动作序列的类型和ID，方便预测
                    app_order.append(int(multi_action[i][j][1]))
                    app_order_id.append(int(multi_action[i][j][0]))
                for j in range(len(short_app_info)):
                    if short_app_info[j][0] not in app_order_id:
                        app_order.append(short_app_info[j][1])
                if min_time > simulation(app_order)[1]:
                    min_time = simulation(app_order)[1]
                    min_id = i
            best = multi_action[min_id].copy()
        else:
            best = multi_action[0].copy()

    # 如果有动作
    # if len(time_out_action):
    #     for i in range(len(time_out_action)):
    #         tmp["id"] = time_out_action[i][0]
    #         tmp["memory"] = time_out_action[i][2]
    #         tmp["cpu"] = time_out_action[i][3]
    #         tmp["clusterTimestamp"] = time_out_action[i][5]
    #         tmp["predictTime"] = None
    #         # 保证应用没有提交过，同时资源满足
    #         # print(tmp)
    #         return_action.append(tmp.copy())
    #         # print(return_action)
    #         history.append(time_out_action[i][0])
    #         print(time_out_action[i][0], "type", time_out_action[i][1])
    #         with open('/var/lib/hadoop-yarn/pas_action.txt', 'a+') as f:
    #             f.write(str(time_out_action[i][0])+" "+str(time_out_action[i][1])+"\n")

    # 如果有动作
    if len(best):
        for i in range(len(best)):
            tmp["id"] = best[i][0]
            tmp["memory"] = best[i][2]
            tmp["cpu"] = best[i][3]
            tmp["clusterTimestamp"] = best[i][5]
            tmp["predictTime"] = None
            # 保证应用没有提交过，同时资源满足
            if best[i][0] not in history:
                return_action.append(tmp.copy())
                history.append(best[i][0])
                print(best[i][0], "type", best[i][1])
                with open('/var/lib/hadoop-yarn/pas_action.txt', 'a+') as f:
                    f.write(str(best[i][0]) + " " + str(best[i][1]) + "\n")
    return return_action


def agent(io_interface):
    print("agent_final start!")
    app_time = {}
    history = []
    for t in itertools.count():
        time_started = time.time() / 1000
        info = io_interface.get_current_state()
        while not info.__contains__("avail_memory"):
            info = io_interface.get_current_state()
        app_info, small_app_info, short_app_info, running_app_info, used_memory, used_cpu, app_time = accept(info, app_time)
        action = wrap_action(app_info, history, used_memory, used_cpu, small_app_info, short_app_info, app_time)
        io_interface.update_action(action)
        # print("Action: ", action)
        time_ended = time.time() / 1000
        # 决策时间满足1s
        time.sleep(para.DecisionInterval - (time_ended - time_started))

