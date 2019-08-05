import itertools
import time
import numpy as np
import para
memo = {
    1: [4, 2, 24.265],
    2: [6, 3, 25.606],
    3: [8, 4, 26.87],
    4: [4, 2, 60.321],
    5: [6, 3, 45.45],
    6: [8, 4, 43.916],
    7: [4, 2, 29.396],
    8: [6, 3, 30.407],
    9: [8, 4, 36.122],
    10: [4, 2, 98.7356],
    11: [6, 3, 69.874],
    12: [8, 4, 43.916]
}


# 提取json的应用信息
def accept(info):
    appInfos = info["app_info"]
    app_infos = []
    for app in appInfos:
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
    return app_infos


# 计算矩阵的优先级向量
def priority_vector(matrix):
    sum_of_gm = 0
    gm = 1
    row = matrix.shape[0]
    column = matrix.shape[1]
    priority = []
    for i in range(row):
        for j in range(column):
            gm = gm * matrix[i, j]
        sum_of_gm = sum_of_gm + pow(gm, 1/len(matrix))
        priority.append(pow(gm, 1/len(matrix)))
        gm = 1
    priority = np.array(priority)
    priority = priority / sum_of_gm
    return priority


# 计算所有Resource_job矩阵的P.V.构成的Δ
def calculate_delta(resources):
    delta = []
    priority_cpu = priority_vector(np.array(resources['CPU']))
    priority_memory = priority_vector(np.array(resources['Memory']))
    delta.append(priority_cpu)
    delta.append(priority_memory)
    delta = np.array(delta).T
    return delta


# 计算PVP
def calculate_pvp(resources_job, resource):
    pvp = np.matmul(calculate_delta(resources_job), priority_vector(resource))
    return pvp


def wrap_action(app_infos, history, avail_memory, avail_cpu):
    action = []
    return_action = []
    tmp = {}
    cpu_job = [[0 for _ in range(len(app_infos))]for _ in range(len(app_infos))]
    memory_job = [[0 for _ in range(len(app_infos))]for _ in range(len(app_infos))]
    resource_job = {}
    # 构建CPU-Job判断矩阵
    for i in range(len(app_infos)):
        for j in range(len(app_infos)):
            if i == j:
                memory_job[i][j] = 1
            elif app_infos[i][2] == 4:
                if app_infos[j][2] == 6:
                    memory_job[i][j] = 1/3
                elif app_infos[j][2] == 4:
                    memory_job[i][j] = 1
                else:
                    memory_job[i][j] = 1/5
            elif app_infos[i][2] == 6:
                if app_infos[j][2] == 4:
                    memory_job[i][j] = 3
                elif app_infos[j][2] == 6:
                    memory_job[i][j] = 1
                else:
                    memory_job[i][j] = 1/2
            elif app_infos[i][2] == 8:
                if app_infos[j][2] == 4:
                    memory_job[i][j] = 5
                elif app_infos[j][2] == 8:
                    memory_job[i][j] = 1
                else:
                    memory_job[i][j] = 2
    # 构建Memory-Job判断矩阵
    for i in range(len(app_infos)):
        for j in range(len(app_infos)):
            if i == j:
                cpu_job[i][j] = 1
            elif app_infos[i][3] == 2:
                if app_infos[j][3] == 3:
                    cpu_job[i][j] = 1/3
                elif app_infos[j][3] == 2:
                    cpu_job[i][j] = 1
                else:
                    cpu_job[i][j] = 1/5
            elif app_infos[i][3] == 3:
                if app_infos[j][3] == 2:
                    cpu_job[i][j] = 3
                elif app_infos[j][3] == 3:
                    cpu_job[i][j] = 1
                else:
                    cpu_job[i][j] = 1/2
            elif app_infos[i][3] == 4:
                if app_infos[j][3] == 2:
                    cpu_job[i][j] = 5
                elif app_infos[j][3] == 4:
                    cpu_job[i][j] = 1
                else:
                    cpu_job[i][j] = 2
    resource_job["CPU"] = cpu_job
    resource_job["Memory"] = memory_job

    # 找到pvp值最大的应用
    pvp = calculate_pvp(resource_job, np.array(para.resource))
    index = np.argmax(pvp)
    if app_infos[index][2] < avail_memory and app_infos[index][3] < avail_cpu:
        avail_cpu = avail_cpu - app_infos[index][3]
        avail_memory = avail_memory - app_infos[index][2]
        if app_infos[index][0] not in history:
            action.append(app_infos[index])
            history.append(app_infos[index][0])

    if len(action) != 0:
        for i in range(len(action)):
            tmp["id"] = action[i][0]
            tmp["memory"] = action[i][2]
            tmp["cpu"] = action[i][3]
            tmp["clusterTimestamp"] = action[i][5]
            tmp["predictTime"] = None
            return_action.append(tmp)
            with open('/var/lib/hadoop-yarn/ahp_action.txt', 'a+') as f:
                f.write(str(action[i][0]) + " " + str(action[i][1]) + "\n")
    return return_action


def ahp(io_interface):
    print("AHP start!")
    history = []
    for t in itertools.count():
        time_started = time.time()/1000
        info = io_interface.get_current_state()
        while not info.__contains__("avail_memory"):
            info = io_interface.get_current_state()
        avail_memory = info["avail_memory"]
        avail_cpu = info["avail_cpu"]
        app_infos = accept(info)
        action = wrap_action(app_infos, history, avail_memory, avail_cpu)
        io_interface.update_action(action)
        print("Action: ", action)
        time_ended = time.time()/1000
        time.sleep(para.DecisionInterval-(time_ended-time_started))
