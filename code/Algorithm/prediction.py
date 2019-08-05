import application

def simulation(app_order):
    total_time = 0
    used_memory = 0
    used_cpu = 0
    running_app = []
    total_turnaround_time = 0
    num = len(app_order)
    average_turnaround_time = 0

    for i in range(0, len(app_order)):
        a = 0
        app = application.Application(app_order[i])
        # 如果资源够用，存入队列的list
        while a == 0:
            if app.resource_check(used_memory, used_cpu) == True:
                # 池子占用资源增加
                used_memory = used_memory + app.memory
                used_cpu = used_cpu + app.cpu
                running_app.append(app)
                app.prediction_time(used_memory, used_cpu)
                # 更新池子应用情况
                for j in range(len(running_app)):
                    running_app[j].update(0, used_memory, used_cpu)
                a = 1
            else:
                min_time = 1000000000
                # min存储了最小剩余时间的应用在List中的下标
                min = -1
                for j in range(0, len(running_app)):
                    if running_app[j].remaining_time <= min_time:
                        min_time = running_app[j].remaining_time
                        min = j
                # print("最小应用", min, running_app[min].type)

                # 删除最小剩余执行时间的应用
                total_time = total_time + min_time
                total_turnaround_time = total_turnaround_time + min_time * num
                # print("时间", total_time)

                # 其他在池子的应用也删除这部分时间，并更新完成进度
                for k in range(len(running_app)):
                    running_app[k].remaining_time = running_app[k].remaining_time - min_time
                    running_app[k].update(min_time, used_memory, used_cpu)

                # 最小应用运行结束
                # print("min", min)
                used_memory = used_memory - running_app[min].memory
                used_cpu = used_cpu - running_app[min].cpu
                running_app.pop(min)
                num = num - 1

    # 最后对池子里剩余的进行时间统计
    while len(running_app) > 0:
        for i in range(len(running_app)):
            min_time = 1000000000
            # min存储了最小剩余时间的应用在List中的下标
            min = -1
            # 选出池中应用最小剩余的执行时间
            for j in range(len(running_app)):
                if running_app[j].remaining_time <= min_time:
                    min_time = running_app[j].remaining_time
                    min = j

            # 删除最小剩余执行时间的应用
            total_time = total_time + min_time
            total_turnaround_time = total_turnaround_time + min_time * num
            # print("时间", total_time)
            used_memory = used_memory - running_app[min].memory
            used_cpu = used_cpu - running_app[min].cpu
            # print(running_app[min].type, "完成")
            running_app.pop(min)
            num = num - 1
            # 其他在池子的应用也删除这部分时间
            for k in range(len(running_app)):
                running_app[k].remaining_time = running_app[k].remaining_time - min_time
                running_app[k].update(min_time, used_memory, used_cpu)

    if len(app_order) != 0:
        average_turnaround_time = total_turnaround_time / len(app_order)

    # print("总周转时间", total_time)
    return total_time, average_turnaround_time
