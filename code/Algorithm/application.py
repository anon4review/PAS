import para

class Application(object):

    def __init__(self, type):
        self.type = type
        self.pre_time = -1
        self.remaining_time = -1
        self.completion = 0
        if type % 3 == 1:
            self.cpu = 2
        elif type % 3 == 2:
            self.cpu = 3
        else:
            self.cpu = 4
        self.memory = self.cpu*2


    def resource_check(self, used_memory, used_cpu):
        if self.memory > 32 - used_memory or self.cpu > 16 - used_cpu:
            return False
        else:
            return True

    def update(self, complete_time, used_memory, used_cpu):
        # 完成度更新
        self.completion = self.completion + complete_time / self.pre_time
        # 新负载下的预测时间更新
        self.prediction_time(used_memory, used_cpu)
        # 剩余时间更新
        if 1-self.completion > 0:
            self.remaining_time = (1 - self.completion) * self.pre_time
        else:
            self.remaining_time = 0
        return self.remaining_time

    def prediction_time(self, used_memory, used_cpu):
        cpu_rate = used_cpu / para.TotalCore
        memory_rate = used_memory / para.TotalMemory
        if self.type == 1:
            self.pre_time = 31.89 * pow(cpu_rate, 3.141) +  17.92
            # 如果剩余时间还未生成
            if self.remaining_time == -1:
                self.remaining_time = self.pre_time
            return self.pre_time

        elif self.type == 2:
            self.pre_time = 73.32 * pow(cpu_rate, 5.513) + 19
            if self.remaining_time == -1:
                self.remaining_time = self.pre_time
            return self.pre_time

        elif self.type == 3:
            self.pre_time = 42.42 * pow(cpu_rate, 3.52) + 20.12
            if self.remaining_time == -1:
                self.remaining_time = self.pre_time
            return self.pre_time

        elif self.type == 4:
            self.pre_time = 71.34 * pow(cpu_rate, 2.689) + 46.71
            if self.remaining_time == -1:
                self.remaining_time = self.pre_time
            return self.pre_time

        elif self.type == 5:
            self.pre_time = 49.23 * pow(cpu_rate, 2.632) + 36.46
            if self.remaining_time == -1:
                self.remaining_time = self.pre_time
            return self.pre_time

        elif self.type == 6:
            self.pre_time = 55.5 * pow(cpu_rate, 2.785) + 33.46
            if self.remaining_time == -1:
                self.remaining_time = self.pre_time
            return self.pre_time

        elif self.type == 7:
            self.pre_time = 1.119 * pow(cpu_rate, 4.239) + 0.5888
            if self.remaining_time == -1:
                self.remaining_time = self.pre_time
            return self.pre_time

        elif self.type == 8:
            self.pre_time = 51.58 * pow(cpu_rate, 4.002) + 24.19
            if self.remaining_time == -1:
                self.remaining_time = self.pre_time
            return self.pre_time

        elif self.type == 9:
            self.pre_time = 134.4 * pow(cpu_rate, 5.049) + 27.19
            if self.remaining_time == -1:
                self.remaining_time = self.pre_time
            return self.pre_time

        elif self.type == 10:
            self.pre_time = 356.5 * pow(cpu_rate, 6.283) + 83.67
            if self.remaining_time == -1:
                self.remaining_time = self.pre_time
            return self.pre_time

        elif self.type == 11:
            self.pre_time = 306.7 * pow(cpu_rate, 5.977) + 56.72
            if self.remaining_time == -1:
                self.remaining_time = self.pre_time
            return self.pre_time

        elif self.type == 12:
            self.pre_time = 55.5 * pow(cpu_rate, 2.785) + 33.46
            if self.remaining_time == -1:
                self.remaining_time = self.pre_time
            return self.pre_time

        else:
            print("There is no this type", self.type)



