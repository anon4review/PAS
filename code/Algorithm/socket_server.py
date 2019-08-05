import socket
import json
import threading
import SJF
import PAS
import AHP

HOST = ''
PORT_APPINFO = 8001
PORT_EXCU_TIME = 8002


class ThreadedTCPRequestHandler():
    def __init__(self):
        self.current_state = {}
        self.action = None
        self.lock = threading.Lock()
        self.execution_time = []

    def server_handler_appinfo(self, client):
        self.lock.acquire()
        # 接收数据
        try:
            buf = client.recv(80000000)
            data = json.loads(buf)
            #print("Data: ", data)
            #print("Get Data Time: ", int(round(time.time() * 1000)))
            self.current_state = data
        except json.decoder.JSONDecodeError:
            #print("JSONDecodeError!!!")
            pass

        # 返回数据
        if self.action is not None:
            results = self.action
            jresults = json.dumps(results)
            self.action = None
            client.sendall(bytes(jresults, encoding='utf8'))
        client.close()

        self.lock.release()

    def get_current_state(self):
        #print(self.current_state)
        return self.current_state

    def get_action(self):
        self.lock.acquire()
        action = self.action
        #print("Get_action: ", action)
        self.lock.release()
        return action

    def update_action(self, action):
        while self.action != None:
            pass
        self.lock.acquire()
        self.action = action
        #print("Socket Action: ", self.action)
        #print("Update Time: ", int(round(time.time()*1000)))
        self.lock.release()

    def server_handler_excution_time(self, client):
        self.lock.acquire()
        buf = client.recv(102400)
        data = json.loads(buf)
        if data.__contains__("turnaroundTime"):
            self.execution_time.append([data['applicationId'], data['turnaroundTime']])
        self.lock.release()

    def get_execution_time(self):
        # if self.execution_time != {}:
            # print("Execution Time", self.execution_time)
        return self.execution_time


def calculate_test(threadedTCPRequestHandler):
    # SJF.sjf(threadedTCPRequestHandler)
    PAS.agent(threadedTCPRequestHandler)
    # AHP.ahp(threadedTCPRequestHandler)
    # for i in range(10000):
    #     print(threadedTCPRequestHandler.get_execution_time())
    #     time.sleep(10)

def accept_request_order(trh):
    s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s1.bind((HOST, PORT_APPINFO))  # 套接字绑定的IP与端口
    s1.listen(5)  # 开始TCP监听,监听1个请求
    while 1:
        conn, addr = s1.accept()
        t_app_info = threading.Thread(target=ThreadedTCPRequestHandler.server_handler_appinfo, args=[trh, conn])
        t_app_info.start()

def main():
    trh = ThreadedTCPRequestHandler()

    cal_thread = threading.Thread(target=calculate_test, args=[trh])
    cal_thread.start()

    cal_order_thread = threading.Thread(target=accept_request_order, args=[trh])

    cal_order_thread.start()


if __name__ == "__main__":
    main()

