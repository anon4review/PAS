## code

There are totally three algorithm in code/algorithm folder, including PAS, AHP and SJF. And we also change the source code of hadoop yarn resource manger. 
To use our code. Firstly, you should export the hadoop-yarn-server-resourcemanager to the jar and replace it on the yarn. Then, you can use our code by running the socket_server.py, because the communication between hadoop and python is via socket.

## data

#### prediction
Here is data of job completion time under different Resource Utilization Ratio (RUR), which including wordcount, sparkpi and SVM.

#### experiment
Here is data of six groups of workloads, including the submission sequence, makespan, turnaround time and RUR.
