package org.apache.hadoop.yarn.server.resourcemanager.scheduler.agentutil;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.multiqueue.CSQueueInfo;
import org.json.JSONArray;
import org.json.JSONObject;
public class SocketClient {
    private static final Log LOG = LogFactory.getLog(SocketClient.class);

    //要连接的服务端IP地址
    private static final String HOST = "127.0.0.1";

    //要连接的服务端对应的监听端口
    private static final int PORT_REQUEST_ORDER = 8001;
    private static final int PORT_EXECUTED_TIME = 8002;

    public static List<ApplicationAttemptId> sendRequireData(int clusterAvailMemory, int clusterAvailCpu,
                                                             String nodeName, int nodeAvailMemory, int nodeAvailCpu,
                                                             CSQueueInfo appInfos) {
        try {
            //与服务端建立连接
            Socket client = new Socket(HOST, PORT_REQUEST_ORDER);
            client.setSoTimeout(5000);

            //建立连接后就可以往服务端写数据了
            HashMap<String, Object> sendData = new HashMap<String, Object>();
            sendData.put("clusterAvailMemory", clusterAvailMemory);
            sendData.put("clusterAvailCpu", clusterAvailCpu);
//            sendData.put("nodeName", nodeName);
            sendData.put("nodeAvailMemory", nodeAvailMemory);
            sendData.put("nodeAvailCpu", nodeAvailCpu);
            sendData.put("queueAppInfo", appInfos);

            // 构建json对象，并转化为字节流
            JSONObject json = new JSONObject(sendData);
            String jsonString = json.toString();
            byte[] jsonByte = jsonString.getBytes();

//            LOG.info("MyLOG:\t".concat(jsonString));

            // 开始传输数据
            DataOutputStream outputStream = new DataOutputStream(client.getOutputStream());
            outputStream.write(jsonByte);
            outputStream.flush();
            // 关闭传输
            client.shutdownOutput();


            // 以下为接受服务器端发来的数据
            // 如果通信关闭
            if (client.isClosed()) {
                LOG.info("MyLOG:\tClient is Closed = 0");
                return null;
            }

            // 开始读取数据
            DataInputStream inputStream = new DataInputStream(new BufferedInputStream(client.getInputStream()));
            byte[] by = new byte[20480];
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            int nbyte;
            while ((nbyte = inputStream.read(by)) != -1) {
                baos.write(by, 0, nbyte);
            }
            String strInputStream = new String(baos.toByteArray());
            client.close();

            // 结果数据长度为零

            if (strInputStream.length() == 0) {
                LOG.info("MyLOG:\tResult Data String Length = 0");
                return null;
            }

            List<ApplicationAttemptId> result = new ArrayList<>();
            JSONArray order = new JSONArray(strInputStream);
            LOG.info("MyLOG:  Socket Result: " + order);

            for (int i = 0; i < order.length(); i++) {
                JSONObject app = order.getJSONObject(i);

                int id = app.getInt("applicationId");
                long clusterTimestamp = app.getLong("clusterTimestamp");
                int attemptId = app.getInt("attemptId");

                LOG.info("MyLOG:  Result-Id = " + id);

                result.add(ApplicationAttemptId.newInstance(
                        ApplicationId.newInstance(clusterTimestamp, id), attemptId));
            }

            return result;
        } catch (Exception e) {
            LOG.error(printLog(e));
        }
        return null;
    }

    public static void sendExecutedTime(ApplicationAttemptId attemptId, long executedTime){
        sendExecutedTime(attemptId.getApplicationId().getId(), attemptId.getApplicationId().getClusterTimestamp(),
                attemptId.getAttemptId(), executedTime);
    }

    public static void sendExecutedTime(int appId, long clusterTimestamp, int attemptId, long turnaroundTime) {
        try {
            //与服务端建立连接
            Socket client = new Socket(HOST, PORT_EXECUTED_TIME);

            //建立连接后就可以往服务端写数据了
            HashMap<String, Object> sendData = new HashMap<String, Object>();
            sendData.put("applicationId", appId);
            sendData.put("clusterTimestamp", clusterTimestamp);
            sendData.put("attemptId", attemptId);
            sendData.put("turnaroundTime", turnaroundTime);

            JSONObject json = new JSONObject(sendData);
            String jsonString = json.toString();
            byte[] jsonByte = jsonString.getBytes();

            DataOutputStream outputStream = new DataOutputStream(client.getOutputStream());
            outputStream.write(jsonByte);
            outputStream.flush();
            client.shutdownOutput();

            client.close();
        } catch (Exception e) {
            LOG.error(printLog(e));
        }
    }

    public static void main(String[] args){
        CSQueueInfo csQueueInfo = new CSQueueInfo();

        while(true){
            System.out.println("Send Start");
            System.out.println(System.currentTimeMillis());

            sendRequireData(2048,8, "test", 1024, 4, csQueueInfo);
            sendExecutedTime(1, 1000L, 0, 1111L);

            System.out.println("Send End");
            System.out.println(System.currentTimeMillis());

            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static String printLog(Exception e) {
        StackTraceElement[] stackTrace = e.getStackTrace();
        StringBuilder sb = new StringBuilder();
        sb.append(e.fillInStackTrace()).append("\n");

        for (StackTraceElement element : stackTrace) {
            sb.append("\tat ").append(element).append("\n");
        }
        return sb.toString();
    }
}
