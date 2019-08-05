package org.apache.hadoop.yarn.server.resourcemanager.scheduler.expr;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;
public class SocketClient {
    private static final Log LOG = LogFactory.getLog(SocketClient.class);

    //要连接的服务端IP地址
//    private static final String HOST = "192.168.0.244";
    private static final String HOST = "127.0.0.1";
    //要连接的服务端对应的监听端口
    private static final int PORT_REQUEST_ORDER = 8001;
    private static final int PORT_EXECUTED_TIME = 8002;

    public static List<AppInfo> sendRequireData(int availMemory, int availCpu, List<AppInfo> waitingAppInfo,
                                                List<AppInfo> runningAppInfo) {
        long clusterTimestamp = waitingAppInfo.get(0).getClusterTimestamp();
        try {
            //与服务端建立连接
            Socket client = new Socket(HOST, PORT_REQUEST_ORDER);

            //建立连接后就可以往服务端写数据了
            HashMap<String, Object> sendData = new HashMap<String, Object>();
            sendData.put("avail_memory", availMemory);
            sendData.put("avail_cpu", availCpu);
            sendData.put("app_info", waitingAppInfo);
            sendData.put("running_app_info", runningAppInfo);

//            LOG.info("MyLOG:  the length of send appInfos = " + appInfos.size());

            JSONObject json = new JSONObject(sendData);
            String jsonString = json.toString();
            byte[] jsonByte = jsonString.getBytes();

            DataOutputStream outputStream = new DataOutputStream(client.getOutputStream());
            outputStream.write(jsonByte);
            outputStream.flush();

            client.shutdownOutput();


            //以下为接受服务器端发来的数据
            List<AppInfo> result = new ArrayList<>();
            if (client.isClosed()) return result;

            DataInputStream inputStream = new DataInputStream(new BufferedInputStream(client.getInputStream()));
            byte[] by = new byte[20480];
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            int nbyte;
            while ((nbyte = inputStream.read(by)) != -1) {
                baos.write(by, 0, nbyte);
            }
            String strInputStream = new String(baos.toByteArray());
            client.close();

            if (strInputStream.length() == 0) return result;

            LOG.info("MyLOG:  Socket Result");
            JSONArray order = new JSONArray(strInputStream);
            for (int i = 0; i < order.length(); i++) {
                JSONObject app = order.getJSONObject(i);
                int id = app.getInt("id");

                LOG.info("MyLOG:  Id  = " + id);

                result.add(new AppInfo(id, 0, 0, clusterTimestamp, 0));
            }

            return result;
        } catch (Exception e) {
            LOG.error(printLog(e));
        }
        return new ArrayList<>();
    }

    public static void sendExecutedTime(int appId, long executedTime){
        try {
            //与服务端建立连接
            Socket client = new Socket(HOST, PORT_EXECUTED_TIME);

            //建立连接后就可以往服务端写数据了
            HashMap<String, Object> sendData = new HashMap<String, Object>();
            sendData.put("app_id", appId);
            sendData.put("execution_time", executedTime);

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
        List<AppInfo> appInfo = new ArrayList<AppInfo>();
        while(true){
            System.out.println("start");
            System.out.println(System.currentTimeMillis());

//            sendRequireData(8,8,appInfo);
            sendExecutedTime(1, 1000);

            System.out.println("end");
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