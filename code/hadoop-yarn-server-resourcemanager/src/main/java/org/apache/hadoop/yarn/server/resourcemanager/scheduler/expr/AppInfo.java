package org.apache.hadoop.yarn.server.resourcemanager.scheduler.expr;

public class AppInfo {
    private int id;
    private int memory;
    private int cpu;
    private long clusterTimestamp;
    private int predictTime;
    private String appName;


    public AppInfo(int id, int memory, int cpu, long clusterTimestamp, int predictTime, String appName) {
        this.id = id;
        this.memory = memory;
        this.cpu = cpu;
        this.clusterTimestamp = clusterTimestamp;
        this.predictTime = predictTime;
        this.appName = appName;
    }

    public AppInfo(int id, int memory, int cpu, long clusterTimestamp, int predictTime) {
        this.id = id;
        this.memory = memory;
        this.cpu = cpu;
        this.clusterTimestamp = clusterTimestamp;
        this.predictTime = predictTime;
    }

    public AppInfo(int id, int memory, int cpu, long clusterTimestamp) {
        this.id = id;
        this.memory = memory;
        this.cpu = cpu;
        this.clusterTimestamp = clusterTimestamp;
        this.predictTime = 0;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getMemory() {
        return memory;
    }

    public void setMemory(int memory) {
        this.memory = memory;
    }

    public int getCpu() {
        return cpu;
    }

    public void setCpu(int cpu) {
        this.cpu = cpu;
    }

    public long getClusterTimestamp() {
        return clusterTimestamp;
    }

    public void setClusterTimestamp(long clusterTimestamp) {
        this.clusterTimestamp = clusterTimestamp;
    }

    public int getPredictTime() {
        return predictTime;
    }

    public void setPredictTime(int predictTime) {
        this.predictTime = predictTime;
    }



    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }
}
