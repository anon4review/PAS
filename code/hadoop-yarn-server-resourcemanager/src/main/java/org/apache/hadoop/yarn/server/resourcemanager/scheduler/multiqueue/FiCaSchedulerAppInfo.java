package org.apache.hadoop.yarn.server.resourcemanager.scheduler.multiqueue;

public class FiCaSchedulerAppInfo {
    private String name;
    private Integer applicationId;
    private long clusterTimestamp;
    private Integer attemptId;

    private Integer totalRequestedMemory;
    private Integer totalRequestedVirtualCores;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(Integer applicationId) {
        this.applicationId = applicationId;
    }

    public long getClusterTimestamp() {
        return clusterTimestamp;
    }

    public void setClusterTimestamp(long clusterTimestamp) {
        this.clusterTimestamp = clusterTimestamp;
    }

    public Integer getAttemptId() {
        return attemptId;
    }

    public void setAttemptId(Integer attemptId) {
        this.attemptId = attemptId;
    }

    public Integer getTotalRequestedMemory() {
        return totalRequestedMemory;
    }

    public void setTotalRequestedMemory(Integer totalRequestedMemory) {
        this.totalRequestedMemory = totalRequestedMemory;
    }

    public Integer getTotalRequestedVirtualCores() {
        return totalRequestedVirtualCores;
    }

    public void setTotalRequestedVirtualCores(Integer totalRequestedVirtualCores) {
        this.totalRequestedVirtualCores = totalRequestedVirtualCores;
    }
}
