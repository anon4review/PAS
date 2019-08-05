package org.apache.hadoop.yarn.server.resourcemanager.scheduler.multiqueue;

import java.util.ArrayList;
import java.util.List;

public class LeafQueueInfo extends CSQueueInfo {
    protected int maxApplications;      // 队列最大应用数

    private int numActiveApplications;
    private int numPendingApplications;

    private int maxActiveApplications; // 最多执行应用数  // Based on absolute max capacity
//    private int maxActiveAppsUsingAbsCap; // 正在执行任务数最大占比 // Based on absolute capacity

//    private int nodeLocalityDelay;

//    private float minimumAllocationFactor;

//    private volatile float absoluteMaxAvailCapacity;   // 占整个集群最大可用资源占比

    List<FiCaSchedulerAppInfo> activeApplications = new ArrayList<>();   // 活动的应用
    // Map<ApplicationAttemptId, FiCaSchedulerApp> applicationAttemptMap =
    //        new HashMap<ApplicationAttemptId, FiCaSchedulerApp>();

    public List<FiCaSchedulerAppInfo> pendingApplications = new ArrayList<>();  // 等待执行的应用

    public int getNumActiveApplications() {
        return numActiveApplications;
    }

    public void setNumActiveApplications(int numActiveApplications) {
        this.numActiveApplications = numActiveApplications;
    }

    public int getNumPendingApplications() {
        return numPendingApplications;
    }

    public void setNumPendingApplications(int numPendingApplications) {
        this.numPendingApplications = numPendingApplications;
    }

    public int getMaxApplications() {
        return maxApplications;
    }

    public void setMaxApplications(int maxApplications) {
        this.maxApplications = maxApplications;
    }


    public int getMaxActiveApplications() {
        return maxActiveApplications;
    }

    public void setMaxActiveApplications(int maxActiveApplications) {
        this.maxActiveApplications = maxActiveApplications;
    }

//    public int getMaxActiveAppsUsingAbsCap() {
//        return maxActiveAppsUsingAbsCap;
//    }
//
//    public void setMaxActiveAppsUsingAbsCap(int maxActiveAppsUsingAbsCap) {
//        this.maxActiveAppsUsingAbsCap = maxActiveAppsUsingAbsCap;
//    }
//
//    public int getNodeLocalityDelay() {
//        return nodeLocalityDelay;
//    }
//
//    public void setNodeLocalityDelay(int nodeLocalityDelay) {
//        this.nodeLocalityDelay = nodeLocalityDelay;
//    }

    public List<FiCaSchedulerAppInfo> getActiveApplications() {
        return activeApplications;
    }

    public void setActiveApplications(List<FiCaSchedulerAppInfo> activeApplications) {
        this.activeApplications = activeApplications;
    }

    public List<FiCaSchedulerAppInfo> getPendingApplications() {
        return pendingApplications;
    }

    public void setPendingApplications(List<FiCaSchedulerAppInfo> pendingApplications) {
        this.pendingApplications = pendingApplications;
    }

//    public float getMinimumAllocationFactor() {
//        return minimumAllocationFactor;
//    }
//
//    public void setMinimumAllocationFactor(float minimumAllocationFactor) {
//        this.minimumAllocationFactor = minimumAllocationFactor;
//    }
//
//    public float getAbsoluteMaxAvailCapacity() {
//        return absoluteMaxAvailCapacity;
//    }
//
//    public void setAbsoluteMaxAvailCapacity(float absoluteMaxAvailCapacity) {
//        this.absoluteMaxAvailCapacity = absoluteMaxAvailCapacity;
//    }

    public void addActiveAppInfo(FiCaSchedulerAppInfo appInfo) {
        activeApplications.add(appInfo);
    }

    public void addPendingAppInfo(FiCaSchedulerAppInfo appInfo) {
        pendingApplications.add(appInfo);
    }

}