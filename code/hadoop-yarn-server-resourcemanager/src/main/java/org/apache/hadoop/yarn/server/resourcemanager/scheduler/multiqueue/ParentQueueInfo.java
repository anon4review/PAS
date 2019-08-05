package org.apache.hadoop.yarn.server.resourcemanager.scheduler.multiqueue;

import java.util.ArrayList;
import java.util.List;


public class ParentQueueInfo extends CSQueueInfo {
    protected List<CSQueueInfo> childQueues = new ArrayList<>();   // 儿子队列
    volatile int numApplications;   // 该队列应用的数量

    public List<CSQueueInfo> getChildQueues() {
        return childQueues;
    }

    public void setChildQueues(List<CSQueueInfo> childQueues) {
        this.childQueues = childQueues;
    }

    public int getNumApplications() {
        return numApplications;
    }

    public void setNumApplications(int numApplications) {
        this.numApplications = numApplications;
    }

    public void addChild(CSQueueInfo curQueueInfo) {
        childQueues.add(curQueueInfo);
    }
}
