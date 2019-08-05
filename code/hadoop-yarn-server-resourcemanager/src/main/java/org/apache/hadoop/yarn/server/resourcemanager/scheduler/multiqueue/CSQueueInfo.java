package org.apache.hadoop.yarn.server.resourcemanager.scheduler.multiqueue;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.Resources;

public class CSQueueInfo {
//    CSQueueInfo parent;  // 父节点
    String queueName;   // 队列名

//    float capacity;    // 固有占父亲节点的占比
//    float maximumCapacity;   // 可占父亲节点的最大占比
//    float absoluteCapacity;  // 固有占整个集群的占比
//    float absoluteMaxCapacity;  // 可占整个集群的最大占比
    float absoluteUsedCapacity = 0.0f;   // 现在使用资源占整个集群的占比
//    float usedCapacity = 0.0f;   // 现在使用资源占父节点的占比

    volatile int numContainers;

    Integer usedMemory;
    Integer usedVirtualCores;

    Integer minAllocationMemory;
    Integer minAllocationVirtualCores;

    Integer maxAllocationMemory;
    Integer maxAllocationVirtualCores;

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

//    public float getCapacity() {
//        return capacity;
//    }
//
//    public void setCapacity(float capacity) {
//        this.capacity = capacity;
//    }
//
//    public float getMaximumCapacity() {
//        return maximumCapacity;
//    }
//
//    public void setMaximumCapacity(float maximumCapacity) {
//        this.maximumCapacity = maximumCapacity;
//    }
//
//    public float getAbsoluteCapacity() {
//        return absoluteCapacity;
//    }
//
//    public void setAbsoluteCapacity(float absoluteCapacity) {
//        this.absoluteCapacity = absoluteCapacity;
//    }
//
//    public float getAbsoluteMaxCapacity() {
//        return absoluteMaxCapacity;
//    }
//
//    public void setAbsoluteMaxCapacity(float absoluteMaxCapacity) {
//        this.absoluteMaxCapacity = absoluteMaxCapacity;
//    }
//
    public float getAbsoluteUsedCapacity() {
        return absoluteUsedCapacity;
    }

    public void setAbsoluteUsedCapacity(float absoluteUsedCapacity) {
        this.absoluteUsedCapacity = absoluteUsedCapacity;
    }
//
//    public float getUsedCapacity() {
//        return usedCapacity;
//    }
//
//    public void setUsedCapacity(float usedCapacity) {
//        this.usedCapacity = usedCapacity;
//    }

    public int getNumContainers() {
        return numContainers;
    }

    public void setNumContainers(int numContainers) {
        this.numContainers = numContainers;
    }

    public Integer getUsedMemory() {
        return usedMemory;
    }

    public void setUsedMemory(Integer usedMemory) {
        this.usedMemory = usedMemory;
    }

    public Integer getUsedVirtualCores() {
        return usedVirtualCores;
    }

    public void setUsedVirtualCores(Integer usedVirtualCores) {
        this.usedVirtualCores = usedVirtualCores;
    }

    public Integer getMinAllocationMemory() {
        return minAllocationMemory;
    }

    public void setMinAllocationMemory(Integer minAllocationMemory) {
        this.minAllocationMemory = minAllocationMemory;
    }

    public Integer getMinAllocationVirtualCores() {
        return minAllocationVirtualCores;
    }

    public void setMinAllocationVirtualCores(Integer minAllocationVirtualCores) {
        this.minAllocationVirtualCores = minAllocationVirtualCores;
    }

    public Integer getMaxAllocationMemory() {
        return maxAllocationMemory;
    }

    public void setMaxAllocationMemory(Integer maxAllocationMemory) {
        this.maxAllocationMemory = maxAllocationMemory;
    }

    public Integer getMaxAllocationVirtualCores() {
        return maxAllocationVirtualCores;
    }

    public void setMaxAllocationVirtualCores(Integer maxAllocationVirtualCores) {
        this.maxAllocationVirtualCores = maxAllocationVirtualCores;
    }
}
