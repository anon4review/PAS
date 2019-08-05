package org.apache.hadoop.yarn.server.resourcemanager.scheduler.agentutil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;

import java.io.FileWriter;
import java.io.IOException;

public class OutputToFile {
    private static final Log LOG = LogFactory.getLog(OutputToFile.class);
    private static String PATH_PREFIX = "/var/lib/hadoop-yarn/";
    private static String RUNNING_FILE = PATH_PREFIX + "running.csv";
    private static String WAIT_FILE = PATH_PREFIX + "wait.csv";
    private static String TURNAROUND_TIME_FILE = PATH_PREFIX + "turnaround-time.csv";
    private static String RESOURCE_INFO_FILE = PATH_PREFIX + "resource-info.csv";
    private static String ORDER_FILE = PATH_PREFIX + "order.csv";

    private static FileWriter runningWriter;
    private static FileWriter waitWriter;
    private static FileWriter turnaroundTimeWriter;
    private static FileWriter resourceInfoWriter;
    private static FileWriter orderWriter;

    private static Integer count = 0;

    // count, nodeId, avalMemory, avalCpu,
    // AppId, priority, reqSourceName,
    // reqMemory, reqCpu, reqContainer,
    // maxAllocatableContainers Or AllocatedConatiners,cd  result
    private static String RUNNING_INFO = "%d, %s, %d, %d, " +
            "%d, %d, %s, " +
            "%d, %d, %d, " +
            "%d, %d\n";

    // AppId, waitTime
    private static String WAIT_INFO = "%d, %d\n";

    // appId, Type, turnaroundTime, attemptId
    private static final String TURNAROUND_TIME_INFO = "%d, %s, %d, %d\n";

    // timestamp, reservedMB, availableMB, allocatedMB,
    //     reservedVirtualCores, availableVirtualCores, allocatedVirtualCores
    private static final String RESOURCE_INFO = "%d, %d, %d\n";

    // ID, Type
    private static final String ORDER_INFO = "%d, %s\n";

    static {
        try {
            runningWriter = new FileWriter(RUNNING_FILE);
            waitWriter = new FileWriter(WAIT_FILE);
            turnaroundTimeWriter = new FileWriter(TURNAROUND_TIME_FILE);
            resourceInfoWriter = new FileWriter(RESOURCE_INFO_FILE);
            orderWriter = new FileWriter(ORDER_FILE);
        } catch (IOException e) {
            LOG.warn(printLog(e));
        }
    }

    public static void addCount() {
        count++;
    }

    // count, nodeId, avalMemory, avalCpu,
    // AppId, priority, reqSourceName,
    // reqMemory, reqCpu, reqContainer,
    // maxAllocatableContainers Or AllocatedConatiners, result
    public static void outputQueueAppInfo(FiCaSchedulerNode node, FiCaSchedulerApp app,
                                          ResourceRequest request, int maxAllocatableContainer) {
        String content = String.format(RUNNING_INFO, count, node.getNodeID().getHost(),
                node.getAvailableResource().getMemory(), node.getAvailableResource().getVirtualCores(),
                app.getApplicationId().getId(), request.getPriority().getPriority(), request.getResourceName(),
                request.getCapability().getMemory(), request.getCapability().getVirtualCores(), request.getNumContainers(),
                maxAllocatableContainer, 0);
        writeFile(runningWriter, content);
    }

    // count, nodeId, avalMemory, avalCpu,
    // AppId, priority, reqSourceName,
    // reqMemory, reqCpu, reqContainer,
    // maxAllocatableContainers Or AllocatedConatiners, result
    public static void outputOrderInfo(FiCaSchedulerNode node, FiCaSchedulerApp application, Priority priority, int assignedContainers) {
        String content = String.format(RUNNING_INFO, count, node.getNodeID().getHost(),
                node.getAvailableResource().getMemory(), node.getAvailableResource().getVirtualCores(),
                application.getApplicationId().getId(), priority.getPriority(), "unknown",
                -1, -1, -1,
                assignedContainers, 1);
        writeFile(runningWriter, content);
    }

    public static void outputWaitInfo(RMAppImpl rmApp, long runningTime) {
        String content = String.format(WAIT_INFO, rmApp.getApplicationId().getId(), runningTime - rmApp.getSubmitTime());
        writeFile(waitWriter, content);
    }

    public static void writeFile(FileWriter writer, String content) {
        try {
            writer.write(content);
            writer.flush();
        } catch (IOException e) {
            LOG.warn(printLog(e));
        }
    }

    private static String printLog(IOException e) {
        StackTraceElement[] stackTrace = e.getStackTrace();
        StringBuilder sb = new StringBuilder();
        sb.append(e.fillInStackTrace()).append("\n");

        for (StackTraceElement element : stackTrace) {
            sb.append("\tat ").append(element).append("\n");
        }
        return sb.toString();
    }

    public static void outputTurnaroundTime(int appId, long clusterTimestamp, int attemptId, String name, long turnaroundTime) {
        String content = String.format(TURNAROUND_TIME_INFO, appId, name.substring(0, 2), turnaroundTime, attemptId);
        writeFile(turnaroundTimeWriter, content);
    }

    public static void outputResourceInfo(long reservedMB, long availableMB, long allocatedMB,
                                          long reservedVirtualCores, long availableVirtualCores, long allocatedVirtualCores) {
        String content = String.format(RESOURCE_INFO, System.currentTimeMillis(), allocatedMB, allocatedVirtualCores);
        writeFile(resourceInfoWriter, content);
    }

    public static void outputScheduledOrder(int id, String type) {
        String content = String.format(ORDER_INFO, id, type);
        writeFile(orderWriter, content);
    }
}
