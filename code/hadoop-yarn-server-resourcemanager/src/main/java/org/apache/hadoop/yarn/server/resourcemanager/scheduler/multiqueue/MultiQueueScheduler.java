/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.multiqueue;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppRejectedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeResourceUpdateEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.PreemptableResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueNotFoundException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.agentutil.SocketClient;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.*;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.QueueMapping;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.QueueMapping.MappingType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerRescheduledEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeResourceUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.Lock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;

import static java.lang.Thread.sleep;

@LimitedPrivate("yarn")
@Evolving
@SuppressWarnings("unchecked")
public class MultiQueueScheduler extends
        AbstractYarnScheduler<FiCaSchedulerApp, FiCaSchedulerNode> implements
        PreemptableResourceScheduler, CapacitySchedulerContext, Configurable {

    private static final Log LOG = LogFactory.getLog(MultiQueueScheduler.class);
    private YarnAuthorizationProvider authorizer;

    private CSQueue root;
    // timeout to join when we stop this service
    protected final long THREAD_JOIN_TIMEOUT_MS = 1000;

    static final Comparator<CSQueue> queueComparator = new Comparator<CSQueue>() {
        @Override
        public int compare(CSQueue q1, CSQueue q2) {
            if (q1.getUsedCapacity() < q2.getUsedCapacity()) {
                return -1;
            } else if (q1.getUsedCapacity() > q2.getUsedCapacity()) {
                return 1;
            }

            return q1.getQueuePath().compareTo(q2.getQueuePath());
        }
    };

    static final Comparator<FiCaSchedulerApp> applicationComparator =
            new Comparator<FiCaSchedulerApp>() {
                @Override
                public int compare(FiCaSchedulerApp a1, FiCaSchedulerApp a2) {
                    long compare = a1.getAddActiveSetTimestamp() - a2.getAddActiveSetTimestamp();
                    if (compare == 0) {
                        return a1.getApplicationId().compareTo(a2.getApplicationId());
                    }
                    if(compare>0){
                        return 1;
                    }else if(compare<0){
                        return -1;
                    }
                    return 0;
                }
            };

    @Override
    public void setConf(Configuration conf) {
        yarnConf = conf;
    }

    private void validateConf(Configuration conf) {
        // validate scheduler memory allocation setting
        int minMem = conf.getInt(
                YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
                YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
        int maxMem = conf.getInt(
                YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
                YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);

        if (minMem <= 0 || minMem > maxMem) {
            throw new YarnRuntimeException("Invalid resource scheduler memory"
                    + " allocation configuration"
                    + ", " + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB
                    + "=" + minMem
                    + ", " + YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB
                    + "=" + maxMem + ", min and max should be greater than 0"
                    + ", max should be no smaller than min.");
        }

        // validate scheduler vcores allocation setting
        int minVcores = conf.getInt(
                YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
                YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
        int maxVcores = conf.getInt(
                YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
                YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);

        if (minVcores <= 0 || minVcores > maxVcores) {
            throw new YarnRuntimeException("Invalid resource scheduler vcores"
                    + " allocation configuration"
                    + ", " + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES
                    + "=" + minVcores
                    + ", " + YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES
                    + "=" + maxVcores + ", min and max should be greater than 0"
                    + ", max should be no smaller than min.");
        }
    }

    @Override
    public Configuration getConf() {
        return yarnConf;
    }

    private CapacitySchedulerConfiguration conf;
    private Configuration yarnConf;

    private Map<String, CSQueue> queues = new ConcurrentHashMap<String, CSQueue>();

    private ResourceCalculator calculator;
    private boolean usePortForNodeName;

    private boolean scheduleAsynchronously;
    private AsyncScheduleThread asyncSchedulerThread;
    private RMNodeLabelsManager labelManager;

    /**
     * EXPERT
     */
    private long asyncScheduleInterval;
    private static final String ASYNC_SCHEDULER_INTERVAL =
            CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_PREFIX
                    + ".scheduling-interval-ms";
    private static final long DEFAULT_ASYNC_SCHEDULER_INTERVAL = 5;

    private boolean overrideWithQueueMappings = false;
    private List<QueueMapping> mappings = null;
    private Groups groups;

    @VisibleForTesting
    public synchronized String getMappedQueueForTest(String user)
            throws IOException {
        return getMappedQueue(user);
    }

    public MultiQueueScheduler() {
        super(MultiQueueScheduler.class.getName());
    }

    @Override
    public QueueMetrics getRootQueueMetrics() {
        return root.getMetrics();
    }

    public CSQueue getRootQueue() {
        return root;
    }

    @Override
    public CapacitySchedulerConfiguration getConfiguration() {
        return conf;
    }

    @Override
    public synchronized RMContainerTokenSecretManager
    getContainerTokenSecretManager() {
        return this.rmContext.getContainerTokenSecretManager();
    }

    @Override
    public Comparator<FiCaSchedulerApp> getApplicationComparator() {
        return applicationComparator;
    }

    @Override
    public ResourceCalculator getResourceCalculator() {
        return calculator;
    }

    @Override
    public Comparator<CSQueue> getQueueComparator() {
        return queueComparator;
    }

    @Override
    public int getNumClusterNodes() {
        return nodeTracker.nodeCount();
    }

    @Override
    public synchronized RMContext getRMContext() {
        return this.rmContext;
    }

    @Override
    public synchronized void setRMContext(RMContext rmContext) {
        this.rmContext = rmContext;
    }

    private synchronized void initScheduler(Configuration configuration) throws
            IOException {
        this.conf = loadCapacitySchedulerConfiguration(configuration);
        validateConf(this.conf);
        this.minimumAllocation = this.conf.getMinimumAllocation();
        initMaximumResourceCapability(this.conf.getMaximumAllocation());
        this.calculator = this.conf.getResourceCalculator();
        this.usePortForNodeName = this.conf.getUsePortForNodeName();
        this.applications =
                new ConcurrentHashMap<ApplicationId,
                        SchedulerApplication<FiCaSchedulerApp>>();
        this.labelManager = rmContext.getNodeLabelManager();
        authorizer = YarnAuthorizationProvider.getInstance(yarnConf);
        initializeQueues(this.conf);

        scheduleAsynchronously = this.conf.getScheduleAynschronously();
        asyncScheduleInterval =
                this.conf.getLong(ASYNC_SCHEDULER_INTERVAL,
                        DEFAULT_ASYNC_SCHEDULER_INTERVAL);
        if (scheduleAsynchronously) {
            asyncSchedulerThread = new AsyncScheduleThread(this);
        }

        LOG.info("Initialized CapacityScheduler with " +
                "calculator=" + getResourceCalculator().getClass() + ", " +
                "minimumAllocation=<" + getMinimumResourceCapability() + ">, " +
                "maximumAllocation=<" + getMaximumResourceCapability() + ">, " +
                "asynchronousScheduling=" + scheduleAsynchronously + ", " +
                "asyncScheduleInterval=" + asyncScheduleInterval + "ms");
    }

    private synchronized void startSchedulerThreads() {
        if (scheduleAsynchronously) {
            Preconditions.checkNotNull(asyncSchedulerThread,
                    "asyncSchedulerThread is null");
            asyncSchedulerThread.start();
        }
    }

    @Override
    public void serviceInit(Configuration conf) throws Exception {
        Configuration configuration = new Configuration(conf);
        super.serviceInit(conf);
        initScheduler(configuration);
    }

    @Override
    public void serviceStart() throws Exception {
        startSchedulerThreads();
        super.serviceStart();
    }

    @Override
    public void serviceStop() throws Exception {
        synchronized (this) {
            if (scheduleAsynchronously && asyncSchedulerThread != null) {
                asyncSchedulerThread.interrupt();
                asyncSchedulerThread.join(THREAD_JOIN_TIMEOUT_MS);
            }
        }
        super.serviceStop();
    }

    @Override
    public synchronized void
    reinitialize(Configuration conf, RMContext rmContext) throws IOException {
        Configuration configuration = new Configuration(conf);
        CapacitySchedulerConfiguration oldConf = this.conf;
        this.conf = loadCapacitySchedulerConfiguration(configuration);
        validateConf(this.conf);
        try {
            LOG.info("Re-initializing queues...");
            reinitializeQueues(this.conf);
        } catch (Throwable t) {
            this.conf = oldConf;
            throw new IOException("Failed to re-init queues", t);
        }
    }

    long getAsyncScheduleInterval() {
        return asyncScheduleInterval;
    }

    private final static Random random = new Random(System.currentTimeMillis());

    /**
     * Schedule on all nodes by starting at a random point.
     * @param cs
     */
    static void schedule(MultiQueueScheduler cs) {
        // First randomize the start point
        int current = 0;
        Collection<FiCaSchedulerNode> nodes = cs.nodeTracker.getAllNodes();
        int start = random.nextInt(nodes.size());
        for (FiCaSchedulerNode node : nodes) {
            if (current++ >= start) {
                cs.allocateContainersToNode(node);
            }
        }
        // Now, just get everyone to be safe
        for (FiCaSchedulerNode node : nodes) {
            cs.allocateContainersToNode(node);
        }
        try {
            sleep(cs.getAsyncScheduleInterval());
        } catch (InterruptedException e) {}
    }



    static class AsyncScheduleThread extends Thread {

        private final MultiQueueScheduler cs;
        private AtomicBoolean runSchedules = new AtomicBoolean(false);

        public AsyncScheduleThread(MultiQueueScheduler cs) {
            this.cs = cs;
            setDaemon(true);
        }

        @Override
        public void run() {
            while (true) {
                if (!runSchedules.get()) {
                    try {
                        sleep(100);
                    } catch (InterruptedException ie) {}
                } else {
                    schedule(cs);
                }
            }
        }

        public void beginSchedule() {
            runSchedules.set(true);
        }

        public void suspendSchedule() {
            runSchedules.set(false);
        }

    }

    @Private
    public static final String ROOT_QUEUE =
            CapacitySchedulerConfiguration.PREFIX + CapacitySchedulerConfiguration.ROOT;

    static class QueueHook {
        public CSQueue hook(CSQueue queue) {
            return queue;
        }
    }
    private static final QueueHook noop = new QueueHook();

    private void initializeQueueMappings() throws IOException {
        overrideWithQueueMappings = conf.getOverrideWithQueueMappings();
        LOG.info("Initialized queue mappings, override: "
                + overrideWithQueueMappings);
        // Get new user/group mappings
        List<QueueMapping> newMappings = conf.getQueueMappings();
        //check if mappings refer to valid queues
        for (QueueMapping mapping : newMappings) {
            if (!mapping.queue.equals(CURRENT_USER_MAPPING) &&
                    !mapping.queue.equals(PRIMARY_GROUP_MAPPING)) {
                CSQueue queue = queues.get(mapping.queue);
                if (queue == null || !(queue instanceof LeafQueue)) {
                    throw new IOException(
                            "mapping contains invalid or non-leaf queue " + mapping.queue);
                }
            }
        }
        //apply the new mappings since they are valid
        mappings = newMappings;
        // initialize groups if mappings are present
        if (mappings.size() > 0) {
            groups = new Groups(conf);
        }
    }

    @Lock(CapacityScheduler.class)
    private void initializeQueues(CapacitySchedulerConfiguration conf)
            throws IOException {

        root =
                parseQueue(this, conf, null, CapacitySchedulerConfiguration.ROOT,
                        queues, queues, noop);
        labelManager.reinitializeQueueLabels(getQueueToLabels());
        LOG.info("Initialized root queue " + root);
        initializeQueueMappings();
        setQueueAcls(authorizer, queues);
    }

    @Lock(CapacityScheduler.class)
    private void reinitializeQueues(CapacitySchedulerConfiguration conf)
            throws IOException {
        // Parse new queues
        Map<String, CSQueue> newQueues = new HashMap<String, CSQueue>();
        CSQueue newRoot =
                parseQueue(this, conf, null, CapacitySchedulerConfiguration.ROOT,
                        newQueues, queues, noop);

        // Ensure all existing queues are still present
        validateExistingQueues(queues, newQueues);

        // Add new queues
        addNewQueues(queues, newQueues);

        // Re-configure queues
        root.reinitialize(newRoot, getClusterResource());
        initializeQueueMappings();

//        LOG.info("MyLOG:\t reinitializeQueues");

        // Re-calculate headroom for active applications
        root.updateClusterResource(getClusterResource());

        labelManager.reinitializeQueueLabels(getQueueToLabels());
        setQueueAcls(authorizer, queues);
    }

    @VisibleForTesting
    public static void setQueueAcls(YarnAuthorizationProvider authorizer,
                                    Map<String, CSQueue> queues) throws IOException {
        for (CSQueue queue : queues.values()) {
            AbstractCSQueue csQueue = (AbstractCSQueue) queue;
            authorizer.setPermission(csQueue.getPrivilegedEntity(),
                    csQueue.getACLs(), UserGroupInformation.getCurrentUser());
        }
    }

    private Map<String, Set<String>> getQueueToLabels() {
        Map<String, Set<String>> queueToLabels = new HashMap<String, Set<String>>();
        for (CSQueue queue : queues.values()) {
            queueToLabels.put(queue.getQueueName(), queue.getAccessibleNodeLabels());
        }
        return queueToLabels;
    }

    /**
     * Ensure all existing queues are present. Queues cannot be deleted
     * @param queues existing queues
     * @param newQueues new queues
     */
    @Lock(MultiQueueScheduler.class)
    private void validateExistingQueues(
            Map<String, CSQueue> queues, Map<String, CSQueue> newQueues)
            throws IOException {
        // check that all static queues are included in the newQueues list
        for (Map.Entry<String, CSQueue> e : queues.entrySet()) {
            if (!(e.getValue() instanceof ReservationQueue)) {
                if (!newQueues.containsKey(e.getKey())) {
                    throw new IOException(e.getKey() + " cannot be found during refresh!");
                }
            }
        }
    }

    /**
     * Add the new queues (only) to our list of queues...
     * ... be careful, do not overwrite existing queues.
     * @param queues
     * @param newQueues
     */
    @Lock(MultiQueueScheduler.class)
    private void addNewQueues(
            Map<String, CSQueue> queues, Map<String, CSQueue> newQueues)
    {
        for (Map.Entry<String, CSQueue> e : newQueues.entrySet()) {
            String queueName = e.getKey();
            CSQueue queue = e.getValue();
            if (!queues.containsKey(queueName)) {
                queues.put(queueName, queue);
            }
        }
    }

    @Lock(CapacityScheduler.class)
    static CSQueue parseQueue(
            CapacitySchedulerContext csContext,
            CapacitySchedulerConfiguration conf,
            CSQueue parent, String queueName, Map<String, CSQueue> queues,
            Map<String, CSQueue> oldQueues,
            QueueHook hook) throws IOException {
        CSQueue queue;
        String fullQueueName =
                (parent == null) ? queueName
                        : (parent.getQueuePath() + "." + queueName);
        String[] childQueueNames =
                conf.getQueues(fullQueueName);
        boolean isReservableQueue = conf.isReservable(fullQueueName);
        if (childQueueNames == null || childQueueNames.length == 0) {
            if (null == parent) {
                throw new IllegalStateException(
                        "Queue configuration missing child queue names for " + queueName);
            }
            // Check if the queue will be dynamically managed by the Reservation
            // system
            if (isReservableQueue) {
                queue =
                        new PlanQueue(csContext, queueName, parent,
                                oldQueues.get(queueName));
            } else {
                queue =
                        new LeafQueue(csContext, queueName, parent,
                                oldQueues.get(queueName));

                // Used only for unit tests
                queue = hook.hook(queue);
            }
        } else {
            if (isReservableQueue) {
                throw new IllegalStateException(
                        "Only Leaf Queues can be reservable for " + queueName);
            }
            ParentQueue parentQueue =
                    new ParentQueue(csContext, queueName, parent, oldQueues.get(queueName));

            // Used only for unit tests
            queue = hook.hook(parentQueue);

            List<CSQueue> childQueues = new ArrayList<CSQueue>();
            for (String childQueueName : childQueueNames) {
                CSQueue childQueue =
                        parseQueue(csContext, conf, queue, childQueueName,
                                queues, oldQueues, hook);
                childQueues.add(childQueue);
            }
            parentQueue.setChildQueues(childQueues);
        }

        if(queue instanceof LeafQueue == true && queues.containsKey(queueName)
                && queues.get(queueName) instanceof LeafQueue == true) {
            throw new IOException("Two leaf queues were named " + queueName
                    + ". Leaf queue names must be distinct");
        }
        queues.put(queueName, queue);

        LOG.info("Initialized queue: " + queue);
        return queue;
    }

    public synchronized CSQueue getQueue(String queueName) {
        if (queueName == null) {
            return null;
        }
        return queues.get(queueName);
    }

    private static final String CURRENT_USER_MAPPING = "%user";

    private static final String PRIMARY_GROUP_MAPPING = "%primary_group";

    private String getMappedQueue(String user) throws IOException {
        for (QueueMapping mapping : mappings) {
            if (mapping.type == MappingType.USER) {
                if (mapping.source.equals(CURRENT_USER_MAPPING)) {
                    if (mapping.queue.equals(CURRENT_USER_MAPPING)) {
                        return user;
                    }
                    else if (mapping.queue.equals(PRIMARY_GROUP_MAPPING)) {
                        return groups.getGroups(user).get(0);
                    }
                    else {
                        return mapping.queue;
                    }
                }
                if (user.equals(mapping.source)) {
                    return mapping.queue;
                }
            }
            if (mapping.type == MappingType.GROUP) {
                for (String userGroups : groups.getGroups(user)) {
                    if (userGroups.equals(mapping.source)) {
                        return mapping.queue;
                    }
                }
            }
        }
        return null;
    }

    private synchronized void addApplication(ApplicationId applicationId,
                                             String queueName, String user, boolean isAppRecovering) {
        if (mappings != null && mappings.size() > 0) {
            try {
                String mappedQueue = getMappedQueue(user);
                if (mappedQueue != null) {
                    // We have a mapping, should we use it?
                    if (queueName.equals(YarnConfiguration.DEFAULT_QUEUE_NAME)
                            || overrideWithQueueMappings) {
                        LOG.info("Application " + applicationId + " user " + user
                                + " mapping [" + queueName + "] to [" + mappedQueue
                                + "] override " + overrideWithQueueMappings);
                        queueName = mappedQueue;
                        RMApp rmApp = rmContext.getRMApps().get(applicationId);
                        rmApp.setQueue(queueName);
                    }
                }
            } catch (IOException ioex) {
                String message = "Failed to submit application " + applicationId +
                        " submitted by user " + user + " reason: " + ioex.getMessage();
                this.rmContext.getDispatcher().getEventHandler()
                        .handle(new RMAppRejectedEvent(applicationId, message));
                return;
            }
        }

        // sanity checks.
        CSQueue queue = getQueue(queueName);
        if (queue == null) {
            //During a restart, this indicates a queue was removed, which is
            //not presently supported
            if (isAppRecovering) {
                String queueErrorMsg = "Queue named " + queueName
                        + " missing during application recovery."
                        + " Queue removal during recovery is not presently supported by the"
                        + " capacity scheduler, please restart with all queues configured"
                        + " which were present before shutdown/restart.";
                LOG.fatal(queueErrorMsg);
                throw new QueueNotFoundException(queueErrorMsg);
            }
            String message = "Application " + applicationId +
                    " submitted by user " + user + " to unknown queue: " + queueName;
            this.rmContext.getDispatcher().getEventHandler()
                    .handle(new RMAppRejectedEvent(applicationId, message));
            return;
        }
        if (!(queue instanceof LeafQueue)) {
            String message = "Application " + applicationId +
                    " submitted by user " + user + " to non-leaf queue: " + queueName;
            this.rmContext.getDispatcher().getEventHandler()
                    .handle(new RMAppRejectedEvent(applicationId, message));
            return;
        }
        // Submit to the queue
        try {
            queue.submitApplication(applicationId, user, queueName);
        } catch (AccessControlException ace) {
            // Ignore the exception for recovered app as the app was previously accepted
            if (!isAppRecovering) {
                LOG.info("Failed to submit application " + applicationId + " to queue "
                        + queueName + " from user " + user, ace);
                this.rmContext.getDispatcher().getEventHandler()
                        .handle(new RMAppRejectedEvent(applicationId, ace.toString()));
                return;
            }
        }
        // update the metrics
        queue.getMetrics().submitApp(user);
        SchedulerApplication<FiCaSchedulerApp> application =
                new SchedulerApplication<FiCaSchedulerApp>(queue, user);
        applications.put(applicationId, application);
        LOG.info("Accepted application " + applicationId + " from user: " + user
                + ", in queue: " + queueName);
        if (isAppRecovering) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(applicationId + " is recovering. Skip notifying APP_ACCEPTED");
            }
        } else {
            rmContext.getDispatcher().getEventHandler()
                    .handle(new RMAppEvent(applicationId, RMAppEventType.APP_ACCEPTED));
        }
    }

    private synchronized void addApplicationAttempt(
            ApplicationAttemptId applicationAttemptId,
            boolean transferStateFromPreviousAttempt,
            boolean isAttemptRecovering) {
        SchedulerApplication<FiCaSchedulerApp> application =
                applications.get(applicationAttemptId.getApplicationId());
        CSQueue queue = (CSQueue) application.getQueue();

        FiCaSchedulerApp attempt =
                new FiCaSchedulerApp(applicationAttemptId, application.getUser(),
                        queue, queue.getActiveUsersManager(), rmContext);
        if (transferStateFromPreviousAttempt) {
            attempt.transferStateFromPreviousAttempt(application
                    .getCurrentAppAttempt());
        }
        application.setCurrentAppAttempt(attempt);

        queue.submitApplicationAttempt(attempt, application.getUser());
        LOG.info("Added Application Attempt " + applicationAttemptId
                + " to scheduler from user " + application.getUser() + " in queue "
                + queue.getQueueName());
        if (isAttemptRecovering) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(applicationAttemptId
                        + " is recovering. Skipping notifying ATTEMPT_ADDED");
            }
        } else {
            rmContext.getDispatcher().getEventHandler().handle(
                    new RMAppAttemptEvent(applicationAttemptId,
                            RMAppAttemptEventType.ATTEMPT_ADDED));
        }
    }

    private synchronized void doneApplication(ApplicationId applicationId,
                                              RMAppState finalState) {
        SchedulerApplication<FiCaSchedulerApp> application =
                applications.get(applicationId);
        if (application == null){
            // The AppRemovedSchedulerEvent maybe sent on recovery for completed apps,
            // ignore it.
            LOG.warn("Couldn't find application " + applicationId);
            return;
        }
        CSQueue queue = (CSQueue) application.getQueue();
        if (!(queue instanceof LeafQueue)) {
            LOG.error("Cannot finish application " + "from non-leaf queue: "
                    + queue.getQueueName());
        } else {
            queue.finishApplication(applicationId, application.getUser());
        }
        application.stop(finalState);
        applications.remove(applicationId);
    }

    private synchronized void doneApplicationAttempt(
            ApplicationAttemptId applicationAttemptId,
            RMAppAttemptState rmAppAttemptFinalState, boolean keepContainers) {
        LOG.info("Application Attempt " + applicationAttemptId + " is done." +
                " finalState=" + rmAppAttemptFinalState);

        FiCaSchedulerApp attempt = getApplicationAttempt(applicationAttemptId);
        SchedulerApplication<FiCaSchedulerApp> application =
                applications.get(applicationAttemptId.getApplicationId());

        if (application == null || attempt == null) {
            LOG.info("Unknown application " + applicationAttemptId + " has completed!");
            return;
        }

        // Release all the allocated, acquired, running containers
        for (RMContainer rmContainer : attempt.getLiveContainers()) {
            if (keepContainers
                    && rmContainer.getState().equals(RMContainerState.RUNNING)) {
                // do not kill the running container in the case of work-preserving AM
                // restart.
                LOG.info("Skip killing " + rmContainer.getContainerId());
                continue;
            }
            completedContainer(
                    rmContainer,
                    SchedulerUtils.createAbnormalContainerStatus(
                            rmContainer.getContainerId(), SchedulerUtils.COMPLETED_APPLICATION),
                    RMContainerEventType.KILL);
        }

        // Release all reserved containers
        for (RMContainer rmContainer : attempt.getReservedContainers()) {
            completedContainer(
                    rmContainer,
                    SchedulerUtils.createAbnormalContainerStatus(
                            rmContainer.getContainerId(), "Application Complete"),
                    RMContainerEventType.KILL);
        }

        // Clean up pending requests, metrics etc.
        attempt.stop(rmAppAttemptFinalState);

        // Inform the queue
        String queueName = attempt.getQueue().getQueueName();
        CSQueue queue = queues.get(queueName);
        if (!(queue instanceof LeafQueue)) {
            LOG.error("Cannot finish application " + "from non-leaf queue: "
                    + queueName);
        } else {
            queue.finishApplicationAttempt(attempt, queue.getQueueName());
        }
    }

    @Override
    @Lock(Lock.NoLock.class)
    public Allocation allocate(ApplicationAttemptId applicationAttemptId,
                               List<ResourceRequest> ask, List<ContainerId> release,
                               List<String> blacklistAdditions, List<String> blacklistRemovals) {

        FiCaSchedulerApp application = getApplicationAttempt(applicationAttemptId);
        if (application == null) {
            LOG.info("Calling allocate on removed " +
                    "or non existant application " + applicationAttemptId);
            return EMPTY_ALLOCATION;
        }

        // Sanity check
        SchedulerUtils.normalizeRequests(
                ask, getResourceCalculator(), getClusterResource(),
                getMinimumResourceCapability(), getMaximumResourceCapability());

        // Release containers
        releaseContainers(release, application);

        synchronized (application) {

            // make sure we aren't stopping/removing the application
            // when the allocate comes in
            if (application.isStopped()) {
                LOG.info("Calling allocate on a stopped " +
                        "application " + applicationAttemptId);
                return EMPTY_ALLOCATION;
            }

            if (!ask.isEmpty()) {

                if(LOG.isDebugEnabled()) {
                    LOG.debug("allocate: pre-update" +
                            " applicationAttemptId=" + applicationAttemptId +
                            " application=" + application);
                }
                application.showRequests();

                // Update application requests
                application.updateResourceRequests(ask);

                LOG.debug("allocate: post-update");
                application.showRequests();
            }

            if(LOG.isDebugEnabled()) {
                LOG.debug("allocate:" +
                        " applicationAttemptId=" + applicationAttemptId +
                        " #ask=" + ask.size());
            }

            if (application.isWaitingForAMContainer(application.getApplicationId())) {
                // Allocate is for AM and update AM blacklist for this
                application.updateAMBlacklist(
                        blacklistAdditions, blacklistRemovals);
            } else {
                application.updateBlacklist(blacklistAdditions, blacklistRemovals);
            }

            return application.getAllocation(getResourceCalculator(),
                    getClusterResource(), getMinimumResourceCapability());
        }
    }

    @Override
    @Lock(Lock.NoLock.class)
    public QueueInfo getQueueInfo(String queueName,
                                  boolean includeChildQueues, boolean recursive)
            throws IOException {
        CSQueue queue = null;

        synchronized (this) {
            queue = this.queues.get(queueName);
        }

        if (queue == null) {
            throw new IOException("Unknown queue: " + queueName);
        }
        return queue.getQueueInfo(includeChildQueues, recursive);
    }

    @Override
    @Lock(Lock.NoLock.class)
    public List<QueueUserACLInfo> getQueueUserAclInfo() {
        UserGroupInformation user = null;
        try {
            user = UserGroupInformation.getCurrentUser();
        } catch (IOException ioe) {
            // should never happen
            return new ArrayList<QueueUserACLInfo>();
        }

        return root.getQueueUserAclInfo(user);
    }

    private synchronized void nodeUpdate(RMNode nm) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("nodeUpdate: " + nm +
                    " clusterResources: " + getClusterResource());
        }

        FiCaSchedulerNode node = getNode(nm.getNodeID());

        List<UpdatedContainerInfo> containerInfoList = nm.pullContainerUpdates();
        List<ContainerStatus> newlyLaunchedContainers = new ArrayList<ContainerStatus>();
        List<ContainerStatus> completedContainers = new ArrayList<ContainerStatus>();
        for(UpdatedContainerInfo containerInfo : containerInfoList) {
            newlyLaunchedContainers.addAll(containerInfo.getNewlyLaunchedContainers());
            completedContainers.addAll(containerInfo.getCompletedContainers());
        }

        // Processing the newly launched containers
        for (ContainerStatus launchedContainer : newlyLaunchedContainers) {
            containerLaunchedOnNode(launchedContainer.getContainerId(), node);
        }

        // Process completed containers
        for (ContainerStatus completedContainer : completedContainers) {
            ContainerId containerId = completedContainer.getContainerId();
            LOG.debug("Container FINISHED: " + containerId);
            completedContainer(getRMContainer(containerId),
                    completedContainer, RMContainerEventType.FINISHED);
        }

        // If the node is decommissioning, send an update to have the total
        // resource equal to the used resource, so no available resource to
        // schedule.
        // TODO: Fix possible race-condition when request comes in before
        // update is propagated
        if (nm.getState() == NodeState.DECOMMISSIONING) {
            this.rmContext
                    .getDispatcher()
                    .getEventHandler()
                    .handle(
                            new RMNodeResourceUpdateEvent(nm.getNodeID(), ResourceOption
                                    .newInstance(getSchedulerNode(nm.getNodeID())
                                            .getUsedResource(), 0)));
        }
        // Now node data structures are upto date and ready for scheduling.
        if(LOG.isDebugEnabled()) {
            LOG.debug("Node being looked for scheduling " + nm
                    + " availableResource: " + node.getAvailableResource());
        }
    }

    /**
     * Process resource update on a node.
     */
    private synchronized void updateNodeAndQueueResource(RMNode nm,
                                                         ResourceOption resourceOption) {
//        LOG.info(("MyLOG:\t Node And Queue Resource Update!!!!!~~~!!~!~!~\n"
//                .concat("New Resource "))
//                .concat(resourceOption.getResource().toString()));

//        LOG.info("MyLOG:\t updateNodeAndQueueResource");

        updateNodeResource(nm, resourceOption);
        Resource clusterResource = getClusterResource();
        root.updateClusterResource(clusterResource);
    }

    private synchronized void allocateContainersToNode(FiCaSchedulerNode node) {
        if (rmContext.isWorkPreservingRecoveryEnabled()
                && !rmContext.isSchedulerReadyForAllocatingContainers()) {
            return;
        }

        // Assign new containers...
        // 1. Check for reserved applications
        // 2. Schedule if there are no reservations

        RMContainer reservedContainer = node.getReservedContainer();
        if (reservedContainer != null) {
            FiCaSchedulerApp reservedApplication =
                    getCurrentAttemptForContainer(reservedContainer.getContainerId());

            // Try to fulfill the reservation
            LOG.info("Trying to fulfill reservation for application " +
                    reservedApplication.getApplicationId() + " on node: " +
                    node.getNodeID());

            LeafQueue queue = ((LeafQueue)reservedApplication.getQueue());
            CSAssignment assignment = queue.assignContainers(getClusterResource(),
                    node, false);

            RMContainer excessReservation = assignment.getExcessReservation();
            if (excessReservation != null) {
                Container container = excessReservation.getContainer();
                queue.completedContainer(
                        getClusterResource(), assignment.getApplication(), node,
                        excessReservation,
                        SchedulerUtils.createAbnormalContainerStatus(
                                container.getId(),
                                SchedulerUtils.UNRESERVED_CONTAINER),
                        RMContainerEventType.RELEASED, null, true);
            }

        }

        // Try to schedule more if there are no reservations to fulfill
        if (node.getReservedContainer() == null) {
            if (calculator.computeAvailableContainers(node.getAvailableResource(),
                    minimumAllocation) > 0) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Trying to schedule on node: " + node.getNodeName() +
                            ", available: " + node.getAvailableResource());
                }

                /**
                 * TODO 插入自定义策略，替换原本的分配
                 * root.assignContainers(getClusterResource(), node, false);
                 */
                assignContainers(node);
            }
        } else {
            LOG.info("Skipping scheduling since node " + node.getNodeID() +
                    " is reserved by application " +
                    node.getReservedContainer().getContainerId().getApplicationAttemptId()
            );
        }

    }

    /**
     * 自定义策略 Agent
     */
    private int numPendingApp = 0;
    private List<LeafQueue> leafQueues = null;
    private long lastPreAllocateTime = System.currentTimeMillis();
    private Thread updateThread = new Thread(new Runnable() {
        @Override
        public void run() {
            CSQueue root = MultiQueueScheduler.this.root;
            // 解决队列超100问题
            while (true) {
                root.updateClusterResource(getClusterResource());

                try {
                    sleep(1000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    });
    private AtomicBoolean isFirst = new AtomicBoolean(true);
    private synchronized void assignContainers(FiCaSchedulerNode node) {
//        if (isFirst.compareAndSet(true, false)) {
//            try {
//                updateThread.start();
//            } catch (Exception e) {
//                LOG.info("MyLOG:\t Thread Start Error "+e);
//            }
//        }

        long startPreAllocate,
                startGetQueueInfo,
                startSendRequest,
                startPostAllocate;

        if (leafQueues == null) {
            leafQueues = new ArrayList<>();
            for (Map.Entry<String, CSQueue> queueEntry: this.queues.entrySet()) {
                CSQueue queue = queueEntry.getValue();
                if (queue instanceof LeafQueue) {
                    leafQueues.add((LeafQueue) queue);
                }
            }
        }

        // 0 首先，执行之前的所给策略
        startPreAllocate = System.nanoTime() / 1000;
        boolean hasApp = false;
        for (LeafQueue queue: leafQueues) {
            if (queue.getPendingApplications().size() > 0) {
                hasApp = true;
            }
            if (queue.getActiveApplications().size() == 0)
                continue;

            Resource clusterResource = getClusterResource();
            CSAssignment assignedAssignment =
                    queue.assignContainers(clusterResource, node, false);
            if (Resources.greaterThan(getResourceCalculator(), getClusterResource(),
                    assignedAssignment.getResource(), Resources.none())) {
                root.updateClusterResource(getClusterResource());
//                LOG.info("MyLOG:\t Update Cluster Resource(us): ");
            }
        }
        startGetQueueInfo = System.nanoTime() / 1000;
        if (startGetQueueInfo - startPreAllocate >= 3000L) {
            LOG.info("MyLOG:\t Pre Allocate Time(us): " + (startGetQueueInfo - startPreAllocate));
        }
        if (!hasApp) return ;  // 如果没有等待的APP，则停止调度


        // 1 获取集群总资源
        Resource clusterAllResource = getClusterResource();
//        LOG.info("MyLOG:\tCluster All Resource ".concat(clusterAllResource.toString()));

        // 2 获取节点可用资源
        String nodeName = node.getNodeName();
        Resource nodeAvailResource = node.getAvailableResource();
//        LOG.info("MyLOG:\t".concat(nodeName)
//                .concat(" Node Available Resource ")
//                .concat(nodeAvailResource.toString()));


        // 3 获取叶子队列所有应用信息
        numPendingApp = 0;
        CSQueueInfo rootInfo = getQueueAndAppInfo();
        LOG.info("MyLOG:\t Num Pending Applications=".concat(String.valueOf(numPendingApp)));

        startSendRequest = System.nanoTime() / 1000;
        LOG.info("MyLOG:\t Get Queue Info time(us): " + (startSendRequest - startGetQueueInfo));

        // 4 发送智能体，得到结果
        List<ApplicationAttemptId> agentOrder = SocketClient.sendRequireData(
                clusterAllResource.getMemory(), clusterAllResource.getVirtualCores(),
                nodeName, nodeAvailResource.getMemory(), nodeAvailResource.getVirtualCores(),
                rootInfo);

        startPostAllocate = System.nanoTime() / 1000;
        LOG.info("MyLOG:\t Get agent order time(us): " + (startPostAllocate - startSendRequest));


        // 5 执行策略
        //   如果结果为空，则直接结束
        if (agentOrder == null) return ;
        //   将策略中的应用添加到activeApplication中，并记录调度的队列
        LOG.info("MyLOG:\tAgent Order App Size = ".concat(String.valueOf(agentOrder.size())));
        Set<LeafQueue> queues = new HashSet<>();
        for (ApplicationAttemptId attemptId: agentOrder) {
            FiCaSchedulerApp app = getApplicationAttempt(attemptId);
            if (app == null) {
                LOG.info("MyLOG:\t App == null: " + attemptId);
                continue;
            }
            LOG.info("MyLOG:\t added app id = ".concat(attemptId.toString()));
            LeafQueue queue = (LeafQueue) app.getQueue();
            queue.addActiveApplication(app);
            queues.add(queue);
        }

        //   对调度的队列执行调度分配
        LOG.info("MyLOG:\tAgent Order Queue Size = ".concat(String.valueOf(queues.size())));
        for (LeafQueue queue: queues) {
            Resource clusterResource = getClusterResource();
            CSAssignment assignedAssignment =
                    queue.assignContainers(clusterResource, node, false);
            if (Resources.greaterThan(getResourceCalculator(), getClusterResource(),
                    assignedAssignment.getResource(), Resources.none())) {
                root.updateClusterResource(getClusterResource());
//                LOG.info("MyLOG:\t Update Cluster Resource(us): ");
            }
        }
        LOG.info("MyLOG:\t Post allocate time(us): " + (System.nanoTime() / 1000 - startPostAllocate));
    }

    /**
     * BFS获取队列树信息
     * @return
     */
    private CSQueueInfo getQueueAndAppInfo() {
        CSQueueInfo info = new ParentQueueInfo();
//        info.setParent(null);
        java.util.Queue<Pair> queues = new LinkedList<>();
        queues.offer(new Pair(root, null));

        while (!queues.isEmpty()) {
            Pair cur = queues.poll();
            AbstractCSQueue curQueue = (AbstractCSQueue) cur.getCurQueue();  // 实际队列
            CSQueueInfo parentQueueInfo = cur.getParentInfo();  // 提取的队列信息

            CSQueueInfo curQueueInfo;
            if (curQueue instanceof ParentQueue) {
                curQueueInfo = new ParentQueueInfo();
            } else {
                curQueueInfo = new LeafQueueInfo();
            }

            if (parentQueueInfo == null) { // 根节点
                info = curQueueInfo;
            } else {
                ((ParentQueueInfo) parentQueueInfo).addChild(curQueueInfo);
            }

            setCSQueueInfo(curQueue, curQueueInfo); // 公共信息
            if (curQueue instanceof ParentQueue) {
                setParentQueueInfo(curQueue, (ParentQueueInfo) curQueueInfo);
                for (CSQueue child: curQueue.getChildQueues()) {
                    queues.offer(new Pair(child, curQueueInfo));
                }
            } else {
                setLeafQueueInfo((LeafQueue) curQueue, (LeafQueueInfo) curQueueInfo);
            }
        }
        return info;
    }

    private void setLeafQueueInfo(LeafQueue curQueue, LeafQueueInfo curQueueInfo) {
//        curQueueInfo.setAbsoluteMaxAvailCapacity(curQueue.getAbsoluteMaxAvailCapacity(getClusterResource()));

        curQueueInfo.setNumActiveApplications(curQueue.getNumActiveApplications());
        curQueueInfo.setNumPendingApplications(curQueue.getNumPendingApplications());
        numPendingApp += curQueue.getNumPendingApplications();

        curQueueInfo.setMaxActiveApplications(curQueue.getMaximumActiveApplications());
//        curQueueInfo.setMaxActiveAppsUsingAbsCap(curQueue.getMaxActiveAppsUsingAbsCap(getClusterResource()));

//        curQueueInfo.setNodeLocalityDelay(curQueue.getNodeLocalityDelay());
//        curQueueInfo.setMinimumAllocationFactor(curQueue.getMinimumAllocationFactor());

        // 获取已经开始执行的应用
        /*for (FiCaSchedulerApp app: curQueue.getActiveApplications()) {
            FiCaSchedulerAppInfo appInfo = new FiCaSchedulerAppInfo();
            curQueueInfo.addActiveAppInfo(appInfo);

            appInfo.setName(rmContext.getRMApps().get(app.getApplicationId()).getName());
            appInfo.setApplicationId(app.getApplicationId().getId());
            appInfo.setClusterTimestamp(app.getApplicationId().getClusterTimestamp());
            appInfo.setAttemptId(app.getApplicationAttemptId().getAttemptId());
            appInfo.setTotalRequestedMemory(app.getTotalPendingRequests().getMemory());
            appInfo.setTotalRequestedVirtualCores(app.getTotalPendingRequests().getVirtualCores());

//            LOG.info("MyLOG:\t--Active ID=".concat(app.getApplicationId().toString())
//                    .concat("\n\tTotalPendingRequests=".concat(app.getTotalPendingRequests().toString())));
        }*/

        // 获取等待队列的应用
        for (FiCaSchedulerApp app: curQueue.getPendingApplications()) {
            FiCaSchedulerAppInfo appInfo = new FiCaSchedulerAppInfo();
            curQueueInfo.addPendingAppInfo(appInfo);

            appInfo.setName(rmContext.getRMApps().get(app.getApplicationId()).getName());
            appInfo.setApplicationId(app.getApplicationId().getId());
            appInfo.setClusterTimestamp(app.getApplicationId().getClusterTimestamp());
            appInfo.setAttemptId(app.getApplicationAttemptId().getAttemptId());
            appInfo.setTotalRequestedMemory(app.getTotalPendingRequests().getMemory());
            appInfo.setTotalRequestedVirtualCores(app.getTotalPendingRequests().getVirtualCores());

//            LOG.info("MyLOG:\t--Pending ID=".concat(app.getApplicationId().toString())
//                    .concat("\n\t\tTotalPendingRequests=".concat(app.getTotalPendingRequests().toString())));
        }
    }

    private void setParentQueueInfo(AbstractCSQueue curQueue, ParentQueueInfo curQueueInfo) {
        curQueueInfo.setNumApplications(curQueue.getNumApplications());
    }

    /**
     * 提取队列公共信息
     * @param curQueue
     * @param curQueueInfo
     */
    private void setCSQueueInfo(AbstractCSQueue curQueue, CSQueueInfo curQueueInfo) {
        curQueueInfo.setQueueName(curQueue.getQueueName());
        curQueueInfo.setNumContainers(curQueue.getNumApplications());

//        curQueueInfo.setAbsoluteCapacity(curQueue.getAbsoluteCapacity());
//        curQueueInfo.setAbsoluteMaxCapacity(curQueue.getAbsoluteMaximumCapacity());
        curQueueInfo.setAbsoluteUsedCapacity(curQueue.getAbsoluteUsedCapacity());
//        curQueueInfo.setCapacity(curQueue.getCapacity());
//        curQueueInfo.setMaximumCapacity(curQueue.getMaximumCapacity());
//        curQueueInfo.setUsedCapacity(curQueue.getUsedCapacity());

        curQueueInfo.setUsedMemory(curQueue.getUsedResources().getMemory());
        curQueueInfo.setUsedVirtualCores(curQueue.getUsedResources().getVirtualCores());
        curQueueInfo.setMaxAllocationMemory(curQueue.getMaximumAllocation().getMemory());
        curQueueInfo.setMaxAllocationVirtualCores(curQueue.getMaximumAllocation().getVirtualCores());
        curQueueInfo.setMinAllocationMemory(curQueue.getMinimumAllocation().getMemory());
        curQueueInfo.setMinAllocationVirtualCores(curQueue.getMinimumAllocation().getVirtualCores());
    }

    @Override
    public void handle(SchedulerEvent event) {
        switch(event.getType()) {
            case NODE_ADDED:
            {
                NodeAddedSchedulerEvent nodeAddedEvent = (NodeAddedSchedulerEvent)event;
                addNode(nodeAddedEvent.getAddedRMNode());
                recoverContainersOnNode(nodeAddedEvent.getContainerReports(),
                        nodeAddedEvent.getAddedRMNode());
            }
            break;
            case NODE_REMOVED:
            {
                NodeRemovedSchedulerEvent nodeRemovedEvent = (NodeRemovedSchedulerEvent)event;
                removeNode(nodeRemovedEvent.getRemovedRMNode());
            }
            break;
            case NODE_RESOURCE_UPDATE:
            {
                NodeResourceUpdateSchedulerEvent nodeResourceUpdatedEvent =
                        (NodeResourceUpdateSchedulerEvent)event;
                updateNodeAndQueueResource(nodeResourceUpdatedEvent.getRMNode(),
                        nodeResourceUpdatedEvent.getResourceOption());
            }
            break;
            case NODE_UPDATE:
            {
                NodeUpdateSchedulerEvent nodeUpdatedEvent = (NodeUpdateSchedulerEvent)event;
                RMNode node = nodeUpdatedEvent.getRMNode();
                nodeUpdate(node);
                if (!scheduleAsynchronously) {
                    allocateContainersToNode(getNode(node.getNodeID()));
                }
            }
            break;
            case APP_ADDED:
            {
                AppAddedSchedulerEvent appAddedEvent = (AppAddedSchedulerEvent) event;
                String queueName =
                        resolveReservationQueueName(appAddedEvent.getQueue(),
                                appAddedEvent.getApplicationId(),
                                appAddedEvent.getReservationID());
                if (queueName != null) {
                    addApplication(appAddedEvent.getApplicationId(),
                            queueName,
                            appAddedEvent.getUser(),
                            appAddedEvent.getIsAppRecovering());
                }
            }
            break;
            case APP_REMOVED:
            {
                AppRemovedSchedulerEvent appRemovedEvent = (AppRemovedSchedulerEvent)event;
                doneApplication(appRemovedEvent.getApplicationID(),
                        appRemovedEvent.getFinalState());
            }
            break;
            case APP_ATTEMPT_ADDED:
            {
                AppAttemptAddedSchedulerEvent appAttemptAddedEvent =
                        (AppAttemptAddedSchedulerEvent) event;
                addApplicationAttempt(appAttemptAddedEvent.getApplicationAttemptId(),
                        appAttemptAddedEvent.getTransferStateFromPreviousAttempt(),
                        appAttemptAddedEvent.getIsAttemptRecovering());
            }
            break;
            case APP_ATTEMPT_REMOVED:
            {
                AppAttemptRemovedSchedulerEvent appAttemptRemovedEvent =
                        (AppAttemptRemovedSchedulerEvent) event;
                doneApplicationAttempt(appAttemptRemovedEvent.getApplicationAttemptID(),
                        appAttemptRemovedEvent.getFinalAttemptState(),
                        appAttemptRemovedEvent.getKeepContainersAcrossAppAttempts());
            }
            break;
            case CONTAINER_EXPIRED:
            {
                ContainerExpiredSchedulerEvent containerExpiredEvent =
                        (ContainerExpiredSchedulerEvent) event;
                ContainerId containerId = containerExpiredEvent.getContainerId();
                completedContainer(getRMContainer(containerId),
                        SchedulerUtils.createAbnormalContainerStatus(
                                containerId,
                                SchedulerUtils.EXPIRED_CONTAINER),
                        RMContainerEventType.EXPIRE);
            }
            break;
            case CONTAINER_RESCHEDULED:
            {
                ContainerRescheduledEvent containerRescheduledEvent =
                        (ContainerRescheduledEvent) event;
                RMContainer container = containerRescheduledEvent.getContainer();
                recoverResourceRequestForContainer(container);
            }
            break;
            default:
                LOG.error("Invalid eventtype " + event.getType() + ". Ignoring!");
        }
    }

    private synchronized void addNode(RMNode nodeManager) {
        FiCaSchedulerNode schedulerNode = new FiCaSchedulerNode(nodeManager,
                usePortForNodeName);
        nodeTracker.addNode(schedulerNode);

        // update this node to node label manager
        if (labelManager != null) {
            labelManager.activateNode(nodeManager.getNodeID(),
                    schedulerNode.getTotalResource());
        }

//        LOG.info("MyLOG:\t addNode");

        Resource clusterResource = getClusterResource();
        root.updateClusterResource(clusterResource);

        LOG.info("Added node " + nodeManager.getNodeAddress() +
                " clusterResource: " + clusterResource);

        if (scheduleAsynchronously && getNumClusterNodes() == 1) {
            asyncSchedulerThread.beginSchedule();
        }
    }

    private synchronized void removeNode(RMNode nodeInfo) {
        // update this node to node label manager
        if (labelManager != null) {
            labelManager.deactivateNode(nodeInfo.getNodeID());
        }

        NodeId nodeId = nodeInfo.getNodeID();
        FiCaSchedulerNode node = nodeTracker.getNode(nodeId);
        if (node == null) {
            LOG.error("Attempting to remove non-existent node " + nodeId);
            return;
        }

        // Remove running containers
        List<RMContainer> runningContainers = node.getRunningContainers();
        for (RMContainer container : runningContainers) {
            completedContainer(container,
                    SchedulerUtils.createAbnormalContainerStatus(
                            container.getContainerId(),
                            SchedulerUtils.LOST_CONTAINER),
                    RMContainerEventType.KILL);
        }

        // Remove reservations, if any
        RMContainer reservedContainer = node.getReservedContainer();
        if (reservedContainer != null) {
            completedContainer(reservedContainer,
                    SchedulerUtils.createAbnormalContainerStatus(
                            reservedContainer.getContainerId(),
                            SchedulerUtils.LOST_CONTAINER),
                    RMContainerEventType.KILL);
        }

//        LOG.info("MyLOG:\t removeNode");

        nodeTracker.removeNode(nodeId);
        Resource clusterResource = getClusterResource();
        root.updateClusterResource(clusterResource);
        int numNodes = nodeTracker.nodeCount();

        if (scheduleAsynchronously && numNodes == 0) {
            asyncSchedulerThread.suspendSchedule();
        }

        LOG.info("Removed node " + nodeInfo.getNodeAddress() +
                " clusterResource: " + getClusterResource());
    }

    @Lock(CapacityScheduler.class)
    @Override
    protected synchronized void completedContainer(RMContainer rmContainer,
                                                   ContainerStatus containerStatus, RMContainerEventType event) {
        if (rmContainer == null) {
            LOG.info("Null container completed...");
            return;
        }

        Container container = rmContainer.getContainer();

        // Get the application for the finished container
        FiCaSchedulerApp application =
                getCurrentAttemptForContainer(container.getId());
        ApplicationId appId =
                container.getId().getApplicationAttemptId().getApplicationId();
        if (application == null) {
            LOG.info("Container " + container + " of" + " unknown application "
                    + appId + " completed with event " + event);
            return;
        }

        // Get the node on which the container was allocated
        FiCaSchedulerNode node = getNode(container.getNodeId());

        // Inform the queue
        LeafQueue queue = (LeafQueue)application.getQueue();
        queue.completedContainer(getClusterResource(), application, node,
                rmContainer, containerStatus, event, null, true);

        LOG.info("Application attempt " + application.getApplicationAttemptId()
                + " released container " + container.getId() + " on node: " + node
                + " with event: " + event);
    }

    @Lock(Lock.NoLock.class)
    @VisibleForTesting
    @Override
    public FiCaSchedulerApp getApplicationAttempt(
            ApplicationAttemptId applicationAttemptId) {
        return super.getApplicationAttempt(applicationAttemptId);
    }

    @Lock(Lock.NoLock.class)
    public FiCaSchedulerNode getNode(NodeId nodeId) {
        return nodeTracker.getNode(nodeId);
    }

    @Override
    @Lock(Lock.NoLock.class)
    public void recover(RMState state) throws Exception {
        // NOT IMPLEMENTED
    }

    @Override
    public void dropContainerReservation(RMContainer container) {
        if(LOG.isDebugEnabled()){
            LOG.debug("DROP_RESERVATION:" + container.toString());
        }
        completedContainer(container,
                SchedulerUtils.createAbnormalContainerStatus(
                        container.getContainerId(),
                        SchedulerUtils.UNRESERVED_CONTAINER),
                RMContainerEventType.KILL);
    }

    @Override
    public void preemptContainer(ApplicationAttemptId aid, RMContainer cont) {
        if(LOG.isDebugEnabled()){
            LOG.debug("PREEMPT_CONTAINER: application:" + aid.toString() +
                    " container: " + cont.toString());
        }
        FiCaSchedulerApp app = getApplicationAttempt(aid);
        if (app != null) {
            app.addPreemptContainer(cont.getContainerId());
        }
    }

    @Override
    public void killContainer(RMContainer cont) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("KILL_CONTAINER: container" + cont.toString());
        }
        completedContainer(cont, SchedulerUtils.createPreemptedContainerStatus(
                cont.getContainerId(), SchedulerUtils.PREEMPTED_CONTAINER),
                RMContainerEventType.KILL);
    }

    @Override
    public synchronized boolean checkAccess(UserGroupInformation callerUGI,
                                            QueueACL acl, String queueName) {
        CSQueue queue = getQueue(queueName);
        if (queue == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("ACL not found for queue access-type " + acl
                        + " for queue " + queueName);
            }
            return false;
        }
        return queue.hasAccess(acl, callerUGI);
    }

    @Override
    public List<ApplicationAttemptId> getAppsInQueue(String queueName) {
        CSQueue queue = queues.get(queueName);
        if (queue == null) {
            return null;
        }
        List<ApplicationAttemptId> apps = new ArrayList<ApplicationAttemptId>();
        queue.collectSchedulerApplications(apps);
        return apps;
    }

    private CapacitySchedulerConfiguration loadCapacitySchedulerConfiguration(
            Configuration configuration) throws IOException {
        try {
            InputStream CSInputStream =
                    this.rmContext.getConfigurationProvider()
                            .getConfigurationInputStream(configuration,
                                    YarnConfiguration.CS_CONFIGURATION_FILE);
            if (CSInputStream != null) {
                configuration.addResource(CSInputStream);
                return new CapacitySchedulerConfiguration(configuration, false);
            }
            return new CapacitySchedulerConfiguration(configuration, true);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private synchronized String resolveReservationQueueName(String queueName,
                                                            ApplicationId applicationId, ReservationId reservationID) {
        CSQueue queue = getQueue(queueName);
        // Check if the queue is a plan queue
        if ((queue == null) || !(queue instanceof PlanQueue)) {
            return queueName;
        }
        if (reservationID != null) {
            String resQName = reservationID.toString();
            queue = getQueue(resQName);
            if (queue == null) {
                String message =
                        "Application "
                                + applicationId
                                + " submitted to a reservation which is not yet currently active: "
                                + resQName;
                this.rmContext.getDispatcher().getEventHandler()
                        .handle(new RMAppRejectedEvent(applicationId, message));
                return null;
            }
            if (!queue.getParent().getQueueName().equals(queueName)) {
                String message =
                        "Application: " + applicationId + " submitted to a reservation "
                                + resQName + " which does not belong to the specified queue: "
                                + queueName;
                this.rmContext.getDispatcher().getEventHandler()
                        .handle(new RMAppRejectedEvent(applicationId, message));
                return null;
            }
            // use the reservation queue to run the app
            queueName = resQName;
        } else {
            // use the default child queue of the plan for unreserved apps
            queueName = queueName + PlanQueue.DEFAULT_QUEUE_SUFFIX;
        }
        return queueName;
    }

    @Override
    public synchronized void removeQueue(String queueName)
            throws SchedulerDynamicEditException {
        LOG.info("Removing queue: " + queueName);
        CSQueue q = this.getQueue(queueName);
        if (!(q instanceof ReservationQueue)) {
            throw new SchedulerDynamicEditException("The queue that we are asked "
                    + "to remove (" + queueName + ") is not a ReservationQueue");
        }
        ReservationQueue disposableLeafQueue = (ReservationQueue) q;
        // at this point we should have no more apps
        if (disposableLeafQueue.getNumApplications() > 0) {
            throw new SchedulerDynamicEditException("The queue " + queueName
                    + " is not empty " + disposableLeafQueue.getApplications().size()
                    + " active apps " + disposableLeafQueue.pendingApplications.size()
                    + " pending apps");
        }

        ((PlanQueue) disposableLeafQueue.getParent()).removeChildQueue(q);
        this.queues.remove(queueName);
        LOG.info("Removal of ReservationQueue " + queueName + " has succeeded");
    }

    @Override
    public synchronized void addQueue(Queue queue)
            throws SchedulerDynamicEditException {

        if (!(queue instanceof ReservationQueue)) {
            throw new SchedulerDynamicEditException("Queue " + queue.getQueueName()
                    + " is not a ReservationQueue");
        }

        ReservationQueue newQueue = (ReservationQueue) queue;

        if (newQueue.getParent() == null
                || !(newQueue.getParent() instanceof PlanQueue)) {
            throw new SchedulerDynamicEditException("ParentQueue for "
                    + newQueue.getQueueName()
                    + " is not properly set (should be set and be a PlanQueue)");
        }

        PlanQueue parentPlan = (PlanQueue) newQueue.getParent();
        String queuename = newQueue.getQueueName();
        parentPlan.addChildQueue(newQueue);
        this.queues.put(queuename, newQueue);
        LOG.info("Creation of ReservationQueue " + newQueue + " succeeded");
    }

    @Override
    public synchronized void setEntitlement(String inQueue,
                                            QueueEntitlement entitlement) throws SchedulerDynamicEditException,
            YarnException {
        LeafQueue queue = getAndCheckLeafQueue(inQueue);
        ParentQueue parent = (ParentQueue) queue.getParent();

        if (!(queue instanceof ReservationQueue)) {
            throw new SchedulerDynamicEditException("Entitlement can not be"
                    + " modified dynamically since queue " + inQueue
                    + " is not a ReservationQueue");
        }

        if (!(parent instanceof PlanQueue)) {
            throw new SchedulerDynamicEditException("The parent of ReservationQueue "
                    + inQueue + " must be an PlanQueue");
        }

        ReservationQueue newQueue = (ReservationQueue) queue;

        float sumChilds = ((PlanQueue) parent).sumOfChildCapacities();
        float newChildCap = sumChilds - queue.getCapacity() + entitlement.getCapacity();

        if (newChildCap >= 0 && newChildCap < 1.0f + CSQueueUtils.EPSILON) {
            // note: epsilon checks here are not ok, as the epsilons might accumulate
            // and become a problem in aggregate
            if (Math.abs(entitlement.getCapacity() - queue.getCapacity()) == 0
                    && Math.abs(entitlement.getMaxCapacity() - queue.getMaximumCapacity()) == 0) {
                return;
            }
            newQueue.setEntitlement(entitlement);
        } else {
            throw new SchedulerDynamicEditException(
                    "Sum of child queues would exceed 100% for PlanQueue: "
                            + parent.getQueueName());
        }
        LOG.info("Set entitlement for ReservationQueue " + inQueue + "  to "
                + queue.getCapacity() + " request was (" + entitlement.getCapacity() + ")");
    }

    @Override
    public synchronized String moveApplication(ApplicationId appId,
                                               String targetQueueName) throws YarnException {
        FiCaSchedulerApp app =
                getApplicationAttempt(ApplicationAttemptId.newInstance(appId, 0));
        String sourceQueueName = app.getQueue().getQueueName();
        LeafQueue source = getAndCheckLeafQueue(sourceQueueName);
        String destQueueName = handleMoveToPlanQueue(targetQueueName);
        LeafQueue dest = getAndCheckLeafQueue(destQueueName);
        // Validation check - ACLs, submission limits for user & queue
        String user = app.getUser();
        try {
            dest.submitApplication(appId, user, destQueueName);
        } catch (AccessControlException e) {
            throw new YarnException(e);
        }
        // Move all live containers
        for (RMContainer rmContainer : app.getLiveContainers()) {
            source.detachContainer(getClusterResource(), app, rmContainer);
            // attach the Container to another queue
            dest.attachContainer(getClusterResource(), app, rmContainer);
        }
        // Detach the application..
        source.finishApplicationAttempt(app, sourceQueueName);
        source.getParent().finishApplication(appId, app.getUser());
        // Finish app & update metrics
        app.move(dest);
        // Submit to a new queue
        dest.submitApplicationAttempt(app, user);
        applications.get(appId).setQueue(dest);
        LOG.info("App: " + app.getApplicationId() + " successfully moved from "
                + sourceQueueName + " to: " + destQueueName);
        return targetQueueName;
    }

    /**
     * Check that the String provided in input is the name of an existing,
     * LeafQueue, if successful returns the queue.
     *
     * @param queue
     * @return the LeafQueue
     * @throws YarnException
     */
    private LeafQueue getAndCheckLeafQueue(String queue) throws YarnException {
        CSQueue ret = this.getQueue(queue);
        if (ret == null) {
            throw new YarnException("The specified Queue: " + queue
                    + " doesn't exist");
        }
        if (!(ret instanceof LeafQueue)) {
            throw new YarnException("The specified Queue: " + queue
                    + " is not a Leaf Queue. Move is supported only for Leaf Queues.");
        }
        return (LeafQueue) ret;
    }

    /** {@inheritDoc} */
    @Override
    public EnumSet<SchedulerResourceTypes> getSchedulingResourceTypes() {
        if (calculator.getClass().getName()
                .equals(DefaultResourceCalculator.class.getName())) {
            return EnumSet.of(SchedulerResourceTypes.MEMORY);
        }
        return EnumSet
                .of(SchedulerResourceTypes.MEMORY, SchedulerResourceTypes.CPU);
    }

    private String handleMoveToPlanQueue(String targetQueueName) {
        CSQueue dest = getQueue(targetQueueName);
        if (dest != null && dest instanceof PlanQueue) {
            // use the default child reservation queue of the plan
            targetQueueName = targetQueueName + PlanQueue.DEFAULT_QUEUE_SUFFIX;
        }
        return targetQueueName;
    }

    @Override
    public Set<String> getPlanQueues() {
        Set<String> ret = new HashSet<String>();
        for (Map.Entry<String, CSQueue> l : queues.entrySet()) {
            if (l.getValue() instanceof PlanQueue) {
                ret.add(l.getKey());
            }
        }
        return ret;
    }



    /**************/

    public void init(Configuration configuration,
                     ConcurrentMap<ApplicationId, SchedulerApplication<FiCaSchedulerApp>> applications)
            throws IOException {
        initScheduler(configuration);
        this.applications = applications;
    }

    class Pair {
        private CSQueue curQueue;
        private CSQueueInfo parentInfo;

        public Pair(CSQueue curQueue, CSQueueInfo parentInfo) {
            this.curQueue = curQueue;
            this.parentInfo = parentInfo;
        }

        public CSQueue getCurQueue() {
            return curQueue;
        }

        public CSQueueInfo getParentInfo() {
            return parentInfo;
        }
    }
}
