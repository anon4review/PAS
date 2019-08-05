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
package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppRecoverEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppRejectedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.agentutil.OutputToFile;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.agentutil.SocketClient;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.expr1.Expr1Scheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.multiqueue.MultiQueueScheduler;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class manages the list of applications for the resource manager. 
 */
public class RMAppManager implements EventHandler<RMAppManagerEvent>, 
                                        Recoverable {

  private static final Log LOG = LogFactory.getLog(RMAppManager.class);

  private int maxCompletedAppsInMemory;
  private int maxCompletedAppsInStateStore;
  protected int completedAppsInStateStore = 0;
  private LinkedList<ApplicationId> completedApps = new LinkedList<ApplicationId>();

  private final RMContext rmContext;
  private final ApplicationMasterService masterService;
  private final YarnScheduler scheduler;
  private final ApplicationACLsManager applicationACLsManager;
  private Configuration conf;

  public RMAppManager(RMContext context,
      YarnScheduler scheduler, ApplicationMasterService masterService,
      ApplicationACLsManager applicationACLsManager, Configuration conf) {
    this.rmContext = context;
    this.scheduler = scheduler;
    this.masterService = masterService;
    this.applicationACLsManager = applicationACLsManager;
    this.conf = conf;
    this.maxCompletedAppsInMemory = conf.getInt(
        YarnConfiguration.RM_MAX_COMPLETED_APPLICATIONS,
        YarnConfiguration.DEFAULT_RM_MAX_COMPLETED_APPLICATIONS);
    this.maxCompletedAppsInStateStore =
        conf.getInt(
          YarnConfiguration.RM_STATE_STORE_MAX_COMPLETED_APPLICATIONS,
          YarnConfiguration.DEFAULT_RM_STATE_STORE_MAX_COMPLETED_APPLICATIONS);
    if (this.maxCompletedAppsInStateStore > this.maxCompletedAppsInMemory) {
      this.maxCompletedAppsInStateStore = this.maxCompletedAppsInMemory;
    }
  }

  /**
   *  This class is for logging the application summary.
   */
  static class ApplicationSummary {
    static final Log LOG = LogFactory.getLog(ApplicationSummary.class);

    // Escape sequences 
    static final char EQUALS = '=';
    static final char[] charsToEscape =
      {StringUtils.COMMA, EQUALS, StringUtils.ESCAPE_CHAR};

    static class SummaryBuilder {
      final StringBuilder buffer = new StringBuilder();

      // A little optimization for a very common case
      SummaryBuilder add(String key, long value) {
        return _add(key, Long.toString(value));
      }

      <T> SummaryBuilder add(String key, T value) {
        String escapedString = StringUtils.escapeString(String.valueOf(value),
            StringUtils.ESCAPE_CHAR, charsToEscape).replaceAll("\n", "\\\\n")
            .replaceAll("\r", "\\\\r");
        return _add(key, escapedString);
      }

      SummaryBuilder add(SummaryBuilder summary) {
        if (buffer.length() > 0) buffer.append(StringUtils.COMMA);
        buffer.append(summary.buffer);
        return this;
      }

      SummaryBuilder _add(String key, String value) {
        if (buffer.length() > 0) buffer.append(StringUtils.COMMA);
        buffer.append(key).append(EQUALS).append(value);
        return this;
      }

      @Override public String toString() {
        return buffer.toString();
      }
    }

    /**
     * create a summary of the application's runtime.
     * 
     * @param app {@link RMApp} whose summary is to be created, cannot
     *            be <code>null</code>.
     */
    public static SummaryBuilder createAppSummary(RMApp app) {
      String trackingUrl = "N/A";
      String host = "N/A";
      RMAppAttempt attempt = app.getCurrentAppAttempt();
      if (attempt != null) {
        trackingUrl = attempt.getTrackingUrl();
        host = attempt.getHost();
      }
      RMAppMetrics metrics = app.getRMAppMetrics();
      SummaryBuilder summary = new SummaryBuilder()
          .add("appId", app.getApplicationId())
          .add("name", app.getName())
          .add("user", app.getUser())
          .add("queue", app.getQueue())
          .add("state", app.getState())
          .add("trackingUrl", trackingUrl)
          .add("appMasterHost", host)
          .add("startTime", app.getStartTime())
          .add("finishTime", app.getFinishTime())
          .add("finalStatus", app.getFinalApplicationStatus())
          .add("memorySeconds", metrics.getMemorySeconds())
          .add("vcoreSeconds", metrics.getVcoreSeconds())
          .add("preemptedAMContainers", metrics.getNumAMContainersPreempted())
          .add("preemptedNonAMContainers", metrics.getNumNonAMContainersPreempted())
          .add("preemptedResources", metrics.getResourcePreempted());
      return summary;
    }

    /**
     * Log a summary of the application's runtime.
     * 
     * @param app {@link RMApp} whose summary is to be logged
     */
    public static void logAppSummary(RMApp app) {
      if (app != null) {
        LOG.info(createAppSummary(app));
      }
    }
  }

  @VisibleForTesting
  public void logApplicationSummary(ApplicationId appId) {
    ApplicationSummary.logAppSummary(rmContext.getRMApps().get(appId));
  }

  protected synchronized int getCompletedAppsListSize() {
    return this.completedApps.size(); 
  }

  protected synchronized void finishApplication(ApplicationId applicationId) {
    if (applicationId == null) {
      LOG.error("RMAppManager received completed appId of null, skipping");
    } else {
      // Inform the DelegationTokenRenewer
      if (UserGroupInformation.isSecurityEnabled()) {
        rmContext.getDelegationTokenRenewer().applicationFinished(applicationId);
      }
      
      completedApps.add(applicationId);
      completedAppsInStateStore++;
      writeAuditLog(applicationId);
    }
  }

  protected void writeAuditLog(ApplicationId appId) {
    RMApp app = rmContext.getRMApps().get(appId);
    String operation = "UNKONWN";
    boolean success = false;
    switch (app.getState()) {
      case FAILED: 
        operation = AuditConstants.FINISH_FAILED_APP;
        break;
      case FINISHED:
        operation = AuditConstants.FINISH_SUCCESS_APP;
        success = true;
        break;
      case KILLED: 
        operation = AuditConstants.FINISH_KILLED_APP;
        success = true;
        break;
      default:
    }
    
    if (success) {
      RMAuditLogger.logSuccess(app.getUser(), operation,
          "RMAppManager", app.getApplicationId());
    } else {
      StringBuilder diag = app.getDiagnostics(); 
      String msg = diag == null ? null : diag.toString();
      RMAuditLogger.logFailure(app.getUser(), operation, msg, "RMAppManager",
          "App failed with state: " + app.getState(), appId);
    }
  }

  /*
   * check to see if hit the limit for max # completed apps kept
   */
  protected synchronized void checkAppNumCompletedLimit() {
    // check apps kept in state store.
    while (completedAppsInStateStore > this.maxCompletedAppsInStateStore) {
      ApplicationId removeId =
          completedApps.get(completedApps.size() - completedAppsInStateStore);
      RMApp removeApp = rmContext.getRMApps().get(removeId);
      LOG.info("Max number of completed apps kept in state store met:"
          + " maxCompletedAppsInStateStore = " + maxCompletedAppsInStateStore
          + ", removing app " + removeApp.getApplicationId()
          + " from state store.");
      rmContext.getStateStore().removeApplication(removeApp);
      completedAppsInStateStore--;
    }

    // check apps kept in memorty.
    while (completedApps.size() > this.maxCompletedAppsInMemory) {
      ApplicationId removeId = completedApps.remove();
      LOG.info("Application should be expired, max number of completed apps"
          + " kept in memory met: maxCompletedAppsInMemory = "
          + this.maxCompletedAppsInMemory + ", removing app " + removeId
          + " from memory: ");
      rmContext.getRMApps().remove(removeId);
      this.applicationACLsManager.removeApplication(removeId);
    }
  }

  @SuppressWarnings("unchecked")
  protected void submitApplication(
      ApplicationSubmissionContext submissionContext, long submitTime,
      String user) throws YarnException {
    ApplicationId applicationId = submissionContext.getApplicationId();

    RMAppImpl application =
        createAndPopulateNewRMApp(submissionContext, submitTime, user, false);
    ApplicationId appId = submissionContext.getApplicationId();
    Credentials credentials = null;
    try {
      credentials = parseCredentials(submissionContext);
      if (UserGroupInformation.isSecurityEnabled()) {
        this.rmContext.getDelegationTokenRenewer().addApplicationAsync(appId,
            credentials, submissionContext.getCancelTokensWhenComplete(),
            application.getUser());
      } else {
        // Dispatcher is not yet started at this time, so these START events
        // enqueued should be guaranteed to be first processed when dispatcher
        // gets started.
        this.rmContext.getDispatcher().getEventHandler()
            .handle(new RMAppEvent(applicationId, RMAppEventType.START));
      }
    } catch (Exception e) {
      LOG.warn("Unable to parse credentials.", e);
      // Sending APP_REJECTED is fine, since we assume that the
      // RMApp is in NEW state and thus we haven't yet informed the
      // scheduler about the existence of the application
      assert application.getState() == RMAppState.NEW;
      this.rmContext.getDispatcher().getEventHandler()
          .handle(new RMAppRejectedEvent(applicationId, e.getMessage()));
      throw RPCUtil.getRemoteException(e);
    }
  }

  protected void recoverApplication(ApplicationStateData appState,
      RMState rmState) throws Exception {
    ApplicationSubmissionContext appContext =
        appState.getApplicationSubmissionContext();
    ApplicationId appId = appContext.getApplicationId();

    // create and recover app.
    RMAppImpl application =
        createAndPopulateNewRMApp(appContext, appState.getSubmitTime(),
            appState.getUser(), true);

    application.handle(new RMAppRecoverEvent(appId, rmState));
  }

  private RMAppImpl createAndPopulateNewRMApp(
      ApplicationSubmissionContext submissionContext, long submitTime,
      String user, boolean isRecovery) throws YarnException {
    ApplicationId applicationId = submissionContext.getApplicationId();
    List<ResourceRequest> amReqs =
        validateAndCreateResourceRequest(submissionContext, isRecovery);

    // Create RMApp
    RMAppImpl application =
        new RMAppImpl(applicationId, rmContext, this.conf,
            submissionContext.getApplicationName(), user,
            submissionContext.getQueue(),
            submissionContext, this.scheduler, this.masterService,
            submitTime, submissionContext.getApplicationType(),
            submissionContext.getApplicationTags(), amReqs);

    // Concurrent app submissions with same applicationId will fail here
    // Concurrent app submissions with different applicationIds will not
    // influence each other
    if (rmContext.getRMApps().putIfAbsent(applicationId, application) !=
        null) {
      String message = "Application with id " + applicationId
          + " is already present! Cannot add a duplicate!";
      LOG.warn(message);
      throw new YarnException(message);
    }
    // Inform the ACLs Manager
    this.applicationACLsManager.addApplication(applicationId,
        submissionContext.getAMContainerSpec().getApplicationACLs());
    String appViewACLs = submissionContext.getAMContainerSpec()
        .getApplicationACLs().get(ApplicationAccessType.VIEW_APP);
    rmContext.getSystemMetricsPublisher().appACLsUpdated(
        application, appViewACLs, System.currentTimeMillis());
    return application;
  }

  private List<ResourceRequest> validateAndCreateResourceRequest(
      ApplicationSubmissionContext submissionContext, boolean isRecovery)
      throws InvalidResourceRequestException {
    // Validation of the ApplicationSubmissionContext needs to be completed
    // here. Only those fields that are dependent on RM's configuration are
    // checked here as they have to be validated whether they are part of new
    // submission or just being recovered.

    // Check whether AM resource requirements are within required limits
    if (!submissionContext.getUnmanagedAM()) {
      List<ResourceRequest> amReqs =
          submissionContext.getAMContainerResourceRequests();
      if (amReqs == null || amReqs.isEmpty()) {
        if (submissionContext.getResource() != null) {
          amReqs = Collections.singletonList(BuilderUtils
              .newResourceRequest(RMAppAttemptImpl.AM_CONTAINER_PRIORITY,
                  ResourceRequest.ANY, submissionContext.getResource(), 1));
        } else {
          throw new InvalidResourceRequestException("Invalid resource request, "
              + "no resources requested");
        }
      }

      try {
        // Find the ANY request and ensure there's only one
        ResourceRequest anyReq = null;
        for (ResourceRequest amReq : amReqs) {
          if (amReq.getResourceName().equals(ResourceRequest.ANY)) {
            if (anyReq == null) {
              anyReq = amReq;
            } else {
              throw new InvalidResourceRequestException("Invalid resource "
                  + "request, only one resource request with "
                  + ResourceRequest.ANY + " is allowed");
            }
          }
        }
        if (anyReq == null) {
          throw new InvalidResourceRequestException("Invalid resource request, "
              + "no resource request specified with " + ResourceRequest.ANY);
        }

        // Make sure that all of the requests agree with the ANY request
        // and have correct values
        for (ResourceRequest amReq : amReqs) {
          amReq.setCapability(anyReq.getCapability());
          amReq.setNumContainers(1);
          amReq.setPriority(RMAppAttemptImpl.AM_CONTAINER_PRIORITY);
        }

        // set label expression for AM ANY request if not set
        if (null == anyReq.getNodeLabelExpression()) {
          anyReq.setNodeLabelExpression(submissionContext
              .getNodeLabelExpression());
        }

        // Put ANY request at the front
        if (!amReqs.get(0).equals(anyReq)) {
          amReqs.remove(anyReq);
          amReqs.add(0, anyReq);
        }

        // Normalize all requests
        for (ResourceRequest amReq : amReqs) {
          SchedulerUtils.normalizeAndValidateRequest(amReq,
              scheduler.getMaximumResourceCapability(),
              submissionContext.getQueue(), scheduler, isRecovery);

          scheduler.normalizeResource(amReq);
        }
        return amReqs;
      } catch (InvalidResourceRequestException e) {
        LOG.warn("RM app submission failed in validating AM resource request"
            + " for application " + submissionContext.getApplicationId(), e);
        throw e;
      }
    }

    return null;
  }
  
  protected Credentials parseCredentials(
      ApplicationSubmissionContext application) throws IOException {
    Credentials credentials = new Credentials();
    DataInputByteBuffer dibb = new DataInputByteBuffer();
    ByteBuffer tokens = application.getAMContainerSpec().getTokens();
    if (tokens != null) {
      dibb.reset(tokens);
      credentials.readTokenStorageStream(dibb);
      tokens.rewind();
    }
    return credentials;
  }
  
  @Override
  public void recover(RMState state) throws Exception {
    RMStateStore store = rmContext.getStateStore();
    assert store != null;
    // recover applications
    Map<ApplicationId, ApplicationStateData> appStates =
        state.getApplicationState();
    LOG.info("Recovering " + appStates.size() + " applications");

    int count = 0;

    try {
      for (ApplicationStateData appState : appStates.values()) {
        recoverApplication(appState, state);
        count += 1;
      }
    } finally {
      LOG.info("Successfully recovered " + count  + " out of "
          + appStates.size() + " applications");
    }
  }

  @Override
  public void handle(RMAppManagerEvent event) {
    ApplicationId applicationId = event.getApplicationId();
    LOG.debug("RMAppManager processing event for " 
        + applicationId + " of type " + event.getType());
    switch(event.getType()) {
      case APP_COMPLETED: 
      {
        finishApplication(applicationId);

        sendFinishedInfo(applicationId);

        logApplicationSummary(applicationId);
        checkAppNumCompletedLimit(); 
      } 
      break;
      default:
        LOG.error("Invalid eventtype " + event.getType() + ". Ignoring!");
      }
  }


  /**
   * ***********************************************************************
   */

  private void sendFinishedInfo(ApplicationId applicationId) {
    RMApp rmApp = rmContext.getRMApps().get(applicationId);
    String name = rmApp.getName();

    try {
      LOG.info("MyLOG:\t RMApp Submit Time".concat(String.valueOf(rmApp.getSubmitTime())));
      LOG.info("MyLOG:\t RMApp Start Time".concat(String.valueOf(rmApp.getStartTime())));
      LOG.info("MyLOG:\t RMApp Finish Time".concat(String.valueOf(rmApp.getFinishTime())));
      LOG.info("MyLOG:\t Attempt Finish Time".concat(String.valueOf(rmApp.getCurrentAppAttempt().getFinishTime())));

      long appSubmitTime = rmApp.getSubmitTime();
      long attemptFinishTime = rmApp.getCurrentAppAttempt().getFinishTime();

      long turnaroundTime = attemptFinishTime - appSubmitTime;

      int appId = applicationId.getId();
      long clusterTimestamp = applicationId.getClusterTimestamp();
//      if (this.scheduler instanceof MultiQueueScheduler ||
//              this.scheduler instanceof Expr1Scheduler) {
//        SocketClient.sendExecutedTime(rmApp.getCurrentAppAttempt().getAppAttemptId(), turnaroundTime);
//      }
      OutputToFile.outputTurnaroundTime(appId, clusterTimestamp,
              rmApp.getCurrentAppAttempt().getAppAttemptId().getAttemptId(), name, turnaroundTime);
    } catch (Exception e) {
      LOG.info("MyLOG:\t ".concat(e.toString()));
    }
  }
}
