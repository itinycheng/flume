/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.lifecycle;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flume.FlumeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class LifecycleSupervisor implements LifecycleAware {

  private static final Logger logger = LoggerFactory.getLogger(LifecycleSupervisor.class);

  private Map<LifecycleAware, Supervisoree> supervisedProcesses;
  private Map<LifecycleAware, ScheduledFuture<?>> monitorFutures;

  private ScheduledThreadPoolExecutor monitorService;

  private LifecycleState lifecycleState;
  private Purger purger;
  private boolean needToPurge;

    // TODO: 2016/11/15 tiny - 10 ~ 20个线程，30s空闲存活时间
    public LifecycleSupervisor() {
        lifecycleState = LifecycleState.IDLE;
        supervisedProcesses = new HashMap<LifecycleAware, Supervisoree>();
        monitorFutures = new HashMap<LifecycleAware, ScheduledFuture<?>>();
        monitorService = new ScheduledThreadPoolExecutor(10,
            new ThreadFactoryBuilder().setNameFormat(
                "lifecycleSupervisor-" + Thread.currentThread().getId() + "-%d")
                .build());
        monitorService.setMaximumPoolSize(20);
        monitorService.setKeepAliveTime(30, TimeUnit.SECONDS);
        // TODO: 2016/11/15 tiny - 用于清除已经取消的任务
        purger = new Purger();
        needToPurge = false;
      }

  @Override
  public synchronized void start() {

    logger.info("Starting lifecycle supervisor {}", Thread.currentThread()
        .getId());
    monitorService.scheduleWithFixedDelay(purger, 2, 2, TimeUnit.HOURS);
    lifecycleState = LifecycleState.START;

    logger.debug("Lifecycle supervisor started");
  }

  @Override
  public synchronized void stop() {

    logger.info("Stopping lifecycle supervisor {}", Thread.currentThread()
        .getId());

    if (monitorService != null) {
      monitorService.shutdown();
      try {
        monitorService.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.error("Interrupted while waiting for monitor service to stop");
      }
      if (!monitorService.isTerminated()) {
        monitorService.shutdownNow();
        try {
          while (!monitorService.isTerminated()) {
            monitorService.awaitTermination(10, TimeUnit.SECONDS);
          }
        } catch (InterruptedException e) {
          logger.error("Interrupted while waiting for monitor service to stop");
        }
      }
    }

    for (final Entry<LifecycleAware, Supervisoree> entry : supervisedProcesses.entrySet()) {

      if (entry.getKey().getLifecycleState().equals(LifecycleState.START)) {
        entry.getValue().status.desiredState = LifecycleState.STOP;
        entry.getKey().stop();
      }
    }

    /* If we've failed, preserve the error state. */
    if (lifecycleState.equals(LifecycleState.START)) {
      lifecycleState = LifecycleState.STOP;
    }
    supervisedProcesses.clear();
    monitorFutures.clear();
    logger.debug("Lifecycle supervisor stopped");
  }

  public synchronized void fail() {
    lifecycleState = LifecycleState.ERROR;
  }

  // TODO: 2016/11/15 tiny - 监督组件lifecycleAware
  // TODO: 2016/11/15 tiny - lifecycleAware包括channel、sink、source、configuration etc.
  public synchronized void supervise(LifecycleAware lifecycleAware,
      SupervisorPolicy policy, LifecycleState desiredState) {
    if (this.monitorService.isShutdown()
        || this.monitorService.isTerminated()
        || this.monitorService.isTerminating()) {
      throw new FlumeException("Supervise called on " + lifecycleAware + " " +
          "after shutdown has been initiated. " + lifecycleAware + " will not" +
          " be started");
    }

    Preconditions.checkState(!supervisedProcesses.containsKey(lifecycleAware),
        "Refusing to supervise " + lifecycleAware + " more than once");

    if (logger.isDebugEnabled()) {
      logger.debug("Supervising service:{} policy:{} desiredState:{}",
          new Object[] { lifecycleAware, policy, desiredState });
    }
    // TODO: 2016/11/15 tiny - 封装当前组件（component）期望状态
    Supervisoree process = new Supervisoree();
    process.status = new Status();

    process.policy = policy;
    process.status.desiredState = desiredState;
    process.status.error = false;

    MonitorRunnable monitorRunnable = new MonitorRunnable();
    monitorRunnable.lifecycleAware = lifecycleAware;
    monitorRunnable.supervisoree = process;
    monitorRunnable.monitorService = monitorService;
    // TODO: 2016/11/15 tiny - unfinished
    supervisedProcesses.put(lifecycleAware, process);

      // TODO: 2016/11/15 tiny - 上次任务完成3s后执行下次操作
      // TODO: 2016/11/15 tiny - 1. lifecycleAware = PollingPropertiesFileConfigurationProvider
    ScheduledFuture<?> future = monitorService.scheduleWithFixedDelay(
        monitorRunnable, 0, 3, TimeUnit.SECONDS);
      // TODO: 2016/11/15 tiny - unfinished
    monitorFutures.put(lifecycleAware, future);
  }

  public synchronized void unsupervise(LifecycleAware lifecycleAware) {

    Preconditions.checkState(supervisedProcesses.containsKey(lifecycleAware),
        "Unaware of " + lifecycleAware + " - can not unsupervise");

    logger.debug("Unsupervising service:{}", lifecycleAware);

    synchronized (lifecycleAware) {
      Supervisoree supervisoree = supervisedProcesses.get(lifecycleAware);
      supervisoree.status.discard = true;
      this.setDesiredState(lifecycleAware, LifecycleState.STOP);
      logger.info("Stopping component: {}", lifecycleAware);
      lifecycleAware.stop();
    }
    supervisedProcesses.remove(lifecycleAware);
    //We need to do this because a reconfiguration simply unsupervises old
    //components and supervises new ones.
    monitorFutures.get(lifecycleAware).cancel(false);
    //purges are expensive, so it is done only once every 2 hours.
    needToPurge = true;
    monitorFutures.remove(lifecycleAware);
  }

  public synchronized void setDesiredState(LifecycleAware lifecycleAware,
      LifecycleState desiredState) {

    Preconditions.checkState(supervisedProcesses.containsKey(lifecycleAware),
        "Unaware of " + lifecycleAware + " - can not set desired state to "
            + desiredState);

    logger.debug("Setting desiredState:{} on service:{}", desiredState,
        lifecycleAware);

    Supervisoree supervisoree = supervisedProcesses.get(lifecycleAware);
    supervisoree.status.desiredState = desiredState;
  }

  @Override
  public synchronized LifecycleState getLifecycleState() {
    return lifecycleState;
  }

  public synchronized boolean isComponentInErrorState(LifecycleAware component) {
    return supervisedProcesses.get(component).status.error;

  }
  public static class MonitorRunnable implements Runnable {
    // TODO: 2016/11/15 tiny - 10 ~ 20线程池，monitorService
    public ScheduledExecutorService monitorService;
      // TODO: 2016/11/15 tiny - 组件（component）
    public LifecycleAware lifecycleAware;
      // TODO: 2016/11/15 tiny - 组件期望状态
    public Supervisoree supervisoree;

      // TODO: 2016/11/15 tiny - 根据supervisoree对组件进行start/stop操作
    @Override
    public void run() {
        // TODO: 2016/11/15 tiny - PollingPropertiesFileConfigurationProvider
        // TODO: 2016/11/15 tiny - PollingZooKeeperConfigurationProvider
      logger.debug("checking process:{} supervisoree:{}", lifecycleAware,
          supervisoree);

      long now = System.currentTimeMillis();

      try {
        if (supervisoree.status.firstSeen == null) {
          logger.debug("first time seeing {}", lifecycleAware);
            // TODO: 2016/11/15 tiny - 首次启动
          supervisoree.status.firstSeen = now;
        }

        supervisoree.status.lastSeen = now;
        synchronized (lifecycleAware) {
          if (supervisoree.status.discard) {
            // Unsupervise has already been called on this.
            logger.info("Component has already been stopped {}", lifecycleAware);
            return;
          } else if (supervisoree.status.error) {
            logger.info("Component {} is in error state, and Flume will not"
                + "attempt to change its state", lifecycleAware);
            return;
          }

          supervisoree.status.lastSeenState = lifecycleAware.getLifecycleState();

            // TODO: 2016/11/15 tiny - 当前状态 != 期望状态 ? execute : do nothing
          if (!lifecycleAware.getLifecycleState().equals(
              supervisoree.status.desiredState)) {

            logger.debug("Want to transition {} from {} to {} (failures:{})",
                new Object[] { lifecycleAware, supervisoree.status.lastSeenState,
                    supervisoree.status.desiredState,
                    supervisoree.status.failures });

            switch (supervisoree.status.desiredState) {
              case START:
                try {
                  // TODO: 2016/11/15 tiny - start
                  lifecycleAware.start();
                } catch (Throwable e) {
                    // TODO: 2016/11/15 tiny - catch 任何可捕获的异常
                  logger.error("Unable to start " + lifecycleAware
                      + " - Exception follows.", e);
                  if (e instanceof Error) {
                    // This component can never recover, shut it down.
                    supervisoree.status.desiredState = LifecycleState.STOP;
                    try {
                      lifecycleAware.stop();
                      logger.warn("Component {} stopped, since it could not be"
                          + "successfully started due to missing dependencies",
                          lifecycleAware);
                    } catch (Throwable e1) {
                      logger.error("Unsuccessful attempt to "
                          + "shutdown component: {} due to missing dependencies."
                          + " Please shutdown the agent"
                          + "or disable this component, or the agent will be"
                          + "in an undefined state.", e1);
                      supervisoree.status.error = true;
                      if (e1 instanceof Error) {
                        throw (Error) e1;
                      }
                      // Set the state to stop, so that the conf poller can
                      // proceed.
                    }
                  }
                  // TODO: 2016/11/15 tiny - 失败次数
                  supervisoree.status.failures++;
                }
                break;
              case STOP:
                try {
                  // TODO: 2016/11/15 tiny - stop
                  lifecycleAware.stop();
                } catch (Throwable e) {
                  logger.error("Unable to stop " + lifecycleAware
                      + " - Exception follows.", e);
                  if (e instanceof Error) {
                    throw (Error) e;
                  }
                  supervisoree.status.failures++;
                }
                break;
              default:
                logger.warn("I refuse to acknowledge {} as a desired state",
                    supervisoree.status.desiredState);
            }
            // TODO: 2016/11/15 tiny - policy 默认为AlwaysRestartPolicy
            if (!supervisoree.policy.isValid(lifecycleAware, supervisoree.status)) {
              logger.error(
                  "Policy {} of {} has been violated - supervisor should exit!",
                  supervisoree.policy, lifecycleAware);
            }
          }
        }
      } catch (Throwable t) {
        logger.error("Unexpected error", t);
      }
      logger.debug("Status check complete");
    }
  }

  private class Purger implements Runnable {

    @Override
    public void run() {
      if (needToPurge) {
        monitorService.purge();
        needToPurge = false;
      }
    }
  }

  public static class Status {
    public Long firstSeen;
    public Long lastSeen;
    public LifecycleState lastSeenState;
    public LifecycleState desiredState;
    public int failures;
    public boolean discard;
    public volatile boolean error;

    @Override
    public String toString() {
      return "{ lastSeen:" + lastSeen + " lastSeenState:" + lastSeenState
          + " desiredState:" + desiredState + " firstSeen:" + firstSeen
          + " failures:" + failures + " discard:" + discard + " error:" +
          error + " }";
    }

  }

  public abstract static class SupervisorPolicy {

    abstract boolean isValid(LifecycleAware object, Status status);

    public static class AlwaysRestartPolicy extends SupervisorPolicy {

      @Override
      boolean isValid(LifecycleAware object, Status status) {
        return true;
      }
    }

    public static class OnceOnlyPolicy extends SupervisorPolicy {

      @Override
      boolean isValid(LifecycleAware object, Status status) {
        return status.failures == 0;
      }
    }

  }

  private static class Supervisoree {

    public SupervisorPolicy policy;
    public Status status;

    @Override
    public String toString() {
      return "{ status:" + status + " policy:" + policy + " }";
    }

  }

}
