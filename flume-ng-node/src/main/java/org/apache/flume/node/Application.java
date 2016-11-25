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

package org.apache.flume.node;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import org.apache.commons.cli.*;
import org.apache.flume.*;
import org.apache.flume.instrumentation.MonitorService;
import org.apache.flume.instrumentation.MonitoringType;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.lifecycle.LifecycleSupervisor;
import org.apache.flume.lifecycle.LifecycleSupervisor.SupervisorPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

public class Application {

  private static final Logger logger = LoggerFactory
      .getLogger(Application.class);

  public static final String CONF_MONITOR_CLASS = "flume.monitoring.type";
  public static final String CONF_MONITOR_PREFIX = "flume.monitoring.";

  private final List<LifecycleAware> components;
  private final LifecycleSupervisor supervisor;
  private MaterializedConfiguration materializedConfiguration;
  private MonitorService monitorServer;

  public Application() {
    this(new ArrayList<LifecycleAware>(0));
  }

  // TODO: 2016/11/15 tiny - 构造函数，默认用LifecycleSupervisor
  public Application(List<LifecycleAware> components) {
    this.components = components;
    supervisor = new LifecycleSupervisor();
  }

  // TODO: 2016/11/15 tiny -
  public synchronized void start() {
    // TODO: 2016/11/15 tiny - 遍历添加组件列表，启动监督
    for (LifecycleAware component : components) {
      //// TODO: 2016/11/15 tiny - 默认AlwaysRestartPolicy策略
      supervisor.supervise(component,
          new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
    }
  }

  // TODO: 2016/11/17 tiny - EventBus's event callback function
  @Subscribe
  public synchronized void handleConfigurationEvent(MaterializedConfiguration conf) {
    stopAllComponents();
    startAllComponents(conf);
  }

  public synchronized void stop() {
    supervisor.stop();
    if (monitorServer != null) {
      monitorServer.stop();
    }
  }

  private void stopAllComponents() {
    if (this.materializedConfiguration != null) {
      // TODO: 2016/11/17 tiny - stop source,sink,channel
      logger.info("Shutting down configuration: {}", this.materializedConfiguration);
      for (Entry<String, SourceRunner> entry :
           this.materializedConfiguration.getSourceRunners().entrySet()) {
        try {
          logger.info("Stopping Source " + entry.getKey());
          supervisor.unsupervise(entry.getValue());
        } catch (Exception e) {
          logger.error("Error while stopping {}", entry.getValue(), e);
        }
      }

      for (Entry<String, SinkRunner> entry :
           this.materializedConfiguration.getSinkRunners().entrySet()) {
        try {
          logger.info("Stopping Sink " + entry.getKey());
          supervisor.unsupervise(entry.getValue());
        } catch (Exception e) {
          logger.error("Error while stopping {}", entry.getValue(), e);
        }
      }

      for (Entry<String, Channel> entry :
           this.materializedConfiguration.getChannels().entrySet()) {
        try {
          logger.info("Stopping Channel " + entry.getKey());
          supervisor.unsupervise(entry.getValue());
        } catch (Exception e) {
          logger.error("Error while stopping {}", entry.getValue(), e);
        }
      }
    }
    // TODO: 2016/11/17 tiny - stop monitor
    if (monitorServer != null) {
      monitorServer.stop();
    }
  }

  private void startAllComponents(MaterializedConfiguration materializedConfiguration) {
    logger.info("Starting new configuration:{}", materializedConfiguration);
    // TODO: 2016/11/17 tiny - start all components
    this.materializedConfiguration = materializedConfiguration;

    // TODO: 2016/11/17 tiny - start channels
    for (Entry<String, Channel> entry :
        materializedConfiguration.getChannels().entrySet()) {
      try {
        logger.info("Starting Channel " + entry.getKey());
        supervisor.supervise(entry.getValue(),
            new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
      } catch (Exception e) {
        logger.error("Error while starting {}", entry.getValue(), e);
      }
    }

    /*
     * Wait for all channels to start.
     */
    for (Channel ch : materializedConfiguration.getChannels().values()) {
      while (ch.getLifecycleState() != LifecycleState.START
          && !supervisor.isComponentInErrorState(ch)) {
        try {
          logger.info("Waiting for channel: " + ch.getName() +
              " to start. Sleeping for 500 ms");
          Thread.sleep(500);
        } catch (InterruptedException e) {
          logger.error("Interrupted while waiting for channel to start.", e);
          Throwables.propagate(e);
        }
      }
    }

    // TODO: 2016/11/17 tiny - start sinks
    for (Entry<String, SinkRunner> entry : materializedConfiguration.getSinkRunners().entrySet()) {
      try {
        logger.info("Starting Sink " + entry.getKey());
        supervisor.supervise(entry.getValue(),
            new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
      } catch (Exception e) {
        logger.error("Error while starting {}", entry.getValue(), e);
      }
    }

    // TODO: 2016/11/17 tiny - start sources
    for (Entry<String, SourceRunner> entry :
         materializedConfiguration.getSourceRunners().entrySet()) {
      try {
        logger.info("Starting Source " + entry.getKey());
        supervisor.supervise(entry.getValue(),
            new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
      } catch (Exception e) {
        logger.error("Error while starting {}", entry.getValue(), e);
      }
    }

    this.loadMonitoring();
  }

  @SuppressWarnings("unchecked")
  private void loadMonitoring() {
    // TODO: 2016/11/17 tiny - monitor
    // TODO: 2016/11/17 tiny - get system properties
    Properties systemProps = System.getProperties();
    Set<String> keys = systemProps.stringPropertyNames();
    try {
      if (keys.contains(CONF_MONITOR_CLASS)) {
        String monitorType = systemProps.getProperty(CONF_MONITOR_CLASS);
        Class<? extends MonitorService> klass;
        try {
          // TODO: 2016/11/17 tiny - GangliaServer, HTTPMetricsServer
          //Is it a known type?
          klass = MonitoringType.valueOf(
              monitorType.toUpperCase(Locale.ENGLISH)).getMonitorClass();
        } catch (Exception e) {
          //Not a known type, use FQCN
          klass = (Class<? extends MonitorService>) Class.forName(monitorType);
        }
        this.monitorServer = klass.newInstance();
        Context context = new Context();
        for (String key : keys) {
          if (key.startsWith(CONF_MONITOR_PREFIX)) {
            context.put(key.substring(CONF_MONITOR_PREFIX.length()),
                systemProps.getProperty(key));
          }
        }
        // TODO: 2016/11/17 tiny - monitor start
        monitorServer.configure(context);
        monitorServer.start();
      }
    } catch (Exception e) {
      logger.warn("Error starting monitoring. "
          + "Monitoring might not be available.", e);
    }

  }

  /**
   * code reading notes start
   * flume 启动的入口函数
   *
   * @author tiny
   * @email tiny_wcl@163.com
   * @date 2016/11/15 19:47
   */
  public static void main(String[] args) {
    try {

      boolean isZkConfigured = false;

      Options options = new Options();

      Option option = new Option("n", "name", true, "the name of this agent");
      option.setRequired(true);
      options.addOption(option);

      option = new Option("f", "conf-file", true,
          "specify a config file (required if -z missing)");
      option.setRequired(false);
      options.addOption(option);

      option = new Option(null, "no-reload-conf", false,
          "do not reload config file if changed");
      options.addOption(option);

      // Options for Zookeeper
      option = new Option("z", "zkConnString", true,
          "specify the ZooKeeper connection to use (required if -f missing)");
      option.setRequired(false);
      options.addOption(option);

      option = new Option("p", "zkBasePath", true,
          "specify the base path in ZooKeeper for agent configs");
      option.setRequired(false);
      options.addOption(option);

      option = new Option("h", "help", false, "display help text");
      options.addOption(option);

      CommandLineParser parser = new GnuParser();
      CommandLine commandLine = parser.parse(options, args);

      if (commandLine.hasOption('h')) {
        new HelpFormatter().printHelp("flume-ng agent", options, true);
        return;
      }
      // TODO: 2016/11/25 tiny - 启动某个agentName对应的sink, channel, source, etc.
      String agentName = commandLine.getOptionValue('n');
      boolean reload = !commandLine.hasOption("no-reload-conf");

      if (commandLine.hasOption('z') || commandLine.hasOption("zkConnString")) {
        isZkConfigured = true;
      }
      Application application = null;
      if (isZkConfigured) {
        // TODO: 2016/11/15 tiny - 从zookeeper读取配置
        // get options
        String zkConnectionStr = commandLine.getOptionValue('z');
        String baseZkPath = commandLine.getOptionValue('p');
        if (reload) {
          // TODO: 2016/11/15 tiny - 感知配置文件变动
          EventBus eventBus = new EventBus(agentName + "-event-bus");
          List<LifecycleAware> components = Lists.newArrayList();
          PollingZooKeeperConfigurationProvider zookeeperConfigurationProvider =
              new PollingZooKeeperConfigurationProvider(
                  agentName, zkConnectionStr, baseZkPath, eventBus);
          // TODO: 2016/11/15 tiny - 添加配置组件到待启动列表
          components.add(zookeeperConfigurationProvider);
          application = new Application(components);
          // TODO: 2016/11/15 tiny - 注册配置变更事件，用于更新配置
          eventBus.register(application);
        } else {
          // TODO: 2016/11/15 tiny - 只加载一次配置文件
          StaticZooKeeperConfigurationProvider zookeeperConfigurationProvider =
              new StaticZooKeeperConfigurationProvider(
                  agentName, zkConnectionStr, baseZkPath);
          application = new Application();
          // TODO: 2016/11/15 tiny - 手动处理配置，非EventBus
          application.handleConfigurationEvent(zookeeperConfigurationProvider.getConfiguration());
        }
      } else {
        // TODO: 2016/11/15 tiny - 本地配置文件
        File configurationFile = new File(commandLine.getOptionValue('f'));

        /*
         * The following is to ensure that by default the agent will fail on
         * startup if the file does not exist.
         */
        if (!configurationFile.exists()) {
          // If command line invocation, then need to fail fast
          if (System.getProperty(Constants.SYSPROP_CALLED_FROM_SERVICE) ==
              null) {
            String path = configurationFile.getPath();
            try {
              path = configurationFile.getCanonicalPath();
            } catch (IOException ex) {
              logger.error("Failed to read canonical path for file: " + path,
                  ex);
            }
            throw new ParseException(
                "The specified configuration file does not exist: " + path);
          }
        }
        List<LifecycleAware> components = Lists.newArrayList();

        if (reload) {
          EventBus eventBus = new EventBus(agentName + "-event-bus");
          PollingPropertiesFileConfigurationProvider configurationProvider =
              new PollingPropertiesFileConfigurationProvider(
                  agentName, configurationFile, eventBus, 30);
          components.add(configurationProvider);
          application = new Application(components);
          eventBus.register(application);
        } else {
          PropertiesFileConfigurationProvider configurationProvider =
              new PropertiesFileConfigurationProvider(agentName, configurationFile);
          application = new Application();
          application.handleConfigurationEvent(configurationProvider.getConfiguration());
        }
      }
      // TODO: 2016/11/15 tiny - 启动应用，初始化已添加的PropertiesFileConfigurationProvider
      application.start();

      // TODO tiny - shutdown flume
      final Application appReference = application;
      Runtime.getRuntime().addShutdownHook(new Thread("agent-shutdown-hook") {
        @Override
        public void run() {
          appReference.stop();
        }
      });

    } catch (Exception e) {
      logger.error("A fatal error occurred while running. Exception follows.", e);
    }
  }
}