/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.launcher;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


public abstract class AbstractSparkAppHandle implements SparkAppHandle {
  private static final Logger LOG = Logger.getLogger(AbstractSparkAppHandle.class.getName());
  protected final String secret;
  protected final LauncherServer server;
  protected boolean disposed;
  protected List<Listener> listeners;
  protected State state;
  private LauncherConnection connection;
  private String appId;
  protected boolean killIfInterrupted = false;
  private List<String> killArguments = null;
  private String master = null;

  OutputRedirector redirector;



  public AbstractSparkAppHandle(LauncherServer server, String secret) {
    this.server = server;
    this.secret = secret;
    this.state = State.UNKNOWN;
  }

  protected void setKillIfInterrupted(boolean killFlag) {
    this.killIfInterrupted = killFlag;
  }

  @Override
  public synchronized void addListener(Listener l) {
    if (listeners == null) {
      listeners = new ArrayList<>();
    }
    listeners.add(l);
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public String getAppId() {
    return appId;
  }

  @Override
  public void stop() {
    CommandBuilderUtils.checkState(connection != null, "Application is still not connected.");
    try {
      connection.send(new LauncherProtocol.Stop());
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public synchronized void disconnect() {
    if (!disposed) {
      disposed = true;
      if (connection != null) {
        try {
          connection.close();
        } catch (IOException ioe) {
          // no-op.
        }
      }
      server.unregister(this);
      if (redirector != null) {
        redirector.stop();
      }
    }
  }

  String getSecret() {
    return secret;
  }

  void setConnection(LauncherConnection connection) {
    this.connection = connection;
  }

  LauncherServer getServer() {
    return server;
  }

  LauncherConnection getConnection() {
    return connection;
  }

  void setState(State s) {
    if (!state.isFinal()) {
      state = s;
      fireEvent(false);
    } else {
      LOG.log(Level.WARNING, "Backend requested transition from final state {0} to {1}.",
        new Object[]{state, s});
    }
  }

  void setAppId(String appId) {
    this.appId = appId;
    fireEvent(true);
  }

  private synchronized void fireEvent(boolean isInfoChanged) {
    if (listeners != null) {
      for (Listener l : listeners) {
        if (isInfoChanged) {
          l.infoChanged(this);
        } else {
          l.stateChanged(this);
        }
      }
    }
  }


  public void setKillArguments(List<String> killArguments) {
    this.killArguments = killArguments;
  }

  protected boolean killIfInterrupted() {
    return killIfInterrupted;
  }

  protected static Method getYarnApplicationMain() throws ClassNotFoundException, NoSuchMethodException {
    Class<?> cls = Class.forName("org.apache.hadoop.yarn.client.cli.ApplicationCLI");
    return cls.getDeclaredMethod("main", String[].class);
  }

  protected  void killJob() {
    LOG.info("Killing job.. " + master);
    //if(killIfInterrupted && appId != null) {
      killArguments = new ArrayList<>();
      Method main = null;
      if(master != null && master.equals("yarn")) {
        try {
          main = getYarnApplicationMain();
        } catch (ClassNotFoundException clsNotFoundEx) {
          LOG.info("Yarn not in class path." +  clsNotFoundEx.getMessage());
        } catch (NoSuchMethodException noSuchMethodEx) {
          LOG.info("Yarn not in class path." +  noSuchMethodEx.getMessage());
        }
        killArguments.add("application");
        killArguments.add("-kill");
      } else {
        killArguments.add("--kill");
      }
      killArguments.add(appId);
      LOG.info("Killing job.. " + appId);
      Runnable killTask = new SparkSubmitRunner(main, killArguments);
      killTask.run();
      //Thread submitJobThread = new Thread(new SparkSubmitRunner(main, killArguments));
      //submitJobThread.setName("Killing-app-" + appId);
      //submitJobThread.start();
      //submitJobThread.run();
      LOG.info("Killed job.. " + appId);
    //}
  }

  public String getMaster() {
    return master;
  }

  public void setMaster(String master) {
    this.master = master;
  }
}
