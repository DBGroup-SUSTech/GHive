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
package org.apache.hive.ptest.execution.ssh;

public class SSHCommand extends AbstractSSHCommand<SSHResult> {

  private final SSHCommandExecutor executor;
  private final String command;
  private final boolean reportErrors;

  public SSHCommand(SSHCommandExecutor executor, String privateKey,
      String user, String host, int instance, String command, boolean reportErrors) {
    super(privateKey, user, host, instance);
    this.executor = executor;
    this.command = command;
    this.reportErrors = reportErrors;
  }

  @Override
  public SSHResult call() {
    executor.execute(this);
    return new SSHResult(getUser(), getHost(), getInstance(), getCommand(),
        getExitCode(), getException(), getOutput());
  }

  public boolean isReportErrors() {
    return reportErrors;
  }

  public String getCommand() {
    return command;
  }

  @Override
  public String toString() {
    return "SSHCommand [command=" + command + ", getHost()=" + getHost()
        + ", getInstance()=" + getInstance() + "]";
  }
}