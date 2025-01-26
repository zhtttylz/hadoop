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
package org.apache.hadoop.mapreduce.v2.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.EventType;
import org.apache.hadoop.mapreduce.jobhistory.EventWriter;
import org.apache.hadoop.mapreduce.jobhistory.HistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEventHandler;
import org.apache.hadoop.mapreduce.jobhistory.JobInitedEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobUnsuccessfulCompletionEvent;
import org.apache.hadoop.mapreduce.split.JobSplitWriter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.commit.CommitterEvent;
import org.apache.hadoop.mapreduce.v2.app.commit.CommitterEventHandler;
import org.apache.hadoop.mapreduce.v2.app.job.JobStateInternal;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.rm.RMHeartbeatHandler;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.event.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestMRAppMaster {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestMRAppMaster.class);
  private static final Path TEST_ROOT_DIR =
      new Path(System.getProperty("test.build.data", "target/test-dir"));
  private static final Path testDir = new Path(TEST_ROOT_DIR,
      TestMRAppMaster.class.getName() + "-tmpDir");
  static String stagingDir = new Path(testDir, "staging").toString();
  private static FileContext localFS = null;

  @BeforeAll
  public static void setup() throws AccessControlException,
      FileNotFoundException, IllegalArgumentException, IOException {
    //Do not error out if metrics are inited multiple times
    DefaultMetricsSystem.setMiniClusterMode(true);
    File dir = new File(stagingDir);
    stagingDir = dir.getAbsolutePath();
    localFS = FileContext.getLocalFSFileContext();
    localFS.delete(testDir, true);
    new File(testDir.toString()).mkdir();
  }

  @BeforeEach
  public void prepare() throws IOException {
    File dir = new File(stagingDir);
    if(dir.exists()) {
      FileUtils.deleteDirectory(dir);
    }
    dir.mkdirs();
  }

  @AfterAll
  public static void cleanup() throws IOException {
    localFS.delete(testDir, true);
  }

  @Test
  public void testMRAppMasterForDifferentUser() throws IOException,
      InterruptedException {
    String applicationAttemptIdStr = "appattempt_1317529182569_0004_000001";
    String containerIdStr = "container_1317529182569_0004_000001_1";

    String userName = "TestAppMasterUser";
    ApplicationAttemptId applicationAttemptId = ApplicationAttemptId.fromString(
        applicationAttemptIdStr);
    ContainerId containerId = ContainerId.fromString(containerIdStr);
    MRAppMasterTest appMaster =
        new MRAppMasterTest(applicationAttemptId, containerId, "host", -1, -1,
            System.currentTimeMillis());
    JobConf conf = new JobConf();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, stagingDir);
    MRAppMaster.initAndStartAppMaster(appMaster, conf, userName);
    Path userPath = new Path(stagingDir, userName);
    Path userStagingPath = new Path(userPath, ".staging");
    assertEquals(userStagingPath.toString(),
      appMaster.stagingDirPath.toString());
  }

  @Test
  public void testMRAppMasterMidLock() throws IOException,
      InterruptedException {
    String applicationAttemptIdStr = "appattempt_1317529182569_0004_000002";
    String containerIdStr = "container_1317529182569_0004_000002_1";
    String userName = "TestAppMasterUser";
    JobConf conf = new JobConf();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, stagingDir);
    conf.setInt(org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.
        FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
    ApplicationAttemptId applicationAttemptId = ApplicationAttemptId.fromString(
        applicationAttemptIdStr);
    JobId jobId =  TypeConverter.toYarn(
        TypeConverter.fromYarn(applicationAttemptId.getApplicationId()));
    Path start = MRApps.getStartJobCommitFile(conf, userName, jobId);
    FileSystem fs = FileSystem.get(conf);
    //Create the file, but no end file so we should unregister with an error.
    fs.create(start).close();
    ContainerId containerId = ContainerId.fromString(containerIdStr);
    MRAppMaster appMaster =
        new MRAppMasterTest(applicationAttemptId, containerId, "host", -1, -1,
            System.currentTimeMillis(), false, false);
    boolean caught = false;
    try {
      MRAppMaster.initAndStartAppMaster(appMaster, conf, userName);
    } catch (IOException e) {
      //The IO Exception is expected
      LOG.info("Caught expected Exception", e);
      caught = true;
    }
    assertTrue(caught);
    assertTrue(appMaster.errorHappenedShutDown);
    assertEquals(JobStateInternal.ERROR, appMaster.forcedState);
    appMaster.stop();

    // verify the final status is FAILED
    verifyFailedStatus((MRAppMasterTest)appMaster, "FAILED");
  }

  @Test
  public void testMRAppMasterJobLaunchTime() throws IOException,
      InterruptedException {
    String applicationAttemptIdStr = "appattempt_1317529182569_0004_000002";
    String containerIdStr = "container_1317529182569_0004_000002_1";
    String userName = "TestAppMasterUser";
    JobConf conf = new JobConf();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, stagingDir);
    conf.setInt(MRJobConfig.NUM_REDUCES, 0);
    conf.set(JHAdminConfig.MR_HS_JHIST_FORMAT, "json");
    ApplicationAttemptId applicationAttemptId = ApplicationAttemptId.fromString(
        applicationAttemptIdStr);
    JobId jobId = TypeConverter.toYarn(
        TypeConverter.fromYarn(applicationAttemptId.getApplicationId()));

    File dir = new File(MRApps.getStagingAreaDir(conf, userName).toString(),
        jobId.toString());
    dir.mkdirs();
    File historyFile = new File(JobHistoryUtils.getStagingJobHistoryFile(
        new Path(dir.toURI().toString()), jobId,
        (applicationAttemptId.getAttemptId() - 1)).toUri().getRawPath());
    historyFile.createNewFile();
    FSDataOutputStream out = new FSDataOutputStream(
        new FileOutputStream(historyFile), null);
    EventWriter writer = new EventWriter(out, EventWriter.WriteMode.JSON);
    writer.close();
    FileSystem fs = FileSystem.get(conf);
    JobSplitWriter.createSplitFiles(new Path(dir.getAbsolutePath()), conf,
        fs, new org.apache.hadoop.mapred.InputSplit[0]);
    ContainerId containerId = ContainerId.fromString(containerIdStr);
    MRAppMasterTestLaunchTime appMaster =
        new MRAppMasterTestLaunchTime(applicationAttemptId, containerId,
            "host", -1, -1, System.currentTimeMillis());
    MRAppMaster.initAndStartAppMaster(appMaster, conf, userName);
    appMaster.stop();
    assertTrue(
           appMaster.jobLaunchTime.get() >= 0, "Job launch time should not be negative.");
  }

  @Test
  public void testMRAppMasterSuccessLock() throws IOException,
      InterruptedException {
    String applicationAttemptIdStr = "appattempt_1317529182569_0004_000002";
    String containerIdStr = "container_1317529182569_0004_000002_1";
    String userName = "TestAppMasterUser";
    JobConf conf = new JobConf();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, stagingDir);
    ApplicationAttemptId applicationAttemptId = ApplicationAttemptId.fromString(
        applicationAttemptIdStr);
    JobId jobId =  TypeConverter.toYarn(
        TypeConverter.fromYarn(applicationAttemptId.getApplicationId()));
    Path start = MRApps.getStartJobCommitFile(conf, userName, jobId);
    Path end = MRApps.getEndJobCommitSuccessFile(conf, userName, jobId);
    FileSystem fs = FileSystem.get(conf);
    fs.create(start).close();
    fs.create(end).close();
    ContainerId containerId = ContainerId.fromString(containerIdStr);
    MRAppMaster appMaster =
        new MRAppMasterTest(applicationAttemptId, containerId, "host", -1, -1,
            System.currentTimeMillis(), false, false);
    boolean caught = false;
    try {
      MRAppMaster.initAndStartAppMaster(appMaster, conf, userName);
    } catch (IOException e) {
      //The IO Exception is expected
      LOG.info("Caught expected Exception", e);
      caught = true;
    }
    assertTrue(caught);
    assertTrue(appMaster.errorHappenedShutDown);
    assertEquals(JobStateInternal.SUCCEEDED, appMaster.forcedState);
    appMaster.stop();

    // verify the final status is SUCCEEDED
    verifyFailedStatus((MRAppMasterTest)appMaster, "SUCCEEDED");
  }

  @Test
  public void testMRAppMasterFailLock() throws IOException,
      InterruptedException {
    String applicationAttemptIdStr = "appattempt_1317529182569_0004_000002";
    String containerIdStr = "container_1317529182569_0004_000002_1";
    String userName = "TestAppMasterUser";
    JobConf conf = new JobConf();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, stagingDir);
    ApplicationAttemptId applicationAttemptId = ApplicationAttemptId.fromString(
        applicationAttemptIdStr);
    JobId jobId =  TypeConverter.toYarn(
        TypeConverter.fromYarn(applicationAttemptId.getApplicationId()));
    Path start = MRApps.getStartJobCommitFile(conf, userName, jobId);
    Path end = MRApps.getEndJobCommitFailureFile(conf, userName, jobId);
    FileSystem fs = FileSystem.get(conf);
    fs.create(start).close();
    fs.create(end).close();
    ContainerId containerId = ContainerId.fromString(containerIdStr);
    MRAppMaster appMaster =
        new MRAppMasterTest(applicationAttemptId, containerId, "host", -1, -1,
            System.currentTimeMillis(), false, false);
    boolean caught = false;
    try {
      MRAppMaster.initAndStartAppMaster(appMaster, conf, userName);
    } catch (IOException e) {
      //The IO Exception is expected
      LOG.info("Caught expected Exception", e);
      caught = true;
    }
    assertTrue(caught);
    assertTrue(appMaster.errorHappenedShutDown);
    assertEquals(JobStateInternal.FAILED, appMaster.forcedState);
    appMaster.stop();

    // verify the final status is FAILED
    verifyFailedStatus((MRAppMasterTest)appMaster, "FAILED");
  }

  @Test
  public void testMRAppMasterMissingStaging() throws IOException,
      InterruptedException {
    String applicationAttemptIdStr = "appattempt_1317529182569_0004_000002";
    String containerIdStr = "container_1317529182569_0004_000002_1";
    String userName = "TestAppMasterUser";
    JobConf conf = new JobConf();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, stagingDir);
    ApplicationAttemptId applicationAttemptId = ApplicationAttemptId.fromString(
        applicationAttemptIdStr);

    //Delete the staging directory
    File dir = new File(stagingDir);
    if(dir.exists()) {
      FileUtils.deleteDirectory(dir);
    }

    ContainerId containerId = ContainerId.fromString(containerIdStr);
    MRAppMaster appMaster =
        new MRAppMasterTest(applicationAttemptId, containerId, "host", -1, -1,
            System.currentTimeMillis(), false, false);
    boolean caught = false;
    try {
      MRAppMaster.initAndStartAppMaster(appMaster, conf, userName);
    } catch (IOException e) {
      //The IO Exception is expected
      LOG.info("Caught expected Exception", e);
      caught = true;
    }
    assertTrue(caught);
    assertTrue(appMaster.errorHappenedShutDown);
    //Copying the history file is disabled, but it is not really visible from 
    //here
    assertEquals(JobStateInternal.ERROR, appMaster.forcedState);
    appMaster.stop();
  }

  @Test
  @Timeout(value = 30)
  public void testMRAppMasterMaxAppAttempts() throws IOException,
      InterruptedException {
    // No matter what's the maxAppAttempt or attempt id, the isLastRetry always
    // equals to false
    Boolean[] expectedBools = new Boolean[]{ false, false, false };

    String applicationAttemptIdStr = "appattempt_1317529182569_0004_000002";
    String containerIdStr = "container_1317529182569_0004_000002_1";

    String userName = "TestAppMasterUser";
    ApplicationAttemptId applicationAttemptId = ApplicationAttemptId.fromString(
        applicationAttemptIdStr);
    ContainerId containerId = ContainerId.fromString(containerIdStr);
    JobConf conf = new JobConf();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, stagingDir);

    File stagingDir =
        new File(MRApps.getStagingAreaDir(conf, userName).toString());
    stagingDir.mkdirs();
    for (int i = 0; i < expectedBools.length; ++i) {
      MRAppMasterTest appMaster =
          new MRAppMasterTest(applicationAttemptId, containerId, "host", -1, -1,
              System.currentTimeMillis(), false, true);
      MRAppMaster.initAndStartAppMaster(appMaster, conf, userName);
      assertEquals(expectedBools[i]
,           appMaster.isLastAMRetry(), "isLastAMRetry is correctly computed.");
    }
  }

  // A dirty hack to modify the env of the current JVM itself - Dirty, but
  // should be okay for testing.
  @SuppressWarnings({ "rawtypes", "unchecked" })
  private static void setNewEnvironmentHack(Map<String, String> newenv)
      throws Exception {
    try {
      Class<?> cl = Class.forName("java.lang.ProcessEnvironment");
      Field field = cl.getDeclaredField("theEnvironment");
      field.setAccessible(true);
      Map<String, String> env = (Map<String, String>) field.get(null);
      env.clear();
      env.putAll(newenv);
      Field ciField = cl.getDeclaredField("theCaseInsensitiveEnvironment");
      ciField.setAccessible(true);
      Map<String, String> cienv = (Map<String, String>) ciField.get(null);
      cienv.clear();
      cienv.putAll(newenv);
    } catch (NoSuchFieldException e) {
      Class[] classes = Collections.class.getDeclaredClasses();
      Map<String, String> env = System.getenv();
      for (Class cl : classes) {
        if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
          Field field = cl.getDeclaredField("m");
          field.setAccessible(true);
          Object obj = field.get(env);
          Map<String, String> map = (Map<String, String>) obj;
          map.clear();
          map.putAll(newenv);
        }
      }
    }
  }

  @Test
  public void testMRAppMasterCredentials() throws Exception {

    GenericTestUtils.setRootLogLevel(Level.DEBUG);

    // Simulate credentials passed to AM via client->RM->NM
    Credentials credentials = new Credentials();
    byte[] identifier = "MyIdentifier".getBytes();
    byte[] password = "MyPassword".getBytes();
    Text kind = new Text("MyTokenKind");
    Text service = new Text("host:port");
    Token<? extends TokenIdentifier> myToken =
        new Token<>(identifier, password, kind, service);
    Text tokenAlias = new Text("myToken");
    credentials.addToken(tokenAlias, myToken);

    Text appTokenService = new Text("localhost:0");
    Token<AMRMTokenIdentifier> appToken =
        new Token<>(identifier, password,
        AMRMTokenIdentifier.KIND_NAME, appTokenService);
    credentials.addToken(appTokenService, appToken);

    Text keyAlias = new Text("mySecretKeyAlias");
    credentials.addSecretKey(keyAlias, "mySecretKey".getBytes());
    Token<? extends TokenIdentifier> storedToken =
        credentials.getToken(tokenAlias);

    JobConf conf = new JobConf();

    Path tokenFilePath = new Path(testDir, "tokens-file");
    Map<String, String> newEnv = new HashMap<>();
    newEnv.put(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION, tokenFilePath
      .toUri().getPath());
    setNewEnvironmentHack(newEnv);
    credentials.writeTokenStorageFile(tokenFilePath, conf);

    ApplicationId appId = ApplicationId.newInstance(12345, 56);
    ApplicationAttemptId applicationAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId containerId =
        ContainerId.newContainerId(applicationAttemptId, 546);
    String userName = UserGroupInformation.getCurrentUser().getShortUserName();

    // Create staging dir, so MRAppMaster doesn't barf.
    File stagingDir =
        new File(MRApps.getStagingAreaDir(conf, userName).toString());
    stagingDir.mkdirs();

    // Set login-user to null as that is how real world MRApp starts with.
    // This is null is the reason why token-file is read by UGI.
    UserGroupInformation.setLoginUser(null);

    MRAppMasterTest appMaster =
        new MRAppMasterTest(applicationAttemptId, containerId, "host", -1, -1,
          System.currentTimeMillis(), false, true);
    MRAppMaster.initAndStartAppMaster(appMaster, conf, userName);

    // Now validate the task credentials
    Credentials appMasterCreds = appMaster.getCredentials();
    Assertions.assertNotNull(appMasterCreds);
    Assertions.assertEquals(1, appMasterCreds.numberOfSecretKeys());
    Assertions.assertEquals(1, appMasterCreds.numberOfTokens());

    // Validate the tokens - app token should not be present
    Token<? extends TokenIdentifier> usedToken =
        appMasterCreds.getToken(tokenAlias);
    Assertions.assertNotNull(usedToken);
    Assertions.assertEquals(storedToken, usedToken);

    // Validate the keys
    byte[] usedKey = appMasterCreds.getSecretKey(keyAlias);
    Assertions.assertNotNull(usedKey);
    Assertions.assertEquals("mySecretKey", new String(usedKey));

    // The credentials should also be added to conf so that OuputCommitter can
    // access it - app token should not be present
    Credentials confCredentials = conf.getCredentials();
    Assertions.assertEquals(1, confCredentials.numberOfSecretKeys());
    Assertions.assertEquals(1, confCredentials.numberOfTokens());
    Assertions.assertEquals(storedToken, confCredentials.getToken(tokenAlias));
    Assertions.assertEquals("mySecretKey",
      new String(confCredentials.getSecretKey(keyAlias)));

    // Verify the AM's ugi - app token should be present
    Credentials ugiCredentials = appMaster.getUgi().getCredentials();
    Assertions.assertEquals(1, ugiCredentials.numberOfSecretKeys());
    Assertions.assertEquals(2, ugiCredentials.numberOfTokens());
    Assertions.assertEquals(storedToken, ugiCredentials.getToken(tokenAlias));
    Assertions.assertEquals(appToken, ugiCredentials.getToken(appTokenService));
    Assertions.assertEquals("mySecretKey",
      new String(ugiCredentials.getSecretKey(keyAlias)));


  }

  @Test
  public void testMRAppMasterShutDownJob() throws Exception,
      InterruptedException {
    String applicationAttemptIdStr = "appattempt_1317529182569_0004_000002";
    String containerIdStr = "container_1317529182569_0004_000002_1";
    String userName = "TestAppMasterUser";
    ApplicationAttemptId applicationAttemptId = ApplicationAttemptId.fromString(
        applicationAttemptIdStr);
    ContainerId containerId = ContainerId.fromString(containerIdStr);
    JobConf conf = new JobConf();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, stagingDir);

    File stagingDir =
        new File(MRApps.getStagingAreaDir(conf, userName).toString());
    stagingDir.mkdirs();
    MRAppMasterTest appMaster =
        spy(new MRAppMasterTest(applicationAttemptId, containerId, "host", -1, -1,
            System.currentTimeMillis(), false, true));
    MRAppMaster.initAndStartAppMaster(appMaster, conf, userName);
    doReturn(conf).when(appMaster).getConfig();
    appMaster.isLastAMRetry = true;
    doNothing().when(appMaster).serviceStop();
    // Test normal shutdown.
    appMaster.shutDownJob();
    Assertions.assertTrue(
                     ExitUtil.terminateCalled(), "Expected shutDownJob to terminate.");
    Assertions.assertEquals(
       0, ExitUtil.getFirstExitException().status, "Expected shutDownJob to exit with status code of 0.");

    // Test shutdown with exception.
    ExitUtil.resetFirstExitException();
    String msg = "Injected Exception";
    doThrow(new RuntimeException(msg))
            .when(appMaster).notifyIsLastAMRetry(anyBoolean());
    appMaster.shutDownJob();
    assertTrue(
       ExitUtil.getFirstExitException().getMessage().contains(msg), "Expected message from ExitUtil.ExitException to be " + msg);
    Assertions.assertEquals(
       1, ExitUtil.getFirstExitException().status, "Expected shutDownJob to exit with status code of 1.");
  }

  private void verifyFailedStatus(MRAppMasterTest appMaster,
      String expectedJobState) {
    ArgumentCaptor<JobHistoryEvent> captor = ArgumentCaptor
        .forClass(JobHistoryEvent.class);
    // handle two events: AMStartedEvent and JobUnsuccessfulCompletionEvent
    verify(appMaster.spyHistoryService, times(2))
        .handleEvent(captor.capture());
    HistoryEvent event = captor.getValue().getHistoryEvent();
    assertTrue(event instanceof JobUnsuccessfulCompletionEvent);
    assertThat(((JobUnsuccessfulCompletionEvent) event).getStatus())
        .isEqualTo(expectedJobState);
  }
}
class MRAppMasterTest extends MRAppMaster {

  Path stagingDirPath;
  private Configuration conf;
  private boolean overrideInit;
  private boolean overrideStart;
  ContainerAllocator mockContainerAllocator;
  CommitterEventHandler mockCommitterEventHandler;
  RMHeartbeatHandler mockRMHeartbeatHandler;
  JobHistoryEventHandler spyHistoryService;

  public MRAppMasterTest(ApplicationAttemptId applicationAttemptId,
      ContainerId containerId, String host, int port, int httpPort,
      long submitTime) {
    this(applicationAttemptId, containerId, host, port, httpPort,
        submitTime, true, true);
  }
  public MRAppMasterTest(ApplicationAttemptId applicationAttemptId,
      ContainerId containerId, String host, int port, int httpPort,
      long submitTime, boolean overrideInit,
      boolean overrideStart) {
    super(applicationAttemptId, containerId, host, port, httpPort, submitTime);
    this.overrideInit = overrideInit;
    this.overrideStart = overrideStart;
    mockContainerAllocator = mock(ContainerAllocator.class);
    mockCommitterEventHandler = mock(CommitterEventHandler.class);
    mockRMHeartbeatHandler = mock(RMHeartbeatHandler.class);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    if (!overrideInit) {
      super.serviceInit(conf);
    }
    this.conf = conf;
  }

  @Override
  protected ContainerAllocator createContainerAllocator(
      final ClientService clientService, final AppContext context) {
    return mockContainerAllocator;
  }

  @Override
  protected EventHandler<CommitterEvent> createCommitterEventHandler(
      AppContext context, OutputCommitter committer) {
    return mockCommitterEventHandler;
  }

  @Override
  protected RMHeartbeatHandler getRMHeartbeatHandler() {
    return mockRMHeartbeatHandler;
  }

  @Override
  protected void serviceStart() throws Exception {
    if (overrideStart) {
      try {
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        String user = ugi.getShortUserName();
        stagingDirPath = MRApps.getStagingAreaDir(conf, user);
      } catch (Exception e) {
        fail(e.getMessage());
      }
    } else {
      super.serviceStart();
    }
  }

  @Override
  public Credentials getCredentials() {
    return super.getCredentials();
  }

  public UserGroupInformation getUgi() {
    return currentUser;
  }

  @Override
  protected EventHandler<JobHistoryEvent> createJobHistoryHandler(
      AppContext context) {
    spyHistoryService =
        Mockito.spy((JobHistoryEventHandler) super
            .createJobHistoryHandler(context));
    spyHistoryService.setForcejobCompletion(this.isLastAMRetry);
    return spyHistoryService;
  }
}

class MRAppMasterTestLaunchTime extends MRAppMasterTest {
  final AtomicLong jobLaunchTime = new AtomicLong(0L);
  public MRAppMasterTestLaunchTime(ApplicationAttemptId applicationAttemptId,
      ContainerId containerId, String host, int port, int httpPort,
      long submitTime) {
    super(applicationAttemptId, containerId, host, port, httpPort,
        submitTime, false, false);
  }

  @Override
  protected EventHandler<CommitterEvent> createCommitterEventHandler(
      AppContext context, OutputCommitter committer) {
    return new CommitterEventHandler(context, committer,
        getRMHeartbeatHandler()) {
      @Override
      public void handle(CommitterEvent event) {
      }
    };
  }

  @Override
  protected EventHandler<JobHistoryEvent> createJobHistoryHandler(
      AppContext context) {
    return new JobHistoryEventHandler(context, getStartCount()) {
      @Override
      public void handle(JobHistoryEvent event) {
        if (event.getHistoryEvent().getEventType() == EventType.JOB_INITED) {
          JobInitedEvent jie = (JobInitedEvent) event.getHistoryEvent();
          jobLaunchTime.set(jie.getLaunchTime());
        }
        super.handle(event);
      }
    };
  }
}