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

package org.apache.hadoop.mapreduce.v2;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

public class TestUberAM extends TestMRJobs {

  private static final Logger LOG = LoggerFactory.getLogger(TestUberAM.class);

  @BeforeAll
  public static void setup() throws IOException {
    TestMRJobs.setup();
    if (mrCluster != null) {
    	mrCluster.getConfig().setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, true);
    	mrCluster.getConfig().setInt(MRJobConfig.JOB_UBERTASK_MAXREDUCES, 3);
    }
  }

  @Override
  @Test
  public void testSleepJob()
  throws Exception {
    numSleepReducers = 1;
    super.testSleepJob();
  }
  
  @Test
  public void testSleepJobWithMultipleReducers()
  throws Exception {
    numSleepReducers = 3;
    super.testSleepJob();
  }
  
  @Override
  protected void verifySleepJobCounters(Job job) throws InterruptedException,
      IOException {
    Counters counters = job.getCounters();
    super.verifySleepJobCounters(job);
    Assertions.assertEquals(3,
        counters.findCounter(JobCounter.NUM_UBER_SUBMAPS).getValue());
    Assertions.assertEquals(numSleepReducers,
        counters.findCounter(JobCounter.NUM_UBER_SUBREDUCES).getValue());
    Assertions.assertEquals(3 + numSleepReducers,
        counters.findCounter(JobCounter.TOTAL_LAUNCHED_UBERTASKS).getValue());
  }

  @Override
  @Test
  public void testRandomWriter()
  throws IOException, InterruptedException, ClassNotFoundException {
    super.testRandomWriter();
  }

  @Override
  protected void verifyRandomWriterCounters(Job job)
      throws InterruptedException, IOException {
    super.verifyRandomWriterCounters(job);
    Counters counters = job.getCounters();
    Assertions.assertEquals(3, counters.findCounter(JobCounter.NUM_UBER_SUBMAPS)
        .getValue());
    Assertions.assertEquals(3,
        counters.findCounter(JobCounter.TOTAL_LAUNCHED_UBERTASKS).getValue());
  }

  @Override
  @Test
  public void testFailingMapper()
  throws IOException, InterruptedException, ClassNotFoundException {
    LOG.info("\n\n\nStarting uberized testFailingMapper().");

    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
               + " not found. Not running test.");
      return;
    }

    Job job = runFailingMapperJob();

    // should be able to get diags for single task attempt...
    TaskID taskID = new TaskID(job.getJobID(), TaskType.MAP, 0);
    TaskAttemptID aId = new TaskAttemptID(taskID, 0);
    System.out.println("Diagnostics for " + aId + " :");
    for (String diag : job.getTaskDiagnostics(aId)) {
      System.out.println(diag);
    }
    // ...but not for second (shouldn't exist:  uber-AM overrode max attempts)
    boolean secondTaskAttemptExists = true;
    try {
      aId = new TaskAttemptID(taskID, 1);
      System.out.println("Diagnostics for " + aId + " :");
      for (String diag : job.getTaskDiagnostics(aId)) {
        System.out.println(diag);
      }
    } catch (Exception e) {
      secondTaskAttemptExists = false;
    }
    assertThat(secondTaskAttemptExists).isFalse();

    TaskCompletionEvent[] events = job.getTaskCompletionEvents(0, 2);
    Assertions.assertEquals(1, events.length);
    // TIPFAILED if it comes from the AM, FAILED if it comes from the JHS
    TaskCompletionEvent.Status status = events[0].getStatus();
    Assertions.assertTrue(status == TaskCompletionEvent.Status.FAILED ||
        status == TaskCompletionEvent.Status.TIPFAILED);
    Assertions.assertEquals(JobStatus.State.FAILED, job.getJobState());
    
    //Disabling till UberAM honors MRJobConfig.MAP_MAX_ATTEMPTS
    //verifyFailingMapperCounters(job);

    // TODO later:  add explicit "isUber()" checks of some sort
  }
  
  @Override
  protected void verifyFailingMapperCounters(Job job)
      throws InterruptedException, IOException {
    Counters counters = job.getCounters();
    super.verifyFailingMapperCounters(job);
    Assertions.assertEquals(2,
        counters.findCounter(JobCounter.TOTAL_LAUNCHED_UBERTASKS).getValue());
    Assertions.assertEquals(2, counters.findCounter(JobCounter.NUM_UBER_SUBMAPS)
        .getValue());
    Assertions.assertEquals(2, counters
        .findCounter(JobCounter.NUM_FAILED_UBERTASKS).getValue());
  }

//@Test  //FIXME:  if/when the corresponding TestMRJobs test gets enabled, do so here as well (potentially with mods for ubermode)
  public void testSleepJobWithSecurityOn()
  throws IOException, InterruptedException, ClassNotFoundException {
    super.testSleepJobWithSecurityOn();
  }
}
