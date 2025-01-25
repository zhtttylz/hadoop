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

package org.apache.hadoop.mapreduce.v2.hs;

import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.mapreduce.v2.api.records.Phase;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestCompletedTask {

  @Test
  @Timeout(value = 5)
  public void testTaskStartTimes() {
    
    TaskId taskId = mock(TaskId.class); 
    TaskInfo taskInfo = mock(TaskInfo.class);
    Map<TaskAttemptID, TaskAttemptInfo> taskAttempts = new TreeMap<>();
    
    TaskAttemptID id = new TaskAttemptID("0", 0, TaskType.MAP, 0, 0);
    TaskAttemptInfo info = mock(TaskAttemptInfo.class);
    when(info.getAttemptId()).thenReturn(id);
    when(info.getStartTime()).thenReturn(10L);
    taskAttempts.put(id, info);
    
    id = new TaskAttemptID("1", 0, TaskType.MAP, 1, 1);
    info = mock(TaskAttemptInfo.class);
    when(info.getAttemptId()).thenReturn(id);
    when(info.getStartTime()).thenReturn(20L);
    taskAttempts.put(id, info);
    
    
    when(taskInfo.getAllTaskAttempts()).thenReturn(taskAttempts);
    CompletedTask task = new CompletedTask(taskId, taskInfo);
    TaskReport report = task.getReport();

    // Make sure the startTime returned by report is the lesser of the 
    // attempt launch times
    assertEquals(10, report.getStartTime());
  }
  /**
   * test some methods of CompletedTaskAttempt
   */
  @Test
  @Timeout(value = 5)
  public void testCompletedTaskAttempt(){
    
    TaskAttemptInfo attemptInfo= mock(TaskAttemptInfo.class);
    when(attemptInfo.getRackname()).thenReturn("Rackname");
    when(attemptInfo.getShuffleFinishTime()).thenReturn(11L);
    when(attemptInfo.getSortFinishTime()).thenReturn(12L);
    when(attemptInfo.getShufflePort()).thenReturn(10);
    
    JobID jobId= new JobID("12345",0);
    TaskID taskId =new TaskID(jobId,TaskType.REDUCE, 0);
    TaskAttemptID taskAttemptId= new TaskAttemptID(taskId, 0);
    when(attemptInfo.getAttemptId()).thenReturn(taskAttemptId);

    CompletedTaskAttempt taskAttempt = new CompletedTaskAttempt(null, attemptInfo);
    assertEquals("Rackname", taskAttempt.getNodeRackName());
    assertEquals(Phase.CLEANUP, taskAttempt.getPhase());
    assertTrue(taskAttempt.isFinished());
    assertEquals(11L, taskAttempt.getShuffleFinishTime());
    assertEquals(12L, taskAttempt.getSortFinishTime());
    assertEquals(10, taskAttempt.getShufflePort());
  }
}
