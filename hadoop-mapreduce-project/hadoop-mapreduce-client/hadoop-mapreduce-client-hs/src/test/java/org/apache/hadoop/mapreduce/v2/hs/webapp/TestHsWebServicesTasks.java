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

package org.apache.hadoop.mapreduce.v2.hs.webapp;

import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.assertResponseStatusCode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.StringReader;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.hs.HistoryContext;
import org.apache.hadoop.mapreduce.v2.hs.MockHistoryContext;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.util.XMLUtils;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.server.ResourceConfig;

/**
 * Test the history server Rest API for getting tasks, a specific task,
 * and task counters.
 *
 * /ws/v1/history/mapreduce/jobs/{jobid}/tasks
 * /ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}
 * /ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}/counters
 */
public class TestHsWebServicesTasks extends JerseyTestBase {

  private static Configuration conf = new Configuration();
  private static MockHistoryContext appContext;
  private static HsWebApp webApp;
  private static ApplicationClientProtocol acp = mock(ApplicationClientProtocol.class);

  @Override
  protected Application configure() {
    ResourceConfig config = new ResourceConfig();
    config.register(new JerseyBinder());
    config.register(HsWebServices.class);
    config.register(GenericExceptionHandler.class);
    config.register(new JettisonFeature()).register(JAXBContextResolver.class);
    return config;
  }

  private static class JerseyBinder extends AbstractBinder {
    @Override
    protected void configure() {
      appContext = new MockHistoryContext(0, 1, 2, 1);
      webApp = mock(HsWebApp.class);
      when(webApp.name()).thenReturn("hsmockwebapp");

      bind(webApp).to(WebApp.class).named("hsWebApp");
      bind(appContext).to(AppContext.class);
      bind(appContext).to(HistoryContext.class).named("ctx");
      bind(conf).to(Configuration.class).named("conf");
      bind(acp).to(ApplicationClientProtocol.class).named("appClient");
      final HttpServletResponse response = mock(HttpServletResponse.class);
      bind(response).to(HttpServletResponse.class);
      final HttpServletRequest request = mock(HttpServletRequest.class);
      bind(request).to(HttpServletRequest.class);
    }
  }

  @Test
  public void testTasks() throws JSONException, Exception {
    WebTarget r = targetWithJsonObject();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      Response response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId).path("tasks")
          .request(MediaType.APPLICATION_JSON).get(Response.class);

      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      JSONObject json = response.readEntity(JSONObject.class);
      assertEquals(1, json.length(), "incorrect number of elements");
      JSONObject tasks = json.getJSONObject("tasks");
      JSONArray arr = tasks.getJSONArray("task");
      assertEquals(2, arr.length(), "incorrect number of elements");

      verifyHsTask(arr, jobsMap.get(id), null);
    }
  }

  @Test
  public void testTasksDefault() throws JSONException, Exception {
    WebTarget r = targetWithJsonObject();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      Response response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId).path("tasks")
          .request().get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      JSONObject json = response.readEntity(JSONObject.class);
      assertEquals(1, json.length(), "incorrect number of elements");
      JSONObject tasks = json.getJSONObject("tasks");
      JSONArray arr = tasks.getJSONArray("task");
      assertEquals(2, arr.length(), "incorrect number of elements");

      verifyHsTask(arr, jobsMap.get(id), null);
    }
  }

  @Test
  public void testTasksSlash() throws JSONException, Exception {
    WebTarget r = targetWithJsonObject();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      Response response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId).path("tasks/")
          .request(MediaType.APPLICATION_JSON).get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      JSONObject json =response.readEntity(JSONObject.class);
      assertEquals(1, json.length(), "incorrect number of elements");
      JSONObject tasks = json.getJSONObject("tasks");
      JSONArray arr = tasks.getJSONArray("task");
      assertEquals(2, arr.length(), "incorrect number of elements");

      verifyHsTask(arr, jobsMap.get(id), null);
    }
  }

  @Test
  public void testTasksXML() throws JSONException, Exception {

    WebTarget r = target();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      Response response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId).path("tasks")
          .request(MediaType.APPLICATION_XML).get(Response.class);
      assertEquals(MediaType.APPLICATION_XML_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      String xml = response.readEntity(String.class);
      DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
      DocumentBuilder db = dbf.newDocumentBuilder();
      InputSource is = new InputSource();
      is.setCharacterStream(new StringReader(xml));
      Document dom = db.parse(is);
      NodeList tasks = dom.getElementsByTagName("tasks");
      assertEquals(1, tasks.getLength(), "incorrect number of elements");
      NodeList task = dom.getElementsByTagName("task");
      verifyHsTaskXML(task, jobsMap.get(id));
    }
  }

  @Test
  public void testTasksQueryMap() throws JSONException, Exception {
    WebTarget r = targetWithJsonObject();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      String type = "m";
      Response response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId).path("tasks")
          .queryParam("type", type).request(MediaType.APPLICATION_JSON)
          .get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      JSONObject json = response.readEntity(JSONObject.class);
      assertEquals(1, json.length(), "incorrect number of elements");
      JSONObject tasks = json.getJSONObject("tasks");
      JSONObject task = tasks.getJSONObject("task");
      JSONArray arr = new JSONArray();
      arr.put(task);
      assertEquals(1, arr.length(), "incorrect number of elements");
      verifyHsTask(arr, jobsMap.get(id), type);
    }
  }

  @Test
  public void testTasksQueryReduce() throws JSONException, Exception {
    WebTarget r = targetWithJsonObject();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      String type = "r";
      Response response = r.path("ws").path("v1").path("history")
          .path("mapreduce").path("jobs").path(jobId).path("tasks")
          .queryParam("type", type).request(MediaType.APPLICATION_JSON)
          .get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      JSONObject json = response.readEntity(JSONObject.class);
      assertEquals(1, json.length(), "incorrect number of elements");
      JSONObject tasks = json.getJSONObject("tasks");
      JSONObject task = tasks.getJSONObject("task");
      JSONArray arr = new JSONArray();
      arr.put(task);
      assertEquals(1, arr.length(), "incorrect number of elements");
      verifyHsTask(arr, jobsMap.get(id), type);
    }
  }

  @Test
  public void testTasksQueryInvalid() throws JSONException, Exception {
    WebTarget r = targetWithJsonObject();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      // tasktype must be exactly either "m" or "r"
      String tasktype = "reduce";

      try {
        Response response = r.path("ws").path("v1").path("history").path("mapreduce").path("jobs")
            .path(jobId).path("tasks").queryParam("type", tasktype)
            .request(MediaType.APPLICATION_JSON).get();
        throw new BadRequestException(response);
      } catch (BadRequestException ue) {
        Response response = ue.getResponse();
        assertResponseStatusCode(Response.Status.BAD_REQUEST, response.getStatusInfo());
        assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
            response.getMediaType().toString());
        JSONObject msg = response.readEntity(JSONObject.class);
        JSONObject exception = msg.getJSONObject("RemoteException");
        assertEquals(3, exception.length(), "incorrect number of elements");
        String message = exception.getString("message");
        String type = exception.getString("exception");
        String classname = exception.getString("javaClassName");
        WebServicesTestUtils.checkStringMatch("exception message",
            "tasktype must be either m or r", message);
        WebServicesTestUtils.checkStringMatch("exception type",
            "BadRequestException", type);
        WebServicesTestUtils.checkStringMatch("exception classname",
            "org.apache.hadoop.yarn.webapp.BadRequestException", classname);
      }
    }
  }

  @Test
  public void testTaskId() throws JSONException, Exception {
    WebTarget r = targetWithJsonObject();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      for (Task task : jobsMap.get(id).getTasks().values()) {

        String tid = MRApps.toString(task.getID());
        Response response = r.path("ws").path("v1").path("history")
            .path("mapreduce").path("jobs").path(jobId).path("tasks").path(tid)
            .request(MediaType.APPLICATION_JSON)
            .get(Response.class);
        assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
            response.getMediaType().toString());
        JSONObject json = response.readEntity(JSONObject.class);
        assertEquals(1, json.length(), "incorrect number of elements");
        JSONObject info = json.getJSONObject("task");
        verifyHsSingleTask(info, task);
      }
    }
  }

  @Test
  public void testTaskIdSlash() throws JSONException, Exception {
    WebTarget r = targetWithJsonObject();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      for (Task task : jobsMap.get(id).getTasks().values()) {

        String tid = MRApps.toString(task.getID());
        Response response = r.path("ws").path("v1").path("history")
            .path("mapreduce").path("jobs").path(jobId).path("tasks")
            .path(tid + "/").request(MediaType.APPLICATION_JSON)
            .get(Response.class);
        assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
            response.getMediaType().toString());
        JSONObject json = response.readEntity(JSONObject.class);
        assertEquals(1, json.length(), "incorrect number of elements");
        JSONObject info = json.getJSONObject("task");
        verifyHsSingleTask(info, task);
      }
    }
  }

  @Test
  public void testTaskIdDefault() throws Exception {
    WebTarget r = targetWithJsonObject();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      for (Task task : jobsMap.get(id).getTasks().values()) {

        String tid = MRApps.toString(task.getID());
        Response response = r.path("ws").path("v1").path("history")
            .path("mapreduce").path("jobs").path(jobId).path("tasks").path(tid).request()
            .get(Response.class);
        assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
            response.getMediaType().toString());
        JSONObject json = response.readEntity(JSONObject.class);
        assertEquals(1, json.length(), "incorrect number of elements");
        JSONObject info = json.getJSONObject("task");
        verifyHsSingleTask(info, task);
      }
    }
  }

  @Test
  public void testTaskIdBogus() throws JSONException, Exception {
    WebTarget r = targetWithJsonObject();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      String tid = "bogustaskid";
      try {
        Response response = r.path("ws").path("v1").path("history").path("mapreduce").path("jobs")
            .path(jobId).path("tasks").path(tid).request().get();
        throw new NotFoundException(response);
      } catch (NotFoundException ue) {
        Response response = ue.getResponse();
        assertResponseStatusCode(Response.Status.NOT_FOUND, response.getStatusInfo());
        assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
            response.getMediaType().toString());
        JSONObject msg = response.readEntity(JSONObject.class);
        JSONObject exception = msg.getJSONObject("RemoteException");
        assertEquals(3, exception.length(), "incorrect number of elements");
        String message = exception.getString("message");
        String type = exception.getString("exception");
        String classname = exception.getString("javaClassName");
        WebServicesTestUtils.checkStringEqual("exception message",
            "TaskId string : " +
            "bogustaskid is not properly formed" +
            "\nReason: java.util.regex.Matcher[pattern=" +
            TaskID.TASK_ID_REGEX + " region=0,11 lastmatch=]", message);
        WebServicesTestUtils.checkStringMatch("exception type", "NotFoundException", type);
        WebServicesTestUtils.checkStringMatch("exception classname",
            "org.apache.hadoop.yarn.webapp.NotFoundException", classname);
      }
    }
  }

  @Test
  public void testTaskIdNonExist() throws JSONException, Exception {
    WebTarget r = targetWithJsonObject();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      String tid = "task_0_0000_m_000000";
      try {
        Response response = r.path("ws").path("v1").path("history").path("mapreduce").path("jobs")
            .path(jobId).path("tasks").path(tid).request().get();
        throw new NotFoundException(response);
      } catch (NotFoundException ue) {
        Response response = ue.getResponse();
        assertResponseStatusCode(Response.Status.NOT_FOUND, response.getStatusInfo());
        assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
            response.getMediaType().toString());
        JSONObject msg = response.readEntity(JSONObject.class);
        JSONObject exception = msg.getJSONObject("RemoteException");
        assertEquals(3, exception.length(), "incorrect number of elements");
        String message = exception.getString("message");
        String type = exception.getString("exception");
        String classname = exception.getString("javaClassName");
        WebServicesTestUtils.checkStringMatch("exception message",
            "task not found with id task_0_0000_m_000000", message);
        WebServicesTestUtils.checkStringMatch("exception type", "NotFoundException", type);
        WebServicesTestUtils.checkStringMatch("exception classname",
            "org.apache.hadoop.yarn.webapp.NotFoundException", classname);
      }
    }
  }

  @Test
  public void testTaskIdInvalid() throws JSONException, Exception {
    WebTarget r = targetWithJsonObject();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      String tid = "task_0_0000_d_000000";
      try {
        Response response = r.path("ws").path("v1").path("history").path("mapreduce").path("jobs")
            .path(jobId).path("tasks").path(tid).request().get();
        throw new NotFoundException(response);
      } catch (NotFoundException ue) {
        Response response = ue.getResponse();
        assertResponseStatusCode(Response.Status.NOT_FOUND, response.getStatusInfo());
        assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
            response.getMediaType().toString());
        JSONObject msg = response.readEntity(JSONObject.class);
        JSONObject exception = msg.getJSONObject("RemoteException");
        assertEquals(3, exception.length(), "incorrect number of elements");
        String message = exception.getString("message");
        String type = exception.getString("exception");
        String classname = exception.getString("javaClassName");
        WebServicesTestUtils.checkStringEqual("exception message",
            "TaskId string : " +
            "task_0_0000_d_000000 is not properly formed" +
            "\nReason: java.util.regex.Matcher[pattern=" +
            TaskID.TASK_ID_REGEX + " region=0,20 lastmatch=]", message);
        WebServicesTestUtils.checkStringMatch("exception type", "NotFoundException", type);
        WebServicesTestUtils.checkStringMatch("exception classname",
            "org.apache.hadoop.yarn.webapp.NotFoundException", classname);
      }
    }
  }

  @Test
  public void testTaskIdInvalid2() throws JSONException, Exception {
    WebTarget r = targetWithJsonObject();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      String tid = "task_0000_m_000000";
      try {
        Response response = r.path("ws").path("v1").path("history").path("mapreduce").path("jobs")
            .path(jobId).path("tasks").path(tid).request().get();
        throw new NotFoundException(response);
      } catch (NotFoundException ue) {
        Response response = ue.getResponse();
        assertResponseStatusCode(Response.Status.NOT_FOUND, response.getStatusInfo());
        assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
            response.getMediaType().toString());
        JSONObject msg = response.readEntity(JSONObject.class);
        JSONObject exception = msg.getJSONObject("RemoteException");
        assertEquals(3, exception.length(), "incorrect number of elements");
        String message = exception.getString("message");
        String type = exception.getString("exception");
        String classname = exception.getString("javaClassName");
        WebServicesTestUtils.checkStringEqual("exception message",
            "TaskId string : " +
            "task_0000_m_000000 is not properly formed" +
            "\nReason: java.util.regex.Matcher[pattern=" +
            TaskID.TASK_ID_REGEX + " region=0,18 lastmatch=]", message);
        WebServicesTestUtils.checkStringMatch("exception type", "NotFoundException", type);
        WebServicesTestUtils.checkStringMatch("exception classname",
            "org.apache.hadoop.yarn.webapp.NotFoundException", classname);
      }
    }
  }

  @Test
  public void testTaskIdInvalid3() throws JSONException, Exception {
    WebTarget r = targetWithJsonObject();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      String tid = "task_0_0000_m";
      try {
        Response response = r.path("ws").path("v1").path("history").path("mapreduce").path("jobs")
            .path(jobId).path("tasks").path(tid).request().get();
        throw new NotFoundException(response);
      } catch (NotFoundException ue) {
        Response response = ue.getResponse();
        assertResponseStatusCode(Response.Status.NOT_FOUND, response.getStatusInfo());
        assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
            response.getMediaType().toString());
        JSONObject msg = response.readEntity(JSONObject.class);
        JSONObject exception = msg.getJSONObject("RemoteException");
        assertEquals(3, exception.length(), "incorrect number of elements");
        String message = exception.getString("message");
        String type = exception.getString("exception");
        String classname = exception.getString("javaClassName");
        WebServicesTestUtils.checkStringEqual("exception message",
            "TaskId string : " +
            "task_0_0000_m is not properly formed" +
            "\nReason: java.util.regex.Matcher[pattern=" +
            TaskID.TASK_ID_REGEX + " region=0,13 lastmatch=]", message);
        WebServicesTestUtils.checkStringMatch("exception type",
            "NotFoundException", type);
        WebServicesTestUtils.checkStringMatch("exception classname",
            "org.apache.hadoop.yarn.webapp.NotFoundException", classname);
      }
    }
  }

  @Test
  public void testTaskIdXML() throws JSONException, Exception {
    WebTarget r = target();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      for (Task task : jobsMap.get(id).getTasks().values()) {

        String tid = MRApps.toString(task.getID());
        Response response = r.path("ws").path("v1").path("history")
            .path("mapreduce").path("jobs").path(jobId).path("tasks").path(tid)
            .request(MediaType.APPLICATION_XML).get(Response.class);

        assertEquals(MediaType.APPLICATION_XML_TYPE + ";" + JettyUtils.UTF_8,
            response.getMediaType().toString());
        String xml = response.readEntity(String.class);
        DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
        DocumentBuilder db = dbf.newDocumentBuilder();
        InputSource is = new InputSource();
        is.setCharacterStream(new StringReader(xml));
        Document dom = db.parse(is);
        NodeList nodes = dom.getElementsByTagName("task");
        for (int i = 0; i < nodes.getLength(); i++) {
          Element element = (Element) nodes.item(i);
          verifyHsSingleTaskXML(element, task);
        }
      }
    }
  }

  public void verifyHsSingleTask(JSONObject info, Task task)
      throws JSONException {
    assertEquals(9, info.length(), "incorrect number of elements");

    verifyTaskGeneric(task, info.getString("id"), info.getString("state"),
        info.getString("type"), info.getString("successfulAttempt"),
        info.getLong("startTime"), info.getLong("finishTime"),
        info.getLong("elapsedTime"), (float) info.getDouble("progress"));
  }

  public void verifyHsTask(JSONArray arr, Job job, String type)
      throws JSONException {
    for (Task task : job.getTasks().values()) {
      TaskId id = task.getID();
      String tid = MRApps.toString(id);
      boolean found = false;
      if (type != null && task.getType() == MRApps.taskType(type)) {

        for (int i = 0; i < arr.length(); i++) {
          JSONObject info = arr.getJSONObject(i);
          if (tid.matches(info.getString("id"))) {
            found = true;
            verifyHsSingleTask(info, task);
          }
        }
        assertTrue(found, "task with id: " + tid + " not in web service output");
      }
    }
  }

  public void verifyTaskGeneric(Task task, String id, String state,
      String type, String successfulAttempt, long startTime, long finishTime,
      long elapsedTime, float progress) {

    TaskId taskid = task.getID();
    String tid = MRApps.toString(taskid);
    TaskReport report = task.getReport();

    WebServicesTestUtils.checkStringMatch("id", tid, id);
    WebServicesTestUtils.checkStringMatch("type", task.getType().toString(),
        type);
    WebServicesTestUtils.checkStringMatch("state", report.getTaskState()
        .toString(), state);
    // not easily checked without duplicating logic, just make sure its here
    assertNotNull(successfulAttempt, "successfulAttempt null");
    assertEquals(report.getStartTime(), startTime, "startTime wrong");
    assertEquals(report.getFinishTime(), finishTime, "finishTime wrong");
    assertEquals(finishTime - startTime, elapsedTime, "elapsedTime wrong");
    assertEquals(report.getProgress() * 100, progress, 1e-3f, "progress wrong");
  }

  public void verifyHsSingleTaskXML(Element element, Task task) {
    verifyTaskGeneric(task, WebServicesTestUtils.getXmlString(element, "id"),
        WebServicesTestUtils.getXmlString(element, "state"),
        WebServicesTestUtils.getXmlString(element, "type"),
        WebServicesTestUtils.getXmlString(element, "successfulAttempt"),
        WebServicesTestUtils.getXmlLong(element, "startTime"),
        WebServicesTestUtils.getXmlLong(element, "finishTime"),
        WebServicesTestUtils.getXmlLong(element, "elapsedTime"),
        WebServicesTestUtils.getXmlFloat(element, "progress"));
  }

  public void verifyHsTaskXML(NodeList nodes, Job job) {

    assertEquals(2, nodes.getLength(), "incorrect number of elements");

    for (Task task : job.getTasks().values()) {
      TaskId id = task.getID();
      String tid = MRApps.toString(id);
      boolean found = false;
      for (int i = 0; i < nodes.getLength(); i++) {
        Element element = (Element) nodes.item(i);

        if (tid.matches(WebServicesTestUtils.getXmlString(element, "id"))) {
          found = true;
          verifyHsSingleTaskXML(element, task);
        }
      }
      assertTrue(found, "task with id: " + tid + " not in web service output");
    }
  }

  @Test
  public void testTaskIdCounters() throws JSONException, Exception {
    WebTarget r = targetWithJsonObject();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      for (Task task : jobsMap.get(id).getTasks().values()) {

        String tid = MRApps.toString(task.getID());
        Response response = r.path("ws").path("v1").path("history")
            .path("mapreduce").path("jobs").path(jobId).path("tasks").path(tid)
            .path("counters").request(MediaType.APPLICATION_JSON)
            .get(Response.class);
        assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
            response.getMediaType().toString());
        JSONObject json = response.readEntity(JSONObject.class);
        assertEquals(1, json.length(), "incorrect number of elements");
        JSONObject info = json.getJSONObject("jobTaskCounters");
        verifyHsJobTaskCounters(info, task);
      }
    }
  }

  @Test
  public void testTaskIdCountersSlash() throws Exception {
    WebTarget r = targetWithJsonObject();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      for (Task task : jobsMap.get(id).getTasks().values()) {

        String tid = MRApps.toString(task.getID());
        Response response = r.path("ws").path("v1").path("history")
            .path("mapreduce").path("jobs").path(jobId).path("tasks").path(tid)
            .path("counters/").request(MediaType.APPLICATION_JSON)
            .get(Response.class);
        assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
            response.getMediaType().toString());
        JSONObject json = response.readEntity(JSONObject.class);
        assertEquals(1, json.length(), "incorrect number of elements");
        JSONObject info = json.getJSONObject("jobTaskCounters");
        verifyHsJobTaskCounters(info, task);
      }
    }
  }

  @Test
  public void testTaskIdCountersDefault() throws JSONException, Exception {
    WebTarget r = targetWithJsonObject();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      for (Task task : jobsMap.get(id).getTasks().values()) {

        String tid = MRApps.toString(task.getID());
        Response response = r.path("ws").path("v1").path("history")
            .path("mapreduce").path("jobs").path(jobId).path("tasks").path(tid)
            .path("counters").request().get(Response.class);
        assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
            response.getMediaType().toString());
        JSONObject json = response.readEntity(JSONObject.class);
        assertEquals(1, json.length(), "incorrect number of elements");
        JSONObject info = json.getJSONObject("jobTaskCounters");
        verifyHsJobTaskCounters(info, task);
      }
    }
  }

  @Test
  public void testJobTaskCountersXML() throws Exception {
    WebTarget r = target();
    Map<JobId, Job> jobsMap = appContext.getAllJobs();
    for (JobId id : jobsMap.keySet()) {
      String jobId = MRApps.toString(id);
      for (Task task : jobsMap.get(id).getTasks().values()) {

        String tid = MRApps.toString(task.getID());
        Response response = r.path("ws").path("v1").path("history")
            .path("mapreduce").path("jobs").path(jobId).path("tasks").path(tid)
            .path("counters").request(MediaType.APPLICATION_XML)
            .get(Response.class);
        assertEquals(MediaType.APPLICATION_XML_TYPE + ";" + JettyUtils.UTF_8,
            response.getMediaType().toString());
        String xml = response.readEntity(String.class);
        DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
        DocumentBuilder db = dbf.newDocumentBuilder();
        InputSource is = new InputSource();
        is.setCharacterStream(new StringReader(xml));
        Document dom = db.parse(is);
        NodeList info = dom.getElementsByTagName("jobTaskCounters");
        verifyHsTaskCountersXML(info, task);
      }
    }
  }

  public void verifyHsJobTaskCounters(JSONObject info, Task task)
      throws JSONException {

    assertEquals(2, info.length(), "incorrect number of elements");

    WebServicesTestUtils.checkStringMatch("id", MRApps.toString(task.getID()),
        info.getString("id"));
    // just do simple verification of fields - not data is correct
    // in the fields
    JSONArray counterGroups = info.getJSONArray("taskCounterGroup");
    for (int i = 0; i < counterGroups.length(); i++) {
      JSONObject counterGroup = counterGroups.getJSONObject(i);
      String name = counterGroup.getString("counterGroupName");
      assertTrue((name != null && !name.isEmpty()), "name not set");
      JSONArray counters = counterGroup.getJSONArray("counter");
      for (int j = 0; j < counters.length(); j++) {
        JSONObject counter = counters.getJSONObject(j);
        String counterName = counter.getString("name");
        assertTrue(
           (counterName != null && !counterName.isEmpty()), "name not set");
        long value = counter.getLong("value");
        assertTrue(value >= 0, "value  >= 0");
      }
    }
  }

  public void verifyHsTaskCountersXML(NodeList nodes, Task task) {

    for (int i = 0; i < nodes.getLength(); i++) {

      Element element = (Element) nodes.item(i);
      WebServicesTestUtils.checkStringMatch("id",
          MRApps.toString(task.getID()),
          WebServicesTestUtils.getXmlString(element, "id"));
      // just do simple verification of fields - not data is correct
      // in the fields
      NodeList groups = element.getElementsByTagName("taskCounterGroup");

      for (int j = 0; j < groups.getLength(); j++) {
        Element counters = (Element) groups.item(j);
        assertNotNull(counters, "should have counters in the web service info");
        String name = WebServicesTestUtils.getXmlString(counters,
            "counterGroupName");
        assertTrue((name != null && !name.isEmpty()), "name not set");
        NodeList counterArr = counters.getElementsByTagName("counter");
        for (int z = 0; z < counterArr.getLength(); z++) {
          Element counter = (Element) counterArr.item(z);
          String counterName = WebServicesTestUtils.getXmlString(counter,
              "name");
          assertTrue(
             (counterName != null && !counterName.isEmpty()), "counter name not set");

          long value = WebServicesTestUtils.getXmlLong(counter, "value");
          assertTrue(value >= 0, "value not >= 0");

        }
      }
    }
  }

}
