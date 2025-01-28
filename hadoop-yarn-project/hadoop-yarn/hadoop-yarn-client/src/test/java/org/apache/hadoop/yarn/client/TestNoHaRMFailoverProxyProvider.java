/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.yarn.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DefaultNoHARMFailoverProxyProvider} and
 * {@link AutoRefreshNoHARMFailoverProxyProvider}.
 */
public class TestNoHaRMFailoverProxyProvider {

  // Default port of yarn RM
  private static final int RM1_PORT = 8032;
  private static final int RM2_PORT = 8031;

  private static final int NUMNODEMANAGERS = 1;
  private Configuration conf;

  private class TestProxy extends Proxy implements Closeable {
    protected TestProxy(InvocationHandler h) {
      super(h);
    }

    @Override
    public void close() throws IOException {
    }
  }

  @BeforeEach
  public void setUp() throws IOException, YarnException {
    conf = new YarnConfiguration();
  }

  /**
   * Tests the proxy generated by {@link DefaultNoHAFailoverProxyProvider}
   * will connect to RM.
   */
  @Test
  public void testRestartedRM() throws Exception {
    MiniYARNCluster cluster =
        new MiniYARNCluster("testRestartedRMNegative", NUMNODEMANAGERS, 1, 1);
    YarnClient rmClient = YarnClient.createYarnClient();
    try {
      cluster.init(conf);
      cluster.start();
      final Configuration yarnConf = cluster.getConfig();
      rmClient = YarnClient.createYarnClient();
      rmClient.init(yarnConf);
      rmClient.start();
      List <NodeReport> nodeReports = rmClient.getNodeReports();
      assertEquals(
      
         NUMNODEMANAGERS, nodeReports.size(), "The proxy didn't get expected number of node reports");
    } finally {
      if (rmClient != null) {
        rmClient.stop();
      }
      cluster.stop();
    }
  }

  /**
   * Tests the proxy generated by
   * {@link AutoRefreshNoHARMFailoverProxyProvider} will connect to RM.
   */
  @Test
  public void testConnectingToRM() throws Exception {
    conf.setClass(YarnConfiguration.CLIENT_FAILOVER_NO_HA_PROXY_PROVIDER,
        AutoRefreshNoHARMFailoverProxyProvider.class,
        RMFailoverProxyProvider.class);
    MiniYARNCluster cluster =
        new MiniYARNCluster("testRestartedRMNegative", NUMNODEMANAGERS, 1, 1);
    YarnClient rmClient = null;
    try {
      cluster.init(conf);
      cluster.start();
      final Configuration yarnConf = cluster.getConfig();
      rmClient = YarnClient.createYarnClient();
      rmClient.init(yarnConf);
      rmClient.start();
      List <NodeReport> nodeReports = rmClient.getNodeReports();
      assertEquals(
      
         NUMNODEMANAGERS, nodeReports.size(), "The proxy didn't get expected number of node reports");
    } finally {
      if (rmClient != null) {
        rmClient.stop();
      }
      cluster.stop();
    }
  }

  /**
   * Test that the {@link DefaultNoHARMFailoverProxyProvider}
   * will generate different proxies after RM IP changed
   * and {@link DefaultNoHARMFailoverProxyProvider#performFailover(Object)}
   * get called.
   */
  @Test
  public void testDefaultFPPGetOneProxy() throws Exception {
    // Create a proxy and mock a RMProxy
    Proxy mockProxy1 = new TestProxy((proxy, method, args) -> null);
    Class protocol = ApplicationClientProtocol.class;
    RMProxy<Proxy> mockRMProxy = mock(RMProxy.class);
    DefaultNoHARMFailoverProxyProvider<Proxy> fpp =
        new DefaultNoHARMFailoverProxyProvider<>();

    InetSocketAddress mockAdd1 = new InetSocketAddress(RM1_PORT);

    // Mock RMProxy methods
    when(mockRMProxy.getRMAddress(any(YarnConfiguration.class),
      any(Class.class))).thenReturn(mockAdd1);
    when(mockRMProxy.getProxy(any(YarnConfiguration.class),
      any(Class.class), eq(mockAdd1))).thenReturn(mockProxy1);

    // Initialize failover proxy provider and get proxy from it.
    fpp.init(conf, mockRMProxy, protocol);
    FailoverProxyProvider.ProxyInfo<Proxy> actualProxy1 = fpp.getProxy();
    assertEquals(
    
       mockProxy1, actualProxy1.proxy, "AutoRefreshRMFailoverProxyProvider doesn't generate " +
        "expected proxy");

    // Invoke fpp.getProxy() multiple times and
    // validate the returned proxy is always mockProxy1
    actualProxy1 = fpp.getProxy();
    assertEquals(
    
       mockProxy1, actualProxy1.proxy, "AutoRefreshRMFailoverProxyProvider doesn't generate " +
        "expected proxy");
    actualProxy1 = fpp.getProxy();
    assertEquals(
    
       mockProxy1, actualProxy1.proxy, "AutoRefreshRMFailoverProxyProvider doesn't generate " +
        "expected proxy");

    // verify that mockRMProxy.getProxy() is invoked once only.
    verify(mockRMProxy, times(1))
        .getProxy(any(YarnConfiguration.class), any(Class.class),
        eq(mockAdd1));

    // Perform Failover and get proxy again from failover proxy provider
    fpp.performFailover(actualProxy1.proxy);
    FailoverProxyProvider.ProxyInfo<Proxy> actualProxy2 = fpp.getProxy();
    assertEquals(
       mockProxy1, actualProxy2.proxy, "AutoRefreshRMFailoverProxyProvider " +
        "doesn't generate expected proxy after failover");

    // verify that mockRMProxy.getProxy() didn't get invoked again after
    // performFailover()
    verify(mockRMProxy, times(1))
        .getProxy(any(YarnConfiguration.class), any(Class.class),
        eq(mockAdd1));
  }

  /**
   * Test that the {@link AutoRefreshNoHARMFailoverProxyProvider}
   * will generate different proxies after RM IP changed
   * and {@link AutoRefreshNoHARMFailoverProxyProvider#performFailover(Object)}
   * get called.
   */
  @Test
  public void testAutoRefreshIPChange() throws Exception {
    conf.setClass(YarnConfiguration.CLIENT_FAILOVER_NO_HA_PROXY_PROVIDER,
        AutoRefreshNoHARMFailoverProxyProvider.class,
        RMFailoverProxyProvider.class);

    // Create two proxies and mock a RMProxy
    Proxy mockProxy1 = new TestProxy((proxy, method, args) -> null);
    Proxy mockProxy2 = new TestProxy((proxy, method, args) -> null);
    Class protocol = ApplicationClientProtocol.class;
    RMProxy<Proxy> mockRMProxy = mock(RMProxy.class);
    AutoRefreshNoHARMFailoverProxyProvider<Proxy> fpp =
        new AutoRefreshNoHARMFailoverProxyProvider<>();

    // generate two address with different ports.
    InetSocketAddress mockAdd1 = new InetSocketAddress(RM1_PORT);
    InetSocketAddress mockAdd2 = new InetSocketAddress(RM2_PORT);

    // Mock RMProxy methods
    when(mockRMProxy.getRMAddress(any(YarnConfiguration.class),
        any(Class.class))).thenReturn(mockAdd1);
    when(mockRMProxy.getProxy(any(YarnConfiguration.class),
        any(Class.class), eq(mockAdd1))).thenReturn(mockProxy1);

    // Initialize proxy provider and get proxy from it.
    fpp.init(conf, mockRMProxy, protocol);
    FailoverProxyProvider.ProxyInfo<Proxy> actualProxy1 = fpp.getProxy();
    assertEquals(
    
       mockProxy1, actualProxy1.proxy, "AutoRefreshRMFailoverProxyProvider doesn't generate " +
        "expected proxy");

    // Invoke fpp.getProxy() multiple times and
    // validate the returned proxy is always mockProxy1
    actualProxy1 = fpp.getProxy();
    assertEquals(
    
       mockProxy1, actualProxy1.proxy, "AutoRefreshRMFailoverProxyProvider doesn't generate " +
        "expected proxy");
    actualProxy1 = fpp.getProxy();
    assertEquals(
    
       mockProxy1, actualProxy1.proxy, "AutoRefreshRMFailoverProxyProvider doesn't generate " +
        "expected proxy");

    // verify that mockRMProxy.getProxy() is invoked once only.
    verify(mockRMProxy, times(1))
        .getProxy(any(YarnConfiguration.class), any(Class.class),
        eq(mockAdd1));

    // Mock RMProxy methods to generate different proxy
    // based on different IP address.
    when(mockRMProxy.getRMAddress(
        any(YarnConfiguration.class),
        any(Class.class))).thenReturn(mockAdd2);
    when(mockRMProxy.getProxy(
        any(YarnConfiguration.class),
        any(Class.class), eq(mockAdd2))).thenReturn(mockProxy2);

    // Perform Failover and get proxy again from failover proxy provider
    fpp.performFailover(actualProxy1.proxy);
    FailoverProxyProvider.ProxyInfo<Proxy> actualProxy2 = fpp.getProxy();
    assertEquals(
       mockProxy2, actualProxy2.proxy, "AutoRefreshNoHARMFailoverProxyProvider " +
        "doesn't generate expected proxy after failover");

    // check the proxy is different with the one we created before.
    assertNotEquals(
       actualProxy1.proxy, actualProxy2.proxy, "AutoRefreshNoHARMFailoverProxyProvider " +
        "shouldn't generate same proxy after failover");
  }
}
