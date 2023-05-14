/*
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

package org.apache.hadoop.fs.store;

import java.io.IOException;

import org.junit.Test;

import org.apache.hadoop.fs.FSBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.impl.AbstractFSBuilderImpl;
import org.apache.hadoop.fs.impl.FSBuilderSupport;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test builder support, forwarding of opt double/float to long,
 * resilience.
 */
@SuppressWarnings("deprecation")
public class TestFSBuilderSupport extends AbstractHadoopTestBase {

  @Test
  public void testOptFloatDoubleForwardsToLong() throws Throwable {
    FSBuilderSupport c = builder()
        .opt("f", 1.8f)
        .opt("d", 2.0e3)
        .build();
    assertThat(c.getLong("f", 2))
        .isEqualTo(1);
    assertThat(c.getLong("d", 2))
        .isEqualTo(2000);
  }

  @Test
  public void testMustFloatDoubleForwardsToLong() throws Throwable {
    FSBuilderSupport c = builder()
        .must("f", 1.8f)
        .must("d", 2.0e3)
        .build();
    assertThat(c.getLong("f", 2))
        .isEqualTo(1);
    assertThat(c.getLong("d", 2))
        .isEqualTo(2000);
  }

  @Test
  public void testLongOptStillWorks() throws Throwable {
    FSBuilderSupport c = builder()
        .opt("o", 1L)
        .must("m", 1L)
        .build();
    assertThat(c.getLong("o", 2))
        .isEqualTo(1L);
    assertThat(c.getLong("m", 2))
        .isEqualTo(1L);
  }

  @Test
  public void testFloatParseFallback() throws Throwable {
    FSBuilderSupport c = builder()
        .opt("f", "1.8f")
        .opt("d", "1.8e20")
        .build();

    assertThat(c.getLong("f", 2))
        .isEqualTo(2);
    assertThat(c.getLong("d", 2))
        .isEqualTo(2);
  }

  @Test
  public void testNegatives() throws Throwable {
    FSBuilderSupport c = builder()
        .optLong("-1", -1)
        .mustLong("-2", -2)
        .build();

    // getLong gets the long value
    assertThat(c.getLong("-1", 2))
        .isEqualTo(-1);


    // but getPositiveLong returns the positive default
    assertThat(c.getPositiveLong("-1", 2))
        .isEqualTo(2);
  }

  @Test
  public void testBoolean() throws Throwable {
    final FSBuilderSupport c = builder()
        .opt("f", false)
        .opt("t", true)
        .opt("o", "other")
        .build();
    assertThat(c.getOptions().getBoolean("f", true))
        .isEqualTo(false);
    assertThat(c.getOptions().getBoolean("t", false))
        .isEqualTo(true);
    // this is handled in Configuration itself.
    assertThat(c.getOptions().getBoolean("o", true))
        .isEqualTo(true);
  }

  private SimpleBuilder builder() {
    return new BuilderImpl();
  }

  private interface SimpleBuilder
      extends FSBuilder<FSBuilderSupport, SimpleBuilder> {
  }

  private static final class BuilderImpl
      extends AbstractFSBuilderImpl<FSBuilderSupport, SimpleBuilder>
      implements SimpleBuilder {

    private BuilderImpl() {
      super(new Path("/"));
    }

    @Override
    public FSBuilderSupport build()
        throws IOException {
      return new FSBuilderSupport(getOptions());
    }
  }
}
