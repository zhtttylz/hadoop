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
package org.apache.hadoop.oncrpc;

import org.apache.hadoop.oncrpc.RpcDeniedReply.RejectState;
import org.apache.hadoop.oncrpc.RpcReply.ReplyState;
import org.apache.hadoop.oncrpc.security.VerifierNone;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test for {@link RpcDeniedReply}
 */
public class TestRpcDeniedReply {
  @Test
  public void testRejectStateFromValue() {
    assertEquals(RejectState.RPC_MISMATCH, RejectState.fromValue(0));
    assertEquals(RejectState.AUTH_ERROR, RejectState.fromValue(1));
  }

  @Test
  public void testRejectStateFromInvalidValue1() {
    assertThrows(IndexOutOfBoundsException.class, () ->
        RejectState.fromValue(2));
  }

  @Test
  public void testConstructor() {
    RpcDeniedReply reply = new RpcDeniedReply(0, ReplyState.MSG_ACCEPTED,
        RejectState.AUTH_ERROR, new VerifierNone());
    assertEquals(0, reply.getXid());
    assertEquals(RpcMessage.Type.RPC_REPLY, reply.getMessageType());
    assertEquals(ReplyState.MSG_ACCEPTED, reply.getState());
    assertEquals(RejectState.AUTH_ERROR, reply.getRejectState());
  }
}
