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

package org.apache.spark.status.api.v1.streaming

import javax.ws.rs.{Path, PathParam}

import org.apache.spark.status.api.v1.UIRootFromServletContext

@Path("/v1")
private[v1] class ApiStreamingApp extends UIRootFromServletContext {

  @Path("applications/{appId}/streaming")
  def getStreamingRoot(@PathParam("appId") appId: String): ApiStreamingRootResource = {
    uiRoot.withSparkUI(appId, None) { ui =>
      new ApiStreamingRootResource(ui)
    }
  }

  @Path("applications/{appId}/{attemptId}/streaming")
  def getStreamingRoot(
      @PathParam("appId") appId: String,
      @PathParam("attemptId") attemptId: String): ApiStreamingRootResource = {
    uiRoot.withSparkUI(appId, Some(attemptId)) { ui =>
      new ApiStreamingRootResource(ui)
    }
  }
}
