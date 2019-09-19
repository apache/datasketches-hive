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

package org.apache.datasketches.hive.tuple;

import org.apache.datasketches.tuple.Sketch;
import org.apache.datasketches.tuple.Summary;
import org.apache.datasketches.tuple.SummarySetOperations;
import org.apache.datasketches.tuple.Union;

class UnionState<S extends Summary> extends State<S> {

  private Union<S> union_;

  boolean isInitialized() {
    return union_ != null;
  }

  void init(final int nominalNumEntries, final SummarySetOperations<S> summarySetOps) {
    super.init(nominalNumEntries);
    union_ = new Union<S>(nominalNumEntries, summarySetOps);
  }

  void update(final Sketch<S> sketch) {
    union_.update(sketch);
  }

  @Override
  Sketch<S> getResult() {
    if (union_ == null) { return null; }
    return union_.getResult();
  }

  @Override
  void reset() {
    union_ = null;
  }

}
