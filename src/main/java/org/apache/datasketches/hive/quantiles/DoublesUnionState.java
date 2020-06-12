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

package org.apache.datasketches.hive.quantiles;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.DoublesUnion;
import org.apache.datasketches.quantiles.DoublesUnionBuilder;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;

class DoublesUnionState extends AbstractAggregationBuffer {

  private DoublesUnion union;

  // initializing is needed only in the first phase (iterate)
  void init(final int k) {
    final DoublesUnionBuilder unionBuilder = DoublesUnion.builder();
    if (k > 0) { unionBuilder.setMaxK(k); }
    union = unionBuilder.build();
  }

  boolean isInitialized() {
    return union != null;
  }

  void update(final double value) {
    if (union == null) {
      union = DoublesUnion.builder().build();
    }
    union.update(value);
  }

  void update(final Memory serializedSketch) {
    final DoublesSketch incomingSketch = DoublesSketch.wrap(serializedSketch);
    if (union == null) {
      union = DoublesUnion.heapify(incomingSketch);
    } else {
      union.update(incomingSketch);
    }
  }

  public DoublesSketch getResult() {
    if (union == null) { return null; }
    return union.getResultAndReset();
  }

  void reset() {
    union = null;
  }

}
