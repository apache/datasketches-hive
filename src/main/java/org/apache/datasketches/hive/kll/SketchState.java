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

package org.apache.datasketches.hive.kll;

import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;

class SketchState extends AbstractAggregationBuffer {

  private KllFloatsSketch state_;

  // initialization is needed in the first phase (iterate) only
  void init() {
    state_ = new KllFloatsSketch();
  }

  void init(final int k) {
    state_ = new KllFloatsSketch(k);
  }

  boolean isInitialized() {
    return state_ != null;
  }

  void update(final float value) {
    state_.update(value);
  }

  void update(final KllFloatsSketch sketch) {
    if (state_ == null) {
      state_ = sketch;
    } else {
      state_.merge(sketch);
    }
  }

  public KllFloatsSketch getResult() {
    return state_;
  }

  void reset() {
    state_ = null;
  }

}
