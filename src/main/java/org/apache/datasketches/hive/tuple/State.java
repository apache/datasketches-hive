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
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;

abstract class State<S extends Summary> extends AbstractAggregationBuffer {

  private int nominalNumEntries_;

  void init(final int nominalNumEntries) {
    nominalNumEntries_ = nominalNumEntries;
  }

  int getNominalNumEntries() {
    return nominalNumEntries_;
  }

  abstract Sketch<S> getResult();

  abstract void reset();

}
