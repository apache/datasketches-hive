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

package org.apache.datasketches.hive.theta;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Union;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

class UnionState extends AbstractAggregationBuffer {

  static final float DEFAULT_SAMPLING_PROBABILITY = 1;

  private int nominalEntries_;
  private long seed_;
  private Union union_;

  public boolean isInitialized() {
    return this.union_ != null;
  }

  // sampling probability is not relevant for merging
  public void init(final int nominalEntries, final long seed) {
    init(nominalEntries, UnionState.DEFAULT_SAMPLING_PROBABILITY, seed);
  }

  public void init(final int nominalEntries, final float samplingProbability, final long seed) {
    this.nominalEntries_ = nominalEntries;
    this.seed_ = seed;
    this.union_ = SetOperation.builder().setNominalEntries(nominalEntries).setP(samplingProbability)
        .setSeed(seed).buildUnion();
  }

  public int getNominalEntries() {
    return this.nominalEntries_;
  }

  public long getSeed() {
    return this.seed_;
  }

  public void update(final Memory mem) {
    this.union_.union(mem);
  }

  public void update(final Object value, final PrimitiveObjectInspector objectInspector) {
    switch (objectInspector.getPrimitiveCategory()) {
    case BINARY:
      this.union_.update(PrimitiveObjectInspectorUtils.getBinary(value, objectInspector).copyBytes());
      return;
    case BYTE:
      this.union_.update(PrimitiveObjectInspectorUtils.getByte(value, objectInspector));
      return;
    case DOUBLE:
      this.union_.update(PrimitiveObjectInspectorUtils.getDouble(value, objectInspector));
      return;
    case FLOAT:
      this.union_.update(PrimitiveObjectInspectorUtils.getFloat(value, objectInspector));
      return;
    case INT:
      this.union_.update(PrimitiveObjectInspectorUtils.getInt(value, objectInspector));
      return;
    case LONG:
      this.union_.update(PrimitiveObjectInspectorUtils.getLong(value, objectInspector));
      return;
    case STRING:
      this.union_.update(PrimitiveObjectInspectorUtils.getString(value, objectInspector));
      return;
    case CHAR:
      this.union_.update(PrimitiveObjectInspectorUtils.getHiveChar(value, objectInspector).getValue());
      return;
    case VARCHAR:
      this.union_.update(PrimitiveObjectInspectorUtils.getHiveVarchar(value, objectInspector).getValue());
      return;
    default:
      throw new IllegalArgumentException(
        "Unrecongnized input data type " + value.getClass().getSimpleName() + " category "
        + objectInspector.getPrimitiveCategory() + ", please use data of the following types: "
        + "byte, double, float, int, long, char, varchar or string.");
    }
  }

  public Sketch getResult() {
    if (this.union_ == null) { return null; }
    return this.union_.getResult();
  }

  public void reset() {
    this.union_ = null;
  }

}
