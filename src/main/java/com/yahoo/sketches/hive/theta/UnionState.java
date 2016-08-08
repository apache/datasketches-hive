/*******************************************************************************
 * Copyright 2016, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 *******************************************************************************/
package com.yahoo.sketches.hive.theta;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Union;

class UnionState extends AbstractAggregationBuffer {

  static final float DEFAULT_SAMPLING_PROBABILITY = 1;

  private int nominalEntries_;
  private long seed_;
  private Union union_;

  public boolean isInitialized() {
    return union_ != null;
  }

  // sampling probability is not relevant for merging
  public void init(final int nominalEntries, final long seed) {
    init(nominalEntries, UnionState.DEFAULT_SAMPLING_PROBABILITY, seed);
  }

  public void init(final int nominalEntries, final float samplingProbability, final long seed) {
    this.nominalEntries_ = nominalEntries;
    this.seed_ = seed;
    union_ = SetOperation.builder().setNominalEntries(nominalEntries).setP(samplingProbability)
        .setSeed(seed).buildUnion();
  }

  public int getNominalEntries() {
    return nominalEntries_;
  }

  public long getSeed() {
    return seed_;
  }

  public void update(final NativeMemory mem) {
    union_.update(mem);
  }

  public void update(final Object value, final PrimitiveObjectInspector objectInspector) {
    switch (objectInspector.getPrimitiveCategory()) {
    case BINARY:
      union_.update(PrimitiveObjectInspectorUtils.getBinary(value, objectInspector).getBytes());
      return;
    case BYTE:
      union_.update(PrimitiveObjectInspectorUtils.getByte(value, objectInspector));
      return;
    case DOUBLE:
      union_.update(PrimitiveObjectInspectorUtils.getDouble(value, objectInspector));
      return;
    case FLOAT:
      union_.update(PrimitiveObjectInspectorUtils.getFloat(value, objectInspector));
      return;
    case INT:
      union_.update(PrimitiveObjectInspectorUtils.getInt(value, objectInspector));
      return;
    case LONG:
      union_.update(PrimitiveObjectInspectorUtils.getLong(value, objectInspector));
      return;
    case STRING:
      union_.update(PrimitiveObjectInspectorUtils.getString(value, objectInspector));
      return;
    default:
      throw new IllegalArgumentException(
          "Unrecongnized input data type, please use data of type binary, byte, double, float, int, long, or string only.");
    }
  }

  public Sketch getResult() {
    if (union_ == null) return null;
    return union_.getResult();
  }

  public void reset() {
    union_ = null;
  }

}
