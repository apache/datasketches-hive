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

class State extends AbstractAggregationBuffer {

  static final float DEFAULT_SAMPLING_PROBABILITY = 1;

  private int nominalEntries;
  private long seed;
  private Union union;

  public boolean isInitialized() {
    return union != null;
  }

  // sampling probability is not relevant for merging
  public void init(final int nominalEntries, final long seed) {
    init(nominalEntries, State.DEFAULT_SAMPLING_PROBABILITY, seed);
  }

  public void init(final int nominalEntries, final float samplingProbability, final long seed) {
    this.nominalEntries = nominalEntries;
    this.seed = seed;
    union = SetOperation.builder().setNominalEntries(nominalEntries).setP(samplingProbability)
        .setSeed(seed).buildUnion();
  }

  public int getNominalEntries() {
    return nominalEntries;
  }

  public long getSeed() {
    return seed;
  }

  public void update(final NativeMemory mem) {
    union.update(mem);
  }
  public void update(final Object value, final PrimitiveObjectInspector objectInspector) {
    switch (objectInspector.getPrimitiveCategory()) {
    case BINARY:
      union.update(PrimitiveObjectInspectorUtils.getBinary(value, objectInspector).getBytes());
      return;
    case BYTE:
      union.update(PrimitiveObjectInspectorUtils.getByte(value, objectInspector));
      return;
    case DOUBLE:
      union.update(PrimitiveObjectInspectorUtils.getDouble(value, objectInspector));
      return;
    case FLOAT:
      union.update(PrimitiveObjectInspectorUtils.getFloat(value, objectInspector));
      return;
    case INT:
      union.update(PrimitiveObjectInspectorUtils.getInt(value, objectInspector));
      return;
    case LONG:
      union.update(PrimitiveObjectInspectorUtils.getLong(value, objectInspector));
      return;
    case STRING:
      union.update(PrimitiveObjectInspectorUtils.getString(value, objectInspector));
      return;
    default:
      throw new IllegalArgumentException(
          "Unrecongnized input data type, please use data of type binary, byte, double, float, int, long, or string only.");
    }
  }

  public Sketch getResult() {
    if (union == null) return null;
    return union.getResult();
  }

  public void reset() {
    union = null;
  }

}
