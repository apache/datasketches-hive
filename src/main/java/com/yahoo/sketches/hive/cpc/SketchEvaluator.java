/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.cpc;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.cpc.CpcSketch;

// This class implements functionality common to DataToSketch and UnionSketch

abstract class SketchEvaluator extends GenericUDAFEvaluator {

  static final int DEFAULT_LG_K = 11;

  protected static final String LG_K_FIELD = "lgK";
  protected static final String SEED_FIELD = "seed";
  protected static final String SKETCH_FIELD = "sketch";

  protected PrimitiveObjectInspector inputInspector_;
  protected PrimitiveObjectInspector lgKInspector_;
  protected PrimitiveObjectInspector seedInspector_;
  protected StructObjectInspector intermediateInspector_;

  @Override
  public Object terminatePartial(final @SuppressWarnings("deprecation") AggregationBuffer buf)
      throws HiveException {
    final State state = (State) buf;
    final CpcSketch intermediate = state.getResult();
    if (intermediate == null) { return null; }
    final byte[] bytes = intermediate.toByteArray();
    return Arrays.asList(
      new IntWritable(state.getLgK()),
      new LongWritable(state.getSeed()),
      new BytesWritable(bytes)
    );
  }

  @Override
  public void merge(final @SuppressWarnings("deprecation") AggregationBuffer buf, final Object data)
      throws HiveException {
    if (data == null) { return; }
    final UnionState state = (UnionState) buf;
    if (!state.isInitialized()) {
      initializeState(state, data);
    }
    final BytesWritable serializedSketch = (BytesWritable) intermediateInspector_.getStructFieldData(
        data, intermediateInspector_.getStructFieldRef(SKETCH_FIELD));
    state.update(CpcSketch.heapify(Memory.wrap(serializedSketch.getBytes()), state.getSeed()));
  }

  private void initializeState(final UnionState state, final Object data) {
    final int lgK = ((IntWritable) intermediateInspector_.getStructFieldData(
        data, intermediateInspector_.getStructFieldRef(LG_K_FIELD))).get();
    final long seed = ((LongWritable) intermediateInspector_.getStructFieldData(
        data, intermediateInspector_.getStructFieldRef(SEED_FIELD))).get();
    state.init(lgK, seed);
  }

  @Override
  public Object terminate(final @SuppressWarnings("deprecation") AggregationBuffer buf)
      throws HiveException {
    final State state = (State) buf;
    if (state == null) { return null; }
    final CpcSketch result = state.getResult();
    if (result == null) { return null; }
    return new BytesWritable(result.toByteArray());
  }

  @Override
  public void reset(@SuppressWarnings("deprecation") final AggregationBuffer buf)
      throws HiveException {
    final State state = (State) buf;
    state.reset();
  }

}
