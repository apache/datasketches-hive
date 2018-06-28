/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.hll;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.TgtHllType;

// This class implements functionality common to DataToSketch and UnionSketch

abstract class SketchEvaluator extends GenericUDAFEvaluator {

  static final int DEFAULT_LG_K = 12;
  static final TgtHllType DEFAULT_HLL_TYPE = TgtHllType.HLL_4;

  protected static final String LG_K_FIELD = "lgK";
  protected static final String HLL_TYPE_FIELD = "type";
  protected static final String SKETCH_FIELD = "sketch";

  protected PrimitiveObjectInspector inputInspector_;
  protected PrimitiveObjectInspector lgKInspector_;
  protected PrimitiveObjectInspector hllTypeInspector_;
  protected StructObjectInspector intermediateInspector_;

  @SuppressWarnings("deprecation")
  @Override
  public AggregationBuffer getNewAggregationBuffer() throws HiveException {
    return new State();
  }

  @Override
  public Object terminatePartial(final @SuppressWarnings("deprecation") AggregationBuffer buf)
      throws HiveException {
    final State state = (State) buf;
    final HllSketch intermediate = state.getResult();
    if (intermediate == null) { return null; }
    final byte[] bytes = intermediate.toCompactByteArray();
    return Arrays.asList(
      new IntWritable(state.getLgK()),
      new Text(state.getType().toString()),
      new BytesWritable(bytes)
    );
  }

  @Override
  public void merge(final @SuppressWarnings("deprecation") AggregationBuffer buf, final Object data)
      throws HiveException {
    if (data == null) { return; }
    final State state = (State) buf;
    if (!state.isInitialized()) {
      initializeState(state, data);
    }
    final BytesWritable serializedSketch = (BytesWritable) intermediateInspector_.getStructFieldData(
        data, intermediateInspector_.getStructFieldRef(SKETCH_FIELD));
    state.update(HllSketch.wrap(Memory.wrap(serializedSketch.getBytes())));
  }

  private void initializeState(final State state, final Object data) {
    final int lgK = ((IntWritable) intermediateInspector_.getStructFieldData(
        data, intermediateInspector_.getStructFieldRef(LG_K_FIELD))).get();
    final TgtHllType type = TgtHllType.valueOf(((Text) intermediateInspector_.getStructFieldData(
        data, intermediateInspector_.getStructFieldRef(HLL_TYPE_FIELD))).toString());
    state.init(lgK, type);
  }

  @Override
  public Object terminate(final @SuppressWarnings("deprecation") AggregationBuffer buf)
      throws HiveException {
    if (buf == null) { return null; }
    final State state = (State) buf;
    final HllSketch result = state.getResult();
    if (result == null) { return null; }
    return new BytesWritable(result.toCompactByteArray());
  }

  @Override
  public void reset(@SuppressWarnings("deprecation") final AggregationBuffer buf)
      throws HiveException {
    final State state = (State) buf;
    state.reset();
  }

}
