/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.tuple;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketches;

abstract class ArrayOfDoublesSketchEvaluator extends GenericUDAFEvaluator {

  protected static final String NOMINAL_NUM_ENTRIES_FIELD = "nominalEntries";
  protected static final String NUM_VALUES_FIELD = "numValues";
  protected static final String SKETCH_FIELD = "sketch";

  protected PrimitiveObjectInspector nominalNumEntriesInspector_;
  protected StructObjectInspector intermediateInspector_;

  @Override
  public Object terminatePartial(final @SuppressWarnings("deprecation") AggregationBuffer buf)
      throws HiveException {
    final ArrayOfDoublesState state = (ArrayOfDoublesState) buf;
    final ArrayOfDoublesSketch intermediate = state.getResult();
    if (intermediate == null) { return null; }
    final byte[] bytes = intermediate.toByteArray();
    return Arrays.asList(
      new IntWritable(state.getNominalNumEntries()),
      new IntWritable(state.getNumValues()),
      new BytesWritable(bytes)
    );
  }

  @Override
  public void merge(final @SuppressWarnings("deprecation") AggregationBuffer buf, final Object data)
      throws HiveException {
    if (data == null) { return; }
    final ArrayOfDoublesUnionState state = (ArrayOfDoublesUnionState) buf;
    if (!state.isInitialized()) {
      initializeState(state, data);
    }
    final BytesWritable serializedSketch =
        (BytesWritable) intermediateInspector_.getStructFieldData(
            data, intermediateInspector_.getStructFieldRef(SKETCH_FIELD));
    state.update(ArrayOfDoublesSketches.wrapSketch(Memory.wrap(serializedSketch.getBytes())));
  }

  private void initializeState(final ArrayOfDoublesUnionState state, final Object data) {
    final int nominalNumEntries = ((IntWritable) intermediateInspector_.getStructFieldData(
        data, intermediateInspector_.getStructFieldRef(NOMINAL_NUM_ENTRIES_FIELD))).get();
    final int numValues = ((IntWritable) intermediateInspector_.getStructFieldData(
        data, intermediateInspector_.getStructFieldRef(NUM_VALUES_FIELD))).get();
    state.init(nominalNumEntries, numValues);
  }

  @Override
  public Object terminate(final @SuppressWarnings("deprecation") AggregationBuffer buf)
      throws HiveException {
    final ArrayOfDoublesState state = (ArrayOfDoublesState) buf;
    if (state == null) { return null; }
    final ArrayOfDoublesSketch result = state.getResult();
    if (result == null) { return null; }
    return new BytesWritable(result.toByteArray());
  }

  @Override
  public void reset(@SuppressWarnings("deprecation") final AggregationBuffer buf)
      throws HiveException {
    final ArrayOfDoublesState state = (ArrayOfDoublesState) buf;
    state.reset();
  }

}
