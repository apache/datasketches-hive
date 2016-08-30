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

import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.tuple.Sketch;
import com.yahoo.sketches.tuple.Sketches;
import com.yahoo.sketches.tuple.Summary;
import com.yahoo.sketches.tuple.SummaryFactory;

abstract class SketchEvaluator<S extends Summary> extends GenericUDAFEvaluator {

  protected static final String NUM_NOMINAL_ENTRIES_FIELD = "nominalEntries";
  protected static final String SKETCH_FIELD = "sketch";

  protected final SummaryFactory<S> summaryFactory_;
  protected PrimitiveObjectInspector numNominalEntriesInspector_;
  protected StructObjectInspector intermediateInspector_;

  public SketchEvaluator(final SummaryFactory<S> summaryFactory) {
    summaryFactory_ = summaryFactory;
  }

  @Override
  public Object terminatePartial(final @SuppressWarnings("deprecation") AggregationBuffer buf) throws HiveException {
    @SuppressWarnings("unchecked")
    final State<S> state = (State<S>) buf;
    final Sketch<S> intermediate = state.getResult();
    if (intermediate == null) return null;
    final byte[] bytes = intermediate.toByteArray();
    return Arrays.asList(
      new IntWritable(state.getNumNominalEntries()),
      new BytesWritable(bytes)
    );
  }

  @Override
  public void merge(final @SuppressWarnings("deprecation") AggregationBuffer buf, final Object data) throws HiveException {
    if (data == null) return;
    @SuppressWarnings("unchecked")
    final UnionState<S> state = (UnionState<S>) buf;
    if (!state.isInitialized()) {
      initializeState(state, data);
    }
    final BytesWritable serializedSketch = 
        (BytesWritable) intermediateInspector_.getStructFieldData(
            data, intermediateInspector_.getStructFieldRef(SKETCH_FIELD));
    state.update(Sketches.heapifySketch(new NativeMemory(serializedSketch.getBytes())));
  }

  private void initializeState(final UnionState<S> state, final Object data) {
    final int nominalEntries = ((IntWritable) intermediateInspector_.getStructFieldData(
        data, intermediateInspector_.getStructFieldRef(NUM_NOMINAL_ENTRIES_FIELD))).get();
    state.init(nominalEntries, summaryFactory_);
  }

  @Override
  public Object terminate(final @SuppressWarnings("deprecation") AggregationBuffer buf) throws HiveException {
    @SuppressWarnings("unchecked")
    final State<S> state = (State<S>) buf;
    if (state == null) return null;
    Sketch<S> result = state.getResult();
    if (result == null) return null;
    return new BytesWritable(result.toByteArray());
  }

  @Override
  public void reset(@SuppressWarnings("deprecation") AggregationBuffer buf) throws HiveException {
    @SuppressWarnings("unchecked")
    final State<S> state = (State<S>) buf;
    state.reset();
  }

}
