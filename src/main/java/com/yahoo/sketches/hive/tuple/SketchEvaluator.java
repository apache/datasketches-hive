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
import com.yahoo.sketches.tuple.Sketch;
import com.yahoo.sketches.tuple.Sketches;
import com.yahoo.sketches.tuple.Summary;
import com.yahoo.sketches.tuple.SummaryDeserializer;
import com.yahoo.sketches.tuple.SummaryFactory;
import com.yahoo.sketches.tuple.SummarySetOperations;

abstract class SketchEvaluator<S extends Summary> extends GenericUDAFEvaluator {

  protected static final String NOMINAL_NUM_ENTRIES_FIELD = "nominalEntries";
  protected static final String SKETCH_FIELD = "sketch";

  protected PrimitiveObjectInspector nominalNumEntriesInspector_;
  protected StructObjectInspector intermediateInspector_;

  /**
   * Get an instance of SummaryDeserializer
   * @return SummaryDeserializer
   */
  protected abstract SummaryDeserializer<S> getSummaryDeserializer();

  /**
   * Get an instance of SummaryFactory possibly parameterized
   * based on the original input array of objects.
   * Might be called once during the first call to iterate.
   * @param data original input array of objects
   * @return an instance of SummaryFactory
   */
  protected abstract SummaryFactory<S> getSummaryFactory(Object[] data);

  /**
   * Get an instance of SummarySetOperations possibly parameterized
   * based on the original input array of objects.
   * Might be called once during the first call to iterate.
   * @param data original input array of objects
   * @return an instance of SummarySetOperations
   */
  protected abstract SummarySetOperations<S> getSummarySetOperationsForIterate(Object[] data);

  @Override
  public Object terminatePartial(final @SuppressWarnings("deprecation") AggregationBuffer buf)
      throws HiveException {
    @SuppressWarnings("unchecked")
    final State<S> state = (State<S>) buf;
    final Sketch<S> intermediate = state.getResult();
    if (intermediate == null) { return null; }
    final byte[] bytes = intermediate.toByteArray();
    return Arrays.asList(
      new IntWritable(state.getNominalNumEntries()),
      new BytesWritable(bytes)
    );
  }

  @Override
  public void merge(final @SuppressWarnings("deprecation") AggregationBuffer buf, final Object data)
      throws HiveException {
    if (data == null) { return; }
    @SuppressWarnings("unchecked")
    final UnionState<S> state = (UnionState<S>) buf;
    if (!state.isInitialized()) {
      initializeState(state, data);
    }
    final BytesWritable serializedSketch =
        (BytesWritable) intermediateInspector_.getStructFieldData(
            data, intermediateInspector_.getStructFieldRef(SKETCH_FIELD));
    state.update(Sketches.heapifySketch(Memory.wrap(serializedSketch.getBytes()), getSummaryDeserializer()));
  }

  protected void initializeState(final UnionState<S> state, final Object data) {
    final int nominalNumEntries = ((IntWritable) intermediateInspector_.getStructFieldData(
        data, intermediateInspector_.getStructFieldRef(NOMINAL_NUM_ENTRIES_FIELD))).get();
    state.init(nominalNumEntries, getSummarySetOperationsForMerge(data));
  }

  /**
   * Get an instance of SummarySetOperations possibly parameterized
   * based on the intermediate data object.
   * Might be called once during the first call to merge.
   * @param data intermediate data object
   * @return an instance of SummarySetOperations
   */
  protected abstract SummarySetOperations<S> getSummarySetOperationsForMerge(Object data);

  @Override
  public Object terminate(final @SuppressWarnings("deprecation") AggregationBuffer buf)
      throws HiveException {
    @SuppressWarnings("unchecked")
    final State<S> state = (State<S>) buf;
    if (state == null) { return null; }
    final Sketch<S> result = state.getResult();
    if (result == null) { return null; }
    return new BytesWritable(result.toByteArray());
  }

  @Override
  public void reset(@SuppressWarnings("deprecation") final AggregationBuffer buf)
      throws HiveException {
    @SuppressWarnings("unchecked")
    final State<S> state = (State<S>) buf;
    state.reset();
  }

}
