/*******************************************************************************
 * Copyright 2016, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 *******************************************************************************/

package com.yahoo.sketches.hive.theta;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.theta.Sketch;

/**
 * Common code for DataToSketchUDAF and UnionSketchUDAF
 */
public abstract class UnionEvaluator extends GenericUDAFEvaluator {

  protected static final String NOMINAL_ENTRIES_FIELD = "nominalEntries";
  protected static final String SEED_FIELD = "seed";
  protected static final String SKETCH_FIELD = "sketch";

  // FOR PARTIAL1 and COMPLETE modes: ObjectInspectors for original data
  protected transient PrimitiveObjectInspector inputObjectInspector;
  protected transient PrimitiveObjectInspector nominalEntriesObjectInspector;
  protected transient PrimitiveObjectInspector seedObjectInspector;

  // FOR PARTIAL2 and FINAL modes: ObjectInspectors for partial aggregations
  protected transient StructObjectInspector intermediateObjectInspector;

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator#terminatePartial
   * (
   * org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer
   * )
   */
  @Override
  public Object terminatePartial(final @SuppressWarnings("deprecation") AggregationBuffer agg)
      throws HiveException {
    final UnionState state = (UnionState) agg;
    final Sketch intermediate = state.getResult();
    if (intermediate == null) return null;
    final byte[] bytes = intermediate.toByteArray();
    // sampling probability is not relevant for merging
    return Arrays.asList(
      new IntWritable(state.getNominalEntries()),
      new LongWritable(state.getSeed()),
      new BytesWritable(bytes)
    );
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator#merge(org.
   * apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer,
   * java.lang.Object)
   */
  @Override
  public void merge(final @SuppressWarnings("deprecation") AggregationBuffer agg, 
      final Object partial) throws HiveException {
    if (partial == null) return;
    final UnionState state = (UnionState) agg;
    if (!state.isInitialized()) {
      initializeState(state, partial);
    }
    final BytesWritable serializedSketch = 
        (BytesWritable) intermediateObjectInspector.getStructFieldData(
            partial, intermediateObjectInspector.getStructFieldRef(SKETCH_FIELD));
    state.update(new NativeMemory(serializedSketch.getBytes()));
  }

  private void initializeState(final UnionState state, final Object partial) {
    final int nominalEntries = ((IntWritable) intermediateObjectInspector.getStructFieldData(
        partial, intermediateObjectInspector.getStructFieldRef(NOMINAL_ENTRIES_FIELD))).get();
    final long seed = ((LongWritable) intermediateObjectInspector.getStructFieldData(
        partial, intermediateObjectInspector.getStructFieldRef(SEED_FIELD))).get();
    state.init(nominalEntries, seed);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator#terminate(
   * org.apache
   * .hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer)
   */
  @Override
  public Object terminate(final @SuppressWarnings("deprecation") AggregationBuffer agg) 
      throws HiveException {
    final UnionState state = (UnionState) agg;
    Sketch result = state.getResult();
    if (result == null) return null;
    return new BytesWritable(result.toByteArray());
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator
   * #reset(org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer)
   */
  @Override
  public void reset(final @SuppressWarnings("deprecation") AggregationBuffer agg) 
      throws HiveException {
    final UnionState state = (UnionState) agg;
    state.reset();
  }

  /**
   * 
   * @see org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator#getNewAggregationBuffer()
   */
  @SuppressWarnings("deprecation")
  @Override
  public AggregationBuffer getNewAggregationBuffer() throws HiveException {
    return new UnionState();
  }

}
