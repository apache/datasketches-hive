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

import java.util.Arrays;

import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.Sketch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

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
    if (intermediate == null) { return null; }
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
    if (partial == null) { return; }
    final UnionState state = (UnionState) agg;
    if (!state.isInitialized()) {
      initializeState(state, partial);
    }
    final Memory serializedSketch = BytesWritableHelper.wrapAsMemory(
        (BytesWritable) intermediateObjectInspector.getStructFieldData(
            partial, intermediateObjectInspector.getStructFieldRef(SKETCH_FIELD)));
    state.update(serializedSketch);
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
    final Sketch result = state.getResult();
    if (result == null) { return null; }
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
