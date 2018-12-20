/*
 * Copyright 2018, Oath Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.kll;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.kll.KllFloatsSketch;

abstract class SketchEvaluator extends GenericUDAFEvaluator {

  protected PrimitiveObjectInspector inputInspector_;
  protected PrimitiveObjectInspector kInspector_;

  @Override
  public ObjectInspector init(final Mode mode, final ObjectInspector[] parameters) throws HiveException {
    super.init(mode, parameters);
    inputInspector_ = (PrimitiveObjectInspector) parameters[0];

    // Parameters:
    // In PARTIAL1 and COMPLETE mode, the parameters are original data.
    // In PARTIAL2 and FINAL mode, the parameters are partial aggregations.
    if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
      if (parameters.length > 1) {
        kInspector_ = (PrimitiveObjectInspector) parameters[1];
      }
    }

    return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY);
  }

  @SuppressWarnings("deprecation")
  @Override
  public void reset(final AggregationBuffer buf) throws HiveException {
    final SketchState state = (SketchState) buf;
    state.reset();
  }

  @SuppressWarnings("deprecation")
  @Override
  public Object terminatePartial(final AggregationBuffer buf) throws HiveException {
    return terminate(buf);
  }

  @SuppressWarnings("deprecation")
  @Override
  public void merge(final AggregationBuffer buf, final Object data) throws HiveException {
    if (data == null) { return; }
    final SketchState state = (SketchState) buf;
    final BytesWritable serializedSketch =
        (BytesWritable) inputInspector_.getPrimitiveWritableObject(data);
    state.update(KllFloatsSketch.heapify(Memory.wrap(serializedSketch.getBytes())));
  }

  @SuppressWarnings("deprecation")
  @Override
  public Object terminate(final AggregationBuffer buf) throws HiveException {
    final SketchState state = (SketchState) buf;
    final KllFloatsSketch resultSketch = state.getResult();
    if (resultSketch == null) { return null; }
    return new BytesWritable(resultSketch.toByteArray());
  }

  @SuppressWarnings("deprecation")
  @Override
  public AggregationBuffer getNewAggregationBuffer() throws HiveException {
    return new SketchState();
  }

}
