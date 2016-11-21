/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.quantiles;

import java.util.Comparator;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.quantiles.ItemsSketch;

abstract class ItemsEvaluator<T> extends GenericUDAFEvaluator {

  private final Comparator<? super T> comparator_;
  private final ArrayOfItemsSerDe<T> serDe_;
  protected PrimitiveObjectInspector inputObjectInspector;
  protected PrimitiveObjectInspector kObjectInspector;

  ItemsEvaluator(final Comparator<? super T> comparator, final ArrayOfItemsSerDe<T> serDe) {
    comparator_ = comparator;
    serDe_ = serDe;
  }

  @Override
  public ObjectInspector init(final Mode mode, final ObjectInspector[] parameters) throws HiveException {
    super.init(mode, parameters);
    inputObjectInspector = (PrimitiveObjectInspector) parameters[0];

    // Parameters:
    // In PARTIAL1 and COMPLETE mode, the parameters are original data.
    // In PARTIAL2 and FINAL mode, the parameters are partial aggregations.
    if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
      if (parameters.length > 1) {
        kObjectInspector = (PrimitiveObjectInspector) parameters[1];
      }
    }

    return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY);
  }

  @SuppressWarnings("deprecation")
  @Override
  public void reset(final AggregationBuffer buf) throws HiveException {
    @SuppressWarnings("unchecked")
    final ItemsUnionState<T> state = (ItemsUnionState<T>) buf;
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
    @SuppressWarnings("unchecked")
    final ItemsUnionState<T> state = (ItemsUnionState<T>) buf;
    final BytesWritable serializedSketch =
        (BytesWritable) inputObjectInspector.getPrimitiveWritableObject(data);
    state.update(serializedSketch.getBytes());
  }

  @SuppressWarnings("deprecation")
  @Override
  public Object terminate(final AggregationBuffer buf) throws HiveException {
    @SuppressWarnings("unchecked")
    final ItemsUnionState<T> state = (ItemsUnionState<T>) buf;
    final ItemsSketch<T> resultSketch = state.getResult();
    if (resultSketch == null) { return null; }
    return new BytesWritable(resultSketch.toByteArray(serDe_));
  }

  @SuppressWarnings("deprecation")
  @Override
  public AggregationBuffer getNewAggregationBuffer() throws HiveException {
    return new ItemsUnionState<T>(comparator_, serDe_);
  }

}
