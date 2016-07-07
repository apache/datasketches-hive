/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.hive.quantiles;

import java.util.Comparator;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

import com.yahoo.sketches.ArrayOfItemsSerDe;

/**
 * This is a generic implementation to be specialized in subclasses
 * @param <T> type of item
 */
public abstract class DataToItemsSketchUDAF<T> extends AbstractGenericUDAFResolver {

  @Override
  public GenericUDAFEvaluator getEvaluator(final GenericUDAFParameterInfo info) throws SemanticException {
    final ObjectInspector[] inspectors = info.getParameterObjectInspectors();
    if (inspectors.length != 1 && inspectors.length != 2) throw new UDFArgumentException("One or two arguments expected");

    if (inspectors[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0, "Primitive argument expected, but "
          + inspectors[0].getCategory().name() + " was recieved");
    }

    if (inspectors.length == 2) {
      if (inspectors[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
        throw new UDFArgumentTypeException(0, "Primitive argument expected, but "
            + inspectors[1].getCategory().name() + " was recieved");
      }
      final PrimitiveObjectInspector inspector2 = (PrimitiveObjectInspector) inspectors[1];
      if (inspector2.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT) {
        throw new UDFArgumentTypeException(0, "Integer value expected as the second argument, but "
            + inspector2.getPrimitiveCategory().name() + " was received");
      }
    }

    return createEvaluator();
  }

  abstract GenericUDAFEvaluator createEvaluator();

  public static abstract class DataToSketchEvaluator<T> extends ItemsEvaluator<T> {

    private PrimitiveObjectInspector kObjectInspector;

    DataToSketchEvaluator(final Comparator<? super T> comparator, final ArrayOfItemsSerDe<T> serDe) {
      super(comparator, serDe);
    }

    @Override
    public ObjectInspector init(final Mode mode, final ObjectInspector[] parameters) throws HiveException {
      final ObjectInspector result = super.init(mode, parameters);

      // Parameters:
      // In PARTIAL1 and COMPLETE mode, the parameters are original data.
      // In PARTIAL2 and FINAL mode, the parameters are just partial aggregations.
      if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
        if (parameters.length > 1) kObjectInspector = (PrimitiveObjectInspector) parameters[1];
      }

      return result;
    }

    @SuppressWarnings("deprecation")
    @Override
    public void iterate(final AggregationBuffer buf, final Object[] data) throws HiveException {
      if (data[0] == null) return;
      @SuppressWarnings("unchecked")
      final ItemsUnionState<T> state = (ItemsUnionState<T>) buf;
      if (!state.isInitialized()) {
        int k = 0;
        if (kObjectInspector != null) {
          k = PrimitiveObjectInspectorUtils.getInt(data[1], kObjectInspector);
        }
        state.init(k);
      }
      state.update(extractValue(data[0], inputObjectInspector));
    }

    abstract T extractValue(final Object data, final ObjectInspector objectInspector) throws HiveException;

  }

}
