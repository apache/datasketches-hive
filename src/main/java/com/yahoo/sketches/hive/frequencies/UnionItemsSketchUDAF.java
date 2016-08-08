/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.frequencies;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

import com.yahoo.sketches.ArrayOfItemsSerDe;

/**
 * This is a generic implementation to be specialized in subclasses
 * @param <T> type of item
 */
public abstract class UnionItemsSketchUDAF<T> extends AbstractGenericUDAFResolver {

  @Override
  public GenericUDAFEvaluator getEvaluator(final GenericUDAFParameterInfo info) throws SemanticException {
    final ObjectInspector[] inspectors = info.getParameterObjectInspectors();
    if (inspectors.length != 1) throw new UDFArgumentException("One argument expected");
    if (inspectors[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0, "Primitive argument expected, but "
          + inspectors[0].getTypeName() + " was recieved");
    }
    final PrimitiveObjectInspector inspector = (PrimitiveObjectInspector) inspectors[0];
    if (inspector.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.BINARY) {
      throw new UDFArgumentTypeException(0, "Binary argument expected, but "
          + inspector.getPrimitiveCategory().name() + " was received");
    }
    return createEvaluator();
  }

  abstract GenericUDAFEvaluator createEvaluator();

  public static class UnionItemsSketchEvaluator<T> extends ItemsEvaluator<T> {

    UnionItemsSketchEvaluator(final ArrayOfItemsSerDe<T> serDe) {
      super(serDe);
    }

    @SuppressWarnings("deprecation")
    @Override
    public void iterate(final AggregationBuffer buf, final Object[] data) throws HiveException {
      merge(buf, data[0]);
    }

  }

}
