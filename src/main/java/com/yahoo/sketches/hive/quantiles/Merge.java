/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.hive.quantiles;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

@Description(name = "Merge", value = "_FUNC_(sketch) - "
    + "Returns a QuantilesSketch in a serialized form as a binary blob."
    + " Input values are also serialized sketches.")
public class Merge extends AbstractGenericUDAFResolver {

  @Override
  public GenericUDAFEvaluator getEvaluator(final GenericUDAFParameterInfo info) throws SemanticException {
    final ObjectInspector[] inspectors = info.getParameterObjectInspectors();
    if (inspectors.length != 1) throw new UDFArgumentException("One argument expected");
    if (inspectors[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentException("Primitive argument expected");
    }
    final PrimitiveObjectInspector inspector = (PrimitiveObjectInspector) inspectors[0];
    if (inspector.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.BINARY) {
      throw new UDFArgumentException("Binary argument expected");
    }
    return new MergeEvaluator();
  }

  static class MergeEvaluator extends QuantilesEvaluator {

    @SuppressWarnings("deprecation")
    @Override
    public void iterate(final AggregationBuffer buf, final Object[] data) throws HiveException {
      merge(buf, data[0]);
    }

  }

}
