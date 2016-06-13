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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

@Description(name = "DataToSketch", value = "_FUNC_(value, k) - "
  + "Returns a QuantilesSketch in a serialized form as a binary blob."
  + " Values must be of type double."
  + " Parameter k controls the accuracy and the size of the sketch."
  + " If k is ommitted, the default value of 128 is used.")
public class DataToSketch extends AbstractGenericUDAFResolver {

  @Override
  public GenericUDAFEvaluator getEvaluator(final GenericUDAFParameterInfo info) throws SemanticException {
    final ObjectInspector[] inspectors = info.getParameterObjectInspectors();
    if (inspectors.length != 1 && inspectors.length != 2) throw new UDFArgumentException("One or two arguments expected");

    if (inspectors[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentException("Primitive argument expected");
    }
    final PrimitiveObjectInspector inspector1 = (PrimitiveObjectInspector) inspectors[0];
    if (inspector1.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.DOUBLE) {
      throw new UDFArgumentException("Double value expected as the first argument");
    }

    if (inspectors.length == 2) {
      if (inspectors[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
        throw new UDFArgumentException("Primitive argument expected");
      }
      final PrimitiveObjectInspector inspector2 = (PrimitiveObjectInspector) inspectors[1];
      if (inspector2.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT) {
        throw new UDFArgumentException("Integer value expected as the second argument");
      }
    }

    return new DataToSketchEvaluator();
  }

  static class DataToSketchEvaluator extends QuantilesEvaluator {

    private PrimitiveObjectInspector kObjectInspector;

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
      final QuantilesUnionState state = (QuantilesUnionState) buf;
      if (!state.isInitialized()) {
        int k = 0;
        if (kObjectInspector != null) {
          k = PrimitiveObjectInspectorUtils.getInt(data[1], kObjectInspector);
        }
        state.init(k);
      }
      final double value = (double) inputObjectInspector.getPrimitiveJavaObject(data[0]);
      state.update(value);
    }

  }

}
