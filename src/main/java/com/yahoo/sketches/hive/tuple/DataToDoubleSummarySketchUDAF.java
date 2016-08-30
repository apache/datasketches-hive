/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.tuple;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

import com.yahoo.sketches.tuple.DoubleSummary;
import com.yahoo.sketches.tuple.DoubleSummaryFactory;

@Description(
  name = "DataToDoubleSummarySketch",
  value = "_FUNC_(key, double value, sketch size, sampling probability)",
  extended = "Returns a DoubleSummarySketch as a binary blob that can be operated on by other"
    + " tuple sketch related functions. The sketch size is optional, must be a power of 2"
    + " and controls the relative error expected from the sketch."
    + " A size of 16384 can be expected to yield errors of roughly +-1.5% in the estimation of"
    + " uniques. The default size is defined in the sketches-core library and at the time of this"
    + " writing was 4096 (about 3% error)."
    + " The sampling probability is optional and must be from 0 to 1. The default is 1 (no sampling)")
public class DataToDoubleSummarySketchUDAF extends DataToSketchUDAF {

  @Override
  public GenericUDAFEvaluator getEvaluator(final GenericUDAFParameterInfo info) throws SemanticException {
    final ObjectInspector[] inspectors = info.getParameterObjectInspectors();
    ObjectInspectorValidator.validateGivenPrimitiveCategory(inspectors[1], 1, PrimitiveCategory.DOUBLE);
    return super.getEvaluator(info);
  }

  @Override
  public GenericUDAFEvaluator createEvaluator() {
    return new DataToDoublesSketchEvaluator();
  }

  static class DataToDoublesSketchEvaluator extends DataToSketchEvaluator<Double, DoubleSummary> {

    public DataToDoublesSketchEvaluator() {
      super(new DoubleSummaryFactory());
    }

    @Override
    public Double extractValue(final Object data, final PrimitiveObjectInspector valueObjectInspector) throws HiveException {
      @SuppressWarnings("unchecked")
      final Double value = (Double) valueObjectInspector.getPrimitiveJavaObject(data);
      return value;
    }

  }

}
