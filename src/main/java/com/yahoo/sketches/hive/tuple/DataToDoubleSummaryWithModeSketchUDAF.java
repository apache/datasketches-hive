/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.tuple;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import com.yahoo.sketches.tuple.DoubleSummary;
import com.yahoo.sketches.tuple.DoubleSummaryFactory;
import com.yahoo.sketches.tuple.Sketch;
import com.yahoo.sketches.tuple.SummaryFactory;

/**
 * This is an example of a concrete UDAF based on the abstract DataToSketchUDAF if extra arguments
 * are needed. For a simpler example with no extra arguments see DataToDoubleSummarySketchUDAF.
 */

@Description(
  name = "DataToDoubleSummaryWithModeSketch",
  value = "_FUNC_(key, double value, nominal number of entries, sampling probability, summary mode)",
  extended = "Returns a Sketch<DoubleSummary> as a binary blob that can be operated on by other"
    + " tuple sketch related functions. The nominal number of entries is optional, must be a power"
    + " of 2 and controls the relative error expected from the sketch."
    + " A number of 16384 can be expected to yield errors of roughly +-1.5% in the estimation of"
    + " uniques. The default number is defined in the sketches-core library, and at the time of this"
    + " writing was 4096 (about 3% error)."
    + " The sampling probability is optional and must be from 0 to 1. The default is 1 (no sampling)."
    + " Summary mode must be one of: 'Sum', 'Min', 'Max'")
public class DataToDoubleSummaryWithModeSketchUDAF extends DataToSketchUDAF {

  @Override
  public GenericUDAFEvaluator getEvaluator(final GenericUDAFParameterInfo info) throws SemanticException {
    super.getEvaluator(info);
    final ObjectInspector[] inspectors = info.getParameterObjectInspectors();
    ObjectInspectorValidator.validateGivenPrimitiveCategory(inspectors[1], 1, PrimitiveCategory.DOUBLE);
    return createEvaluator();
  }

  @Override
  public GenericUDAFEvaluator createEvaluator() {
    return new DataToDoubleSummaryWithModeSketchEvaluator();
  }

  @Override
  protected void checkExtraArguments(final ObjectInspector[] inspectors) throws SemanticException {
    if (inspectors.length > 5) {
      throw new UDFArgumentException("Expected no more than 5 arguments");
    }

    // summary mode
    if (inspectors.length > 4) {
      ObjectInspectorValidator.validateGivenPrimitiveCategory(inspectors[4], 4, PrimitiveCategory.STRING);
    }

  }

  static class DataToDoubleSummaryWithModeSketchEvaluator
      extends DataToSketchEvaluator<Double, DoubleSummary> {

    private static final String SUMMARY_MODE_FIELD = "summaryMode";
    private PrimitiveObjectInspector summaryModeInspector_;
    private DoubleSummary.Mode summaryMode_;

    public DataToDoubleSummaryWithModeSketchEvaluator() {
      summaryMode_ = DoubleSummary.Mode.Sum;
    }

    // need to add summary mode
    @Override
    public ObjectInspector init(final Mode mode, final ObjectInspector[] inspectors) throws HiveException {
      final ObjectInspector resultInspector = super.init(mode, inspectors);
      if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
        // input is original data
        if (inspectors.length > 4) {
          summaryModeInspector_ = (PrimitiveObjectInspector) inspectors[4];
        }
      }
      if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
        // intermediate results need to include the nominal number of entries and the summary mode
        return ObjectInspectorFactory.getStandardStructObjectInspector(
          Arrays.asList(NOMINAL_NUM_ENTRIES_FIELD, SUMMARY_MODE_FIELD, SKETCH_FIELD),
          Arrays.asList(
            PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.INT),
            PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.STRING),
            PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY)
          )
        );
      } else {
        return resultInspector;
      }
    }

    @Override
    protected SummaryFactory<DoubleSummary> getSummaryFactoryForIterate(final Object[] data) {
      if (summaryModeInspector_ != null) {
        summaryMode_ = DoubleSummary.Mode.valueOf(
          PrimitiveObjectInspectorUtils.getString(data[4], summaryModeInspector_)
        );
      }
      return new DoubleSummaryFactory(summaryMode_);
    }

    // need to add summary mode
    @Override
    public Object terminatePartial(final @SuppressWarnings("deprecation") AggregationBuffer buf)
        throws HiveException {
      @SuppressWarnings("unchecked")
      final State<DoubleSummary> state = (State<DoubleSummary>) buf;
      final Sketch<DoubleSummary> intermediate = state.getResult();
      if (intermediate == null) { return null; }
      final byte[] bytes = intermediate.toByteArray();
      return Arrays.asList(
        new IntWritable(state.getNominalNumEntries()),
        new Text(summaryMode_.toString()),
        new BytesWritable(bytes)
      );
    }

    @Override
    protected SummaryFactory<DoubleSummary> getSummaryFactoryForMerge(final Object data) {
      summaryMode_ = DoubleSummary.Mode.valueOf(((Text) intermediateInspector_.getStructFieldData(
          data, intermediateInspector_.getStructFieldRef(SUMMARY_MODE_FIELD))).toString());
      return new DoubleSummaryFactory(summaryMode_);
    }

  }

}
