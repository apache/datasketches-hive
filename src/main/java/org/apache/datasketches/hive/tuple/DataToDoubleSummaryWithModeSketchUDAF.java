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

package org.apache.datasketches.hive.tuple;

import java.util.Arrays;

import org.apache.datasketches.tuple.Sketch;
import org.apache.datasketches.tuple.SummaryDeserializer;
import org.apache.datasketches.tuple.SummaryFactory;
import org.apache.datasketches.tuple.SummarySetOperations;
import org.apache.datasketches.tuple.adouble.DoubleSummary;
import org.apache.datasketches.tuple.adouble.DoubleSummaryDeserializer;
import org.apache.datasketches.tuple.adouble.DoubleSummaryFactory;
import org.apache.datasketches.tuple.adouble.DoubleSummarySetOperations;
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

  @SuppressWarnings("resource")
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

    private static final SummaryDeserializer<DoubleSummary> SUMMARY_DESERIALIZER =
        new DoubleSummaryDeserializer();
    private static final String SUMMARY_MODE_FIELD = "summaryMode";
    private PrimitiveObjectInspector summaryModeInspector_;
    private DoubleSummary.Mode summaryMode_;

    public DataToDoubleSummaryWithModeSketchEvaluator() {
      summaryMode_ = DoubleSummary.Mode.Sum;
    }

    @Override
    protected SummaryDeserializer<DoubleSummary> getSummaryDeserializer() {
      return SUMMARY_DESERIALIZER;
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
      }
      return resultInspector;
    }

    @Override
    protected SummaryFactory<DoubleSummary> getSummaryFactory(final Object[] data) {
      if (summaryModeInspector_ != null) {
        summaryMode_ = DoubleSummary.Mode.valueOf(
          PrimitiveObjectInspectorUtils.getString(data[4], summaryModeInspector_)
        );
      }
      return new DoubleSummaryFactory(summaryMode_);
    }

    @Override
    protected SummarySetOperations<DoubleSummary> getSummarySetOperationsForIterate(final Object[] data) {
      return null; // not needed for building sketches
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
    protected SummarySetOperations<DoubleSummary> getSummarySetOperationsForMerge(final Object data) {
      summaryMode_ = DoubleSummary.Mode.valueOf(((Text) intermediateInspector_.getStructFieldData(
          data, intermediateInspector_.getStructFieldRef(SUMMARY_MODE_FIELD))).toString());
      return new DoubleSummarySetOperations(summaryMode_);
    }

  }

}
