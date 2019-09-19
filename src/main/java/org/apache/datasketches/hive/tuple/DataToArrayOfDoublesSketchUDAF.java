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

import static org.apache.datasketches.Util.DEFAULT_NOMINAL_ENTRIES;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

@Description(
  name = "DataToArrayOfDoublesSketch",
  value = "_FUNC_(key, double param 1, ..., double param N, nominal number of entries, sampling probability)",
  extended = "Returns an ArrayOfDoublesSketch as a binary blob that can be operated on by other"
    + " ArrayOfDoublesSketch related functions. "
    + "The nominal number of entries is optional, must be a power of 2,"
    + " and controls the relative error expected from the sketch."
    + " A number of 16384 can be expected to yield errors of roughly +-1.5% in the estimation of"
    + " uniques. The default number is defined in the sketches-core library, and at the time of this"
    + " writing was 4096 (about 3% error)."
    + " The sampling probability is optional and must be from 0 to 1. The default is 1 (no sampling)")
@SuppressWarnings("javadoc")
public class DataToArrayOfDoublesSketchUDAF extends AbstractGenericUDAFResolver {

  @Override
  public GenericUDAFEvaluator getEvaluator(final GenericUDAFParameterInfo info) throws SemanticException {
    final ObjectInspector[] inspectors = info.getParameterObjectInspectors();

    if (inspectors.length < 2) {
      throw new UDFArgumentException("Expected at least 2 arguments");
    }
    ObjectInspectorValidator.validateCategoryPrimitive(inspectors[0], 0);

    int numValues = 0;
    while ((numValues + 1) < inspectors.length) {
      ObjectInspectorValidator.validateCategoryPrimitive(inspectors[numValues + 1], numValues + 1);
      final PrimitiveObjectInspector primitiveInspector =
          (PrimitiveObjectInspector) inspectors[numValues + 1];
      if (primitiveInspector.getPrimitiveCategory() != PrimitiveCategory.DOUBLE) { break; }
      numValues++;
    }
    if (numValues == 0) {
      throw new UDFArgumentException("Expected at least 1 double value");
    }

    // nominal number of entries
    if (inspectors.length > (numValues + 1)) {
      ObjectInspectorValidator.validateIntegralParameter(inspectors[numValues + 1], numValues + 1);
    }

    // sampling probability
    if (inspectors.length > (numValues + 2)) {
      ObjectInspectorValidator.validateGivenPrimitiveCategory(inspectors[numValues + 2],
          numValues + 2, PrimitiveCategory.FLOAT);
    }

    // there must be nothing after sampling probability
    if (inspectors.length > (numValues + 3)) {
      throw new UDFArgumentException("Unexpected argument " + (numValues + 4));
    }

    return new DataToArrayOfDoublesSketchEvaluator();
  }

  public static class DataToArrayOfDoublesSketchEvaluator extends ArrayOfDoublesSketchEvaluator {

    private static final float DEFAULT_SAMPLING_PROBABILITY = 1f;

    private PrimitiveObjectInspector keyInspector_;
    private PrimitiveObjectInspector[] valuesInspectors_;
    private PrimitiveObjectInspector samplingProbabilityInspector_;

    private int numValues_;
    private Mode mode_;

    @Override
    public ObjectInspector init(final Mode mode, final ObjectInspector[] parameters) throws HiveException {
      super.init(mode, parameters);
      mode_ = mode;
      if ((mode == Mode.PARTIAL1) || (mode == Mode.COMPLETE)) {
        // input is original data
        keyInspector_ = (PrimitiveObjectInspector) parameters[0];
        numValues_ = 0;
        while ((numValues_ + 1) < parameters.length) {
          if (((PrimitiveObjectInspector) parameters[numValues_ + 1]).getPrimitiveCategory()
              != PrimitiveCategory.DOUBLE) {
            break;
          }
          numValues_++;
        }
        valuesInspectors_ = new PrimitiveObjectInspector[numValues_];
        for (int i = 0; i < numValues_; i++) {
          valuesInspectors_[i] = (PrimitiveObjectInspector) parameters[i + 1];
        }
        if (parameters.length > (numValues_ + 1)) {
          nominalNumEntriesInspector_ = (PrimitiveObjectInspector) parameters[numValues_ + 1];
        }
        if (parameters.length > (numValues_ + 2)) {
          samplingProbabilityInspector_ = (PrimitiveObjectInspector) parameters[numValues_ + 2];
        }
      } else {
        // input for PARTIAL2 and FINAL is the output from PARTIAL1
        intermediateInspector_ = (StructObjectInspector) parameters[0];
      }

      if ((mode == Mode.PARTIAL1) || (mode == Mode.PARTIAL2)) {
        // intermediate results need to include the the nominal number of entries and number of values
        return ObjectInspectorFactory.getStandardStructObjectInspector(
          Arrays.asList(NOMINAL_NUM_ENTRIES_FIELD, NUM_VALUES_FIELD, SKETCH_FIELD),
          Arrays.asList(
            PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.INT),
            PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.INT),
            PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY)
          )
        );
      }
      // final results include just the sketch
      return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY);
    }

    @Override
    public void iterate(final @SuppressWarnings("deprecation") AggregationBuffer buf,
        final Object[] data) throws HiveException {
      if (data[0] == null) { return; }
      final ArrayOfDoublesSketchState state = (ArrayOfDoublesSketchState) buf;
      if (!state.isInitialized()) {
        initializeState(state, data);
      }
      state.update(data, keyInspector_, valuesInspectors_);
    }

    private void initializeState(final ArrayOfDoublesSketchState state, final Object[] data) {
      int nominalNumEntries = DEFAULT_NOMINAL_ENTRIES;
      if (nominalNumEntriesInspector_ != null) {
        nominalNumEntries =
            PrimitiveObjectInspectorUtils.getInt(data[numValues_ + 1], nominalNumEntriesInspector_);
      }
      float samplingProbability = DEFAULT_SAMPLING_PROBABILITY;
      if (samplingProbabilityInspector_ != null) {
        samplingProbability = PrimitiveObjectInspectorUtils.getFloat(data[numValues_ + 2],
            samplingProbabilityInspector_);
      }
      state.init(nominalNumEntries, samplingProbability, numValues_);
    }

    @SuppressWarnings("deprecation")
    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      if ((mode_ == Mode.PARTIAL1) || (mode_ == Mode.COMPLETE)) {
        return new ArrayOfDoublesSketchState();
      }
      return new ArrayOfDoublesUnionState();
    }

  }

}
