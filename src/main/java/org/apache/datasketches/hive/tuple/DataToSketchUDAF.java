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

import org.apache.datasketches.tuple.UpdatableSummary;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
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

@SuppressWarnings("javadoc")
public abstract class DataToSketchUDAF extends AbstractGenericUDAFResolver {

  @Override
  public GenericUDAFEvaluator getEvaluator(final GenericUDAFParameterInfo info) throws SemanticException {
    final ObjectInspector[] inspectors = info.getParameterObjectInspectors();

    if (inspectors.length < 2) {
      throw new UDFArgumentException("Expected at least 2 arguments");
    }
    ObjectInspectorValidator.validateCategoryPrimitive(inspectors[0], 0);

    // No validation of the value inspector since it can be anything.
    // Override this method to validate if needed.

    // nominal number of entries
    if (inspectors.length > 2) {
      ObjectInspectorValidator.validateIntegralParameter(inspectors[2], 2);
    }

    // sampling probability
    if (inspectors.length > 3) {
      ObjectInspectorValidator.validateCategoryPrimitive(inspectors[3], 3);
      final PrimitiveObjectInspector primitiveInspector = (PrimitiveObjectInspector) inspectors[3];
      if ((primitiveInspector.getPrimitiveCategory() != PrimitiveCategory.FLOAT)
          && (primitiveInspector.getPrimitiveCategory() != PrimitiveCategory.DOUBLE)) {
        throw new UDFArgumentTypeException(3, "float or double value expected as parameter 4 but "
            + primitiveInspector.getPrimitiveCategory().name() + " was received");
      }
    }

    checkExtraArguments(inspectors);

    return createEvaluator();
  }

  /**
   * Override this if your UDF has more arguments
   * @param inspectors array of inspectors
   * @throws SemanticException if anything is wrong
   */
  protected void checkExtraArguments(final ObjectInspector[] inspectors) throws SemanticException {
    if (inspectors.length > 4) {
      throw new UDFArgumentException("Expected no more than 4 arguments");
    }
  }

  /**
   * This is needed because a concrete UDAF is going to have its own concrete evaluator static inner class.
   * @return an instance of evaluator
   */
  public abstract GenericUDAFEvaluator createEvaluator();

  public static abstract class DataToSketchEvaluator<U, S extends UpdatableSummary<U>>
      extends SketchEvaluator<S> {

    private static final float DEFAULT_SAMPLING_PROBABILITY = 1f;

    private PrimitiveObjectInspector keyInspector_;
    private PrimitiveObjectInspector valueInspector_;
    private PrimitiveObjectInspector samplingProbabilityInspector_;

    private Mode mode_;

    @Override
    public ObjectInspector init(final Mode mode, final ObjectInspector[] inspectors) throws HiveException {
      super.init(mode, inspectors);
      mode_ = mode;
      if ((mode == Mode.PARTIAL1) || (mode == Mode.COMPLETE)) {
        // input is original data
        keyInspector_ = (PrimitiveObjectInspector) inspectors[0];
        valueInspector_ = (PrimitiveObjectInspector) inspectors[1];
        if (inspectors.length > 2) {
          nominalNumEntriesInspector_ = (PrimitiveObjectInspector) inspectors[2];
        }
        if (inspectors.length > 3) {
          samplingProbabilityInspector_ = (PrimitiveObjectInspector) inspectors[3];
        }
      } else {
        // input for PARTIAL2 and FINAL is the output from PARTIAL1
        intermediateInspector_ = (StructObjectInspector) inspectors[0];
      }

      if ((mode == Mode.PARTIAL1) || (mode == Mode.PARTIAL2)) {
        // intermediate results need to include the nominal number of entries
        return ObjectInspectorFactory.getStandardStructObjectInspector(
          Arrays.asList(NOMINAL_NUM_ENTRIES_FIELD, SKETCH_FIELD),
          Arrays.asList(
            PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.INT),
            PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY)
          )
        );
      }
      // final results include just the sketch
      return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY);
    }

    @Override
    public void iterate(final @SuppressWarnings("deprecation") AggregationBuffer buf, final Object[] data)
        throws HiveException {
      if (data[0] == null) { return; }
      @SuppressWarnings("unchecked")
      final SketchState<U, S> state = (SketchState<U, S>) buf;
      if (!state.isInitialized()) {
        initializeState(state, data);
      }
      state.update(data[0], keyInspector_, extractValue(data[1], valueInspector_));
    }

    private void initializeState(final SketchState<U, S> state, final Object[] data) {
      int nominalNumEntries = DEFAULT_NOMINAL_ENTRIES;
      if (nominalNumEntriesInspector_ != null) {
        nominalNumEntries = PrimitiveObjectInspectorUtils.getInt(data[2], nominalNumEntriesInspector_);
      }
      float samplingProbability = DEFAULT_SAMPLING_PROBABILITY;
      if (samplingProbabilityInspector_ != null) {
        samplingProbability = PrimitiveObjectInspectorUtils.getFloat(data[3],
            samplingProbabilityInspector_);
      }
      state.init(nominalNumEntries, samplingProbability, getSummaryFactory(data));
    }

    @SuppressWarnings("deprecation")
    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      if ((mode_ == Mode.PARTIAL1) || (mode_ == Mode.COMPLETE)) {
        return new SketchState<U, S>();
      }
      return new UnionState<S>();
    }

    /**
     * Override this if it takes more than a cast to convert Hive value into the sketch update type U
     * @param data Hive value object
     * @param valueInspector PrimitiveObjectInspector for the value
     * @return extracted value
     * @throws HiveException if anything goes wrong
     */
    public U extractValue(final Object data, final PrimitiveObjectInspector valueInspector)
        throws HiveException {
      @SuppressWarnings("unchecked")
      final U value = (U) valueInspector.getPrimitiveJavaObject(data);
      return value;
    }

  }

}
