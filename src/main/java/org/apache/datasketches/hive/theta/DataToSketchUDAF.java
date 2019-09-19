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

package org.apache.datasketches.hive.theta;

import static org.apache.datasketches.Util.DEFAULT_NOMINAL_ENTRIES;
import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

@Description(
    name = "dataToSketch",
    value = "_FUNC_(expr, size, prob, seed) - "
        + "Compute a sketch of given size, sampling probability and seed on data 'expr'",
    extended = "Example:\n"
    + "> SELECT dataToSketch(val, 16384) FROM src;\n"
    + "The return value is a binary blob that can be operated on by other sketch related functions."
    + " The sketch size is optional, must be a power of 2 and "
    + "controls the relative error expected from the sketch."
    + " A size of 16384 can be expected to yield errors of roughly +-1.5% in the estimation of uniques."
    + " The default size is defined in the sketches-core library "
    + "and at the time of this writing was 4096 (about 3% error)."
    + " The sampling probability is optional and must be from 0 to 1. The default is 1 (no sampling)"
    + " The seed is optional, and using it is not recommended unless you really know why you need it")
@SuppressWarnings("javadoc")
public class DataToSketchUDAF extends AbstractGenericUDAFResolver {

  /**
   * Performs argument number and type validation. DataToSketch expects
   * to receive between one and four arguments.
   * <ul>
   * <li>The first (required) is the value to add to the sketch and must be a primitive.</li>
   *
   * <li>The second (optional) is the sketch size to use. This must be an integral value
   * and must be constant.</li>
   *
   * <li>The third (optional) is the sampling probability and is a floating point value between
   * 0.0 and 1.0. It must be a constant</li>
   *
   * <li>The fourth (optional) is an update seed.
   * It must be an integral value and must be constant.</li>
   * </ul>
   *
   * @see org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver
   * #getEvaluator(org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo)
   *
   * @param info Parameter info to validate
   * @return The GenericUDAFEvaluator that should be used to calculate the function.
   */
  @Override
  public GenericUDAFEvaluator getEvaluator(final GenericUDAFParameterInfo info) throws SemanticException {
    final ObjectInspector[] parameters = info.getParameterObjectInspectors();

    // Validate the correct number of parameters
    if (parameters.length < 1) {
      throw new UDFArgumentException("Please specify at least 1 argument");
    }

    if (parameters.length > 4) {
      throw new UDFArgumentException("Please specify no more than 4 arguments");
    }

    // Validate first parameter type
    ObjectInspectorValidator.validateCategoryPrimitive(parameters[0], 0);

    // Validate second argument if present
    if (parameters.length > 1) {
      ObjectInspectorValidator.validateIntegralParameter(parameters[1], 1);
      if (!ObjectInspectorUtils.isConstantObjectInspector(parameters[1])) {
        throw new UDFArgumentTypeException(1, "The second argument must be a constant");
      }
    }

    // Validate third argument if present
    if (parameters.length > 2) {
      ObjectInspectorValidator.validateFloatingPointParameter(parameters[2], 2);
      if (!ObjectInspectorUtils.isConstantObjectInspector(parameters[2])) {
        throw new UDFArgumentTypeException(2, "The third argument must be a constant");
      }
    }

    // Validate fourth argument if present
    if (parameters.length > 3) {
      ObjectInspectorValidator.validateIntegralParameter(parameters[3], 3);
      if (!ObjectInspectorUtils.isConstantObjectInspector(parameters[3])) {
        throw new UDFArgumentTypeException(3, "The fourth argument must be a constant");
      }
    }

    return new DataToSketchEvaluator();
  }

  public static class DataToSketchEvaluator extends UnionEvaluator {

    // FOR PARTIAL1 and COMPLETE modes: ObjectInspectors for original data
    private transient PrimitiveObjectInspector samplingProbabilityObjectInspector;


    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator#init(org.apache
     * .hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode,
     * org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector[])
     */
    @Override
    public ObjectInspector init(final Mode mode, final ObjectInspector[] parameters) throws HiveException {
      super.init(mode, parameters);

      if ((mode == Mode.PARTIAL1) || (mode == Mode.COMPLETE)) {
        // input is original data
        inputObjectInspector = (PrimitiveObjectInspector) parameters[0];
        if (parameters.length > 1) {
          nominalEntriesObjectInspector = (PrimitiveObjectInspector) parameters[1];
        }
        if (parameters.length > 2) {
          samplingProbabilityObjectInspector = (PrimitiveObjectInspector) parameters[2];
        }
        if (parameters.length > 3) {
          seedObjectInspector = (PrimitiveObjectInspector) parameters[3];
        }
      } else {
        // input for PARTIAL2 and FINAL is the output from PARTIAL1
        intermediateObjectInspector = (StructObjectInspector) parameters[0];
      }

      if ((mode == Mode.PARTIAL1) || (mode == Mode.PARTIAL2)) {
        // intermediate results need to include the the nominal number of entries and the seed
        return ObjectInspectorFactory.getStandardStructObjectInspector(
          Arrays.asList(NOMINAL_ENTRIES_FIELD, SEED_FIELD, SKETCH_FIELD),
          Arrays.asList(
            PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.INT),
            PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.LONG),
            PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY)
          )
        );
      }
      // final results include just the sketch
      return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator#iterate(org
     * .apache
     * .hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer,
     * java.lang.Object[])
     */
    @Override
    public void iterate(final @SuppressWarnings("deprecation") AggregationBuffer agg,
        final Object[] parameters) throws HiveException {
      if (parameters[0] == null) { return; }
      final UnionState state = (UnionState) agg;
      if (!state.isInitialized()) {
        initializeState(state, parameters);
      }
      state.update(parameters[0], inputObjectInspector);
    }

    private void initializeState(final UnionState state, final Object[] parameters) {
      int sketchSize = DEFAULT_NOMINAL_ENTRIES;
      if (nominalEntriesObjectInspector != null) {
        sketchSize = PrimitiveObjectInspectorUtils.getInt(parameters[1], nominalEntriesObjectInspector);
      }
      float samplingProbability = UnionState.DEFAULT_SAMPLING_PROBABILITY;
      if (samplingProbabilityObjectInspector != null) {
        samplingProbability = PrimitiveObjectInspectorUtils.getFloat(parameters[2],
            samplingProbabilityObjectInspector);
      }
      long seed = DEFAULT_UPDATE_SEED;
      if (seedObjectInspector != null) {
        seed = PrimitiveObjectInspectorUtils.getLong(parameters[3], seedObjectInspector);
      }
      state.init(sketchSize, samplingProbability, seed);
    }

  }

}
