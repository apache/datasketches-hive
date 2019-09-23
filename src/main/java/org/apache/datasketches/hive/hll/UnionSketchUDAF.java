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

package org.apache.datasketches.hive.hll;

import java.util.Arrays;

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.memory.Memory;
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
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

/**
 * Hive UDAF to compute union of HllSketch objects
 */
@Description(
    name = "unionSketch",
    value = "_FUNC_(sketch, lgK, type) - Compute the union of sketches of given size and seed",
    extended = "Example:\n"
    + "> SELECT UnionSketch(sketch) FROM src;\n"
    + "The return value is a binary blob that can be operated on by other sketch related functions."
    + " The lgK parameter controls the sketch size and rlative error expected from the sketch."
    + " It is optional an must be from 4 to 21. The default is 12, which is expected to yield errors"
    + " of roughly +-3% in the estimation of uniques with 95% confidence."
    + " The target type parameter is optional and must be 'HLL_4', 'HLL_6' or 'HLL_8'."
    + " The default is 'HLL_4'")
public class UnionSketchUDAF extends AbstractGenericUDAFResolver {

  /**
   * Perform argument count check and argument type checking, returns an
   * appropriate evaluator to perform based on input type (which should always
   * be BINARY sketch). Also check lgK and target HLL type parameters if they are passed in.
   *
   * @see org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver
   * #getEvaluator(org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo)
   *
   * @param info The parameter info to validate
   * @return The GenericUDAFEvaluator to use to compute the function.
   */
  @Override
  public GenericUDAFEvaluator getEvaluator(final GenericUDAFParameterInfo info) throws SemanticException {
    final ObjectInspector[] inspectors = info.getParameterObjectInspectors();

    if (inspectors.length < 1) {
      throw new UDFArgumentException("Please specify at least 1 argument");
    }

    if (inspectors.length > 3) {
      throw new UDFArgumentTypeException(inspectors.length - 1, "Please specify no more than 3 arguments");
    }

    ObjectInspectorValidator.validateGivenPrimitiveCategory(inspectors[0], 0, PrimitiveCategory.BINARY);

    // Validate second argument if present
    if (inspectors.length > 1) {
      ObjectInspectorValidator.validateIntegralParameter(inspectors[1], 1);
      if (!ObjectInspectorUtils.isConstantObjectInspector(inspectors[1])) {
        throw new UDFArgumentTypeException(1, "The second argument must be a constant");
      }
    }

    // Validate third argument if present
    if (inspectors.length > 2) {
      ObjectInspectorValidator.validateGivenPrimitiveCategory(inspectors[2], 2, PrimitiveCategory.STRING);
      if (!ObjectInspectorUtils.isConstantObjectInspector(inspectors[2])) {
        throw new UDFArgumentTypeException(2, "The third argument must be a constant");
      }
    }

    return new UnionSketchUDAFEvaluator();
  }

  /**
   * Evaluator class, main logic of our UDAF.
   *
   */
  public static class UnionSketchUDAFEvaluator extends SketchEvaluator {

    @SuppressWarnings("deprecation")
    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      return new UnionState();
    }

    /**
     * Receives the passed in argument object inspectors and returns the desired
     * return type's object inspector to inform hive of return type of UDAF.
     *
     * @param mode
     *          Mode (i.e. PARTIAL 1, COMPLETE...) for determining input and output
     *          object inspector type.
     * @param parameters
     *          List of object inspectors for input arguments.
     * @return The object inspector type indicates the UDAF return type (i.e.
     *         returned type of terminate(...)).
     */
    @Override
    public ObjectInspector init(final Mode mode, final ObjectInspector[] parameters) throws HiveException {
      super.init(mode, parameters);

      if ((mode == Mode.PARTIAL1) || (mode == Mode.COMPLETE)) {
        inputInspector_ = (PrimitiveObjectInspector) parameters[0];
        if (parameters.length > 1) {
          lgKInspector_ = (PrimitiveObjectInspector) parameters[1];
        }
        if (parameters.length > 2) {
          hllTypeInspector_ = (PrimitiveObjectInspector) parameters[2];
        }
      } else {
        // mode = partial2 || final
        intermediateInspector_ = (StandardStructObjectInspector) parameters[0];
      }

      if ((mode == Mode.PARTIAL1) || (mode == Mode.PARTIAL2)) {
        // intermediate results need to include the lgK and the target HLL type
        return ObjectInspectorFactory.getStandardStructObjectInspector(
          Arrays.asList(LG_K_FIELD, HLL_TYPE_FIELD, SKETCH_FIELD),
          Arrays.asList(
            PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.INT),
            PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.STRING),
            PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY)
          )
        );
      }
      // final results include just the sketch
      return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY);
    }

    /**
     * Add the incoming sketch into the internal state.
     *
     * @param buf
     *          aggregation buffer storing intermediate results.
     * @param parameters
     *          sketches in the form of Object passed in to be merged.
     */
    @Override
    public void iterate(final @SuppressWarnings("deprecation") AggregationBuffer buf,
        final Object[] parameters) throws HiveException {
      if (parameters[0] == null) { return; }
      final UnionState state = (UnionState) buf;
      if (!state.isInitialized()) {
        initializeState(state, parameters);
      }
      final byte[] serializedSketch = (byte[]) inputInspector_.getPrimitiveJavaObject(parameters[0]);
      if (serializedSketch == null) { return; }
      state.update(HllSketch.wrap(Memory.wrap(serializedSketch)));
    }

    private void initializeState(final UnionState state, final Object[] parameters) {
      int lgK = DEFAULT_LG_K;
      if (lgKInspector_ != null) {
        lgK = PrimitiveObjectInspectorUtils.getInt(parameters[1], lgKInspector_);
      }
      TgtHllType type = DEFAULT_HLL_TYPE;
      if (hllTypeInspector_ != null) {
        type = TgtHllType.valueOf(PrimitiveObjectInspectorUtils.getString(parameters[2], hllTypeInspector_));
      }
      state.init(lgK, type);
    }

  }

}
