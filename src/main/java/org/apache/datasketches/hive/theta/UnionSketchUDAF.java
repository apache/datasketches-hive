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
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

/**
 * Hive UDAF to compute union of theta Sketch objects
 */
@Description(
    name = "unionSketch",
    value = "_FUNC_(sketch, size, seed) - Compute the union of sketches of given size and seed",
    extended = "Example:\n"
    + "> SELECT UnionSketch(sketch, 16384) FROM src;\n"
    + "The return value is a binary blob that contains a compact sketch, which can "
    + "be operated on by the other sketch-related functions. The optional "
    + "size must be a power of 2, and controls the relative error of the expected "
    + "result. A size of 16384 can be expected to yeild errors of roughly +-1.5% "
    + "in the estimation of uniques with 95% confidence. "
    + "The default size is defined in the sketches-core library and at the time of this writing "
    + "was 4096 (about 3% error). "
    + "The seed is optional, and using it is not recommended unless you really know why you need it")
public class UnionSketchUDAF extends AbstractGenericUDAFResolver {

  /**
   * Perform argument count check and argument type checking, returns an
   * appropriate evaluator to perform based on input type (which should always
   * be BINARY sketch). Also check sketch size and seed params if they are passed in.
   *
   * @see org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver
   * #getEvaluator(org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo)
   *
   * @param info The parameter info to validate
   * @return The GenericUDAFEvaluator to use to compute the function.
   */
  @Override
  public GenericUDAFEvaluator getEvaluator(final GenericUDAFParameterInfo info) throws SemanticException {
    final ObjectInspector[] parameters = info.getParameterObjectInspectors();

    if (parameters.length < 1) {
      throw new UDFArgumentException("Please specify at least 1 argument");
    }

    if (parameters.length > 3) {
      throw new UDFArgumentTypeException(parameters.length - 1, "Please specify no more than 3 arguments");
    }

    ObjectInspectorValidator.validateGivenPrimitiveCategory(parameters[0], 0, PrimitiveCategory.BINARY);

    if (parameters.length > 1) {
      ObjectInspectorValidator.validateIntegralParameter(parameters[1], 1);
    }

    if (parameters.length > 2) {
      ObjectInspectorValidator.validateIntegralParameter(parameters[2], 2);
    }
    return new UnionSketchUDAFEvaluator();
  }

  /**
   * Evaluator class of Generic UDAF, main logic of our UDAF.
   *
   */
  public static class UnionSketchUDAFEvaluator extends UnionEvaluator {

    /**
     * Receives the passed in argument object inspectors and returns the desired
     * return type's object inspector to inform hive of return type of UDAF.
     *
     * @param mode
     *          Mode (i.e. PARTIAL 1, COMPLETE...) for determining input/output
     *          object inspector type.
     * @param parameters
     *          List of object inspectors for input arguments.
     * @return The object inspector type indicates the UDAF return type (i.e.
     *         returned type of terminate(...)).
     */
    @Override
    public ObjectInspector init(final Mode mode, final ObjectInspector[] parameters) throws HiveException {
      super.init(mode, parameters);

      if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
        inputObjectInspector = (PrimitiveObjectInspector) parameters[0];
        if (parameters.length > 1) {
          nominalEntriesObjectInspector = (PrimitiveObjectInspector) parameters[1];
        }
        if (parameters.length > 2) {
          seedObjectInspector = (PrimitiveObjectInspector) parameters[2];
        }
      } else {
        // mode = partial2 || final
        intermediateObjectInspector = (StandardStructObjectInspector) parameters[0];
      }

      if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
        // intermediate results need to include the nominal number of entries and the seed
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

    /**
     * Add the incoming sketch into the internal state.
     *
     * @param agg
     *          aggregation buffer storing intermediate results.
     * @param parameters
     *          sketches in the form of Object passed in to be merged.
     */
    @Override
    public void iterate(final @SuppressWarnings("deprecation") AggregationBuffer agg,
        final Object[] parameters) throws HiveException {
      if (parameters[0] == null) { return; }
      final UnionState state = (UnionState) agg;
      if (!state.isInitialized()) {
        initializeState(state, parameters);
      }
      final byte[] serializedSketch = (byte[]) inputObjectInspector.getPrimitiveJavaObject(parameters[0]);
      if (serializedSketch == null) { return; }
      state.update(Memory.wrap(serializedSketch));
    }

    private void initializeState(final UnionState state, final Object[] parameters) {
      int nominalEntries = DEFAULT_NOMINAL_ENTRIES;
      if (nominalEntriesObjectInspector != null) {
        nominalEntries = PrimitiveObjectInspectorUtils.getInt(parameters[1], nominalEntriesObjectInspector);
      }
      long seed = DEFAULT_UPDATE_SEED;
      if (seedObjectInspector != null) {
        seed = PrimitiveObjectInspectorUtils.getLong(parameters[2], seedObjectInspector);
      }
      state.init(nominalEntries, seed);
    }

  }

}
