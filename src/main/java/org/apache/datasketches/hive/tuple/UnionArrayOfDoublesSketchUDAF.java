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

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.tuple.ArrayOfDoublesSketches;
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
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

@Description(
  name = "UnionArrayOfDoublesSketch",
  value = "_FUNC_(sketch, nominal number of entries, number of values)",
  extended = "Returns an ArrayOfDoublesSketch as a binary blob that can be operated on by other"
    + " ArrayOfDoublesSketch related functions. "
    + "The nominal number of entries is optional, must be a power of 2,"
    + " does not have to match the input sketches, and controls the relative error expected"
    + " from the sketch. A number of 16384 can be expected to yield errors of roughly +-1.5% in"
    + " the estimation of uniques. The default number is defined in the sketches-core library,"
    + " and at the time of this writing was 4096 (about 3% error)."
    + " The number of values is optional and must match all input sketches (defaults to 1)")
@SuppressWarnings("javadoc")
public class UnionArrayOfDoublesSketchUDAF extends AbstractGenericUDAFResolver {

  @Override
  public GenericUDAFEvaluator getEvaluator(final GenericUDAFParameterInfo info) throws SemanticException {
    final ObjectInspector[] inspectors = info.getParameterObjectInspectors();

    if (inspectors.length < 1) {
      throw new UDFArgumentException("Expected at least 1 argument");
    }
    if (inspectors.length > 3) {
      throw new UDFArgumentTypeException(inspectors.length - 1, "Expected no more than 3 arguments");
    }

    ObjectInspectorValidator.validateGivenPrimitiveCategory(inspectors[0], 0, PrimitiveCategory.BINARY);

    // nominal number of entries
    if (inspectors.length > 1) {
      ObjectInspectorValidator.validateIntegralParameter(inspectors[1], 1);
    }

    // number of double values per key
    if (inspectors.length > 2) {
      ObjectInspectorValidator.validateIntegralParameter(inspectors[2], 2);
    }

    return new UnionArrayOfDoublesSketchEvaluator();
  }

  public static class UnionArrayOfDoublesSketchEvaluator extends ArrayOfDoublesSketchEvaluator {

    private static final int DEFAULT_NUM_VALUES = 1;

    private PrimitiveObjectInspector sketchInspector_;
    private PrimitiveObjectInspector numValuesInspector_;

    @Override
    public ObjectInspector init(final Mode mode, final ObjectInspector[] inspectors) throws HiveException {
      super.init(mode, inspectors);
      if ((mode == Mode.PARTIAL1) || (mode == Mode.COMPLETE)) {
        // input is original data
        sketchInspector_ = (PrimitiveObjectInspector) inspectors[0];
        if (inspectors.length > 1) {
          nominalNumEntriesInspector_ = (PrimitiveObjectInspector) inspectors[1];
        }
        if (inspectors.length > 2) {
          numValuesInspector_ = (PrimitiveObjectInspector) inspectors[2];
        }
      } else {
        // input for PARTIAL2 and FINAL is the output from PARTIAL1
        intermediateInspector_ = (StructObjectInspector) inspectors[0];
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
    public void iterate(final @SuppressWarnings("deprecation") AggregationBuffer buf, final Object[] data)
        throws HiveException {
      if (data[0] == null) { return; }
      final ArrayOfDoublesUnionState state = (ArrayOfDoublesUnionState) buf;
      if (!state.isInitialized()) {
        initializeState(state, data);
      }
      final byte[] serializedSketch = (byte[]) sketchInspector_.getPrimitiveJavaObject(data[0]);
      if (serializedSketch == null) { return; }
      state.update(ArrayOfDoublesSketches.wrapSketch(Memory.wrap(serializedSketch)));
    }

    private void initializeState(final ArrayOfDoublesUnionState state, final Object[] data) {
      int nominalNumEntries = DEFAULT_NOMINAL_ENTRIES;
      if (nominalNumEntriesInspector_ != null) {
        nominalNumEntries = PrimitiveObjectInspectorUtils.getInt(data[1], nominalNumEntriesInspector_);
      }
      int numValues = DEFAULT_NUM_VALUES;
      if (numValuesInspector_ != null) {
        numValues = PrimitiveObjectInspectorUtils.getInt(data[2], numValuesInspector_);
      }
      state.init(nominalNumEntries, numValues);
    }

    @SuppressWarnings("deprecation")
    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      return new ArrayOfDoublesUnionState();
    }

  }

}
