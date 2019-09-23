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
import org.apache.datasketches.tuple.Sketches;
import org.apache.datasketches.tuple.Summary;
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

/**
 * Generic implementation to be sub-classed with a particular type of Summary
 */
public abstract class UnionSketchUDAF extends AbstractGenericUDAFResolver {

  @Override
  public GenericUDAFEvaluator getEvaluator(final GenericUDAFParameterInfo info) throws SemanticException {
    final ObjectInspector[] inspectors = info.getParameterObjectInspectors();

    if (inspectors.length < 1) {
      throw new UDFArgumentException("Expected at least 1 argument");
    }
    ObjectInspectorValidator.validateGivenPrimitiveCategory(inspectors[0], 0, PrimitiveCategory.BINARY);

    // nominal number of entries
    if (inspectors.length > 1) {
      ObjectInspectorValidator.validateIntegralParameter(inspectors[1], 1);
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
    if (inspectors.length > 2) {
      throw new UDFArgumentException("Expected no more than 2 arguments");
    }
  }

  /**
   * This is needed because a concrete UDAF is going to have its own concrete evaluator static inner class.
   * @return an instance of evaluator
   */
  public abstract GenericUDAFEvaluator createEvaluator();

  @SuppressWarnings("javadoc")
  public static abstract class UnionSketchEvaluator<S extends Summary> extends SketchEvaluator<S> {

    private PrimitiveObjectInspector sketchInspector_;

    @Override
    public ObjectInspector init(final Mode mode, final ObjectInspector[] inspectors) throws HiveException {
      super.init(mode, inspectors);
      if ((mode == Mode.PARTIAL1) || (mode == Mode.COMPLETE)) {
        // input is original data
        sketchInspector_ = (PrimitiveObjectInspector) inspectors[0];
        if (inspectors.length > 1) {
          nominalNumEntriesInspector_ = (PrimitiveObjectInspector) inspectors[1];
        }
      } else {
        // input for PARTIAL2 and FINAL is the output from PARTIAL1
        intermediateInspector_ = (StructObjectInspector) inspectors[0];
      }

      if ((mode == Mode.PARTIAL1) || (mode == Mode.PARTIAL2)) {
        // intermediate results need to include the the nominal number of entries
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
      final UnionState<S> state = (UnionState<S>) buf;
      if (!state.isInitialized()) {
        initializeState(state, data);
      }
      final byte[] serializedSketch = (byte[]) sketchInspector_.getPrimitiveJavaObject(data[0]);
      if (serializedSketch == null) { return; }
      state.update(Sketches.heapifySketch(Memory.wrap(serializedSketch), getSummaryDeserializer()));
    }

    protected void initializeState(final UnionState<S> state, final Object[] data) {
      int nominalNumEntries = DEFAULT_NOMINAL_ENTRIES;
      if (nominalNumEntriesInspector_ != null) {
        nominalNumEntries = PrimitiveObjectInspectorUtils.getInt(data[1], nominalNumEntriesInspector_);
      }
      state.init(nominalNumEntries, getSummarySetOperationsForIterate(data));
    }

    @SuppressWarnings("deprecation")
    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      return new UnionState<S>();
    }

  }

}
