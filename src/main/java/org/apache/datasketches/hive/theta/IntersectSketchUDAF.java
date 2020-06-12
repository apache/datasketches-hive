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

import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;

import java.util.Arrays;

import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.Intersection;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
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
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;

@Description(
    name = "intersectSketch",
    value = "_FUNC_(sketch, seed) - Compute the intersection of sketches",
    extended = "Example:\n"
    + "> SELECT intersectSketch(sketch) FROM src;\n"
    + "The return value is a binary blob that contains a compact sketch, which can "
    + "be operated on by the other sketch-related functions. "
    + "The seed is optional, "
    + "and using it is not recommended unless you really know why you need it.")
@SuppressWarnings("javadoc")
public class IntersectSketchUDAF extends AbstractGenericUDAFResolver {

  @Override
  public GenericUDAFEvaluator getEvaluator(final GenericUDAFParameterInfo info)
      throws SemanticException {
    final ObjectInspector[] inspectors = info.getParameterObjectInspectors();
    if (inspectors.length < 1) {
      throw new UDFArgumentException("Please specify at least 1 argument");
    }
    if (inspectors.length > 2) {
      throw new
      UDFArgumentTypeException(inspectors.length - 1, "Please specify no more than 2 arguments");
    }
    ObjectInspectorValidator.validateGivenPrimitiveCategory(inspectors[0], 0,
        PrimitiveCategory.BINARY);
    if (inspectors.length > 1) {
      ObjectInspectorValidator.validateIntegralParameter(inspectors[1], 1);
    }
    return new IntersectSketchUDAFEvaluator();
  }

  static class IntersectSketchUDAFEvaluator extends GenericUDAFEvaluator {
    protected static final String SEED_FIELD = "seed";
    protected static final String SKETCH_FIELD = "sketch";

    // FOR PARTIAL1 and COMPLETE modes: ObjectInspectors for original data
    private transient PrimitiveObjectInspector inputObjectInspector;
    protected transient PrimitiveObjectInspector seedObjectInspector;

    // FOR PARTIAL2 and FINAL modes: ObjectInspectors for partial aggregations
    protected transient StructObjectInspector intermediateObjectInspector;

    @Override
    public ObjectInspector init(final Mode mode, final ObjectInspector[] parameters) throws HiveException {
      super.init(mode, parameters);
      if ((mode == Mode.PARTIAL1) || (mode == Mode.COMPLETE)) {
        inputObjectInspector = (PrimitiveObjectInspector) parameters[0];
        if (parameters.length > 1) {
          seedObjectInspector = (PrimitiveObjectInspector) parameters[1];
        }
      } else {
        intermediateObjectInspector = (StandardStructObjectInspector) parameters[0];
      }

      if ((mode == Mode.PARTIAL1) || (mode == Mode.PARTIAL2)) {
        // intermediate results need to include the seed
        return ObjectInspectorFactory.getStandardStructObjectInspector(
          Arrays.asList(SEED_FIELD, SKETCH_FIELD),
          Arrays.asList(
            PrimitiveObjectInspectorFactory
              .getPrimitiveWritableObjectInspector(PrimitiveCategory.LONG),
            PrimitiveObjectInspectorFactory
              .getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY)
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
      final IntersectionState state = (IntersectionState) buf;
      if (!state.isInitialized()) {
        long seed = DEFAULT_UPDATE_SEED;
        if (seedObjectInspector != null) {
          seed = PrimitiveObjectInspectorUtils.getLong(data[1], seedObjectInspector);
        }
        state.init(seed);
      }
      final byte[] serializedSketch = (byte[]) inputObjectInspector.getPrimitiveJavaObject(data[0]);
      if (serializedSketch == null) { return; }
      state.update(Memory.wrap(serializedSketch));
    }

    @Override
    public Object terminatePartial(final @SuppressWarnings("deprecation") AggregationBuffer buf)
        throws HiveException {
      final IntersectionState state = (IntersectionState) buf;
      final Sketch intermediate = state.getResult();
      if (intermediate == null) { return null; }
      final byte[] bytes = intermediate.toByteArray();
      return Arrays.asList(
        new LongWritable(state.getSeed()),
        new BytesWritable(bytes)
      );
    }

    @Override
    public void merge(final @SuppressWarnings("deprecation") AggregationBuffer buf,
        final Object data) throws HiveException {
      if (data == null) { return; }
      final IntersectionState state = (IntersectionState) buf;
      if (!state.isInitialized()) {
        final long seed = ((LongWritable) intermediateObjectInspector.getStructFieldData(
            data, intermediateObjectInspector.getStructFieldRef(SEED_FIELD))).get();
        state.init(seed);
      }

      final Memory serializedSketch = BytesWritableHelper.wrapAsMemory(
          (BytesWritable) intermediateObjectInspector.getStructFieldData(
          data, intermediateObjectInspector.getStructFieldRef(SKETCH_FIELD)));
      state.update(serializedSketch);
    }

    @Override
    public Object terminate(final @SuppressWarnings("deprecation") AggregationBuffer buf)
        throws HiveException {
      final IntersectionState state = (IntersectionState) buf;
      final Sketch resultSketch = state.getResult();
      if (resultSketch == null) { return null; }
      return new BytesWritable(resultSketch.toByteArray());
    }

    @SuppressWarnings("deprecation")
    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      return new IntersectionState();
    }

    @Override
    public void reset(final @SuppressWarnings("deprecation") AggregationBuffer buf)
        throws HiveException {
      final IntersectionState state = (IntersectionState) buf;
      state.reset();
    }

    static class IntersectionState extends AbstractAggregationBuffer {
      private long seed_;
      private Intersection intersection_;

      boolean isInitialized() {
        return intersection_ != null;
      }

      void init(final long seed) {
        seed_ = seed;
        intersection_ = SetOperation.builder().setSeed(seed).buildIntersection();
      }

      long getSeed() {
        return seed_;
      }

      void update(final Memory serializedSketch) {
        intersection_.update(Sketches.wrapSketch(serializedSketch, seed_));
      }

      Sketch getResult() {
        if (intersection_ == null) { return null; }
        return intersection_.getResult();
      }

      void reset() {
        intersection_ = null;
      }
    }
  }
}
