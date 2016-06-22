/*******************************************************************************
 * Copyright 2016, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 *******************************************************************************/
package com.yahoo.sketches.hive.theta;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.CompactSketch;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.Intersection;

@Description(
    name = "intersectSketch", 
    value = "_FUNC_(sketch) - Compute the intersection of sketches",
    extended = "Example:\n"
    + "> SELECT intersectSketch(sketch) FROM src;\n"
    + "The return value is a binary blob that contains a compact sketch, which can "
    + "be operated on by the other sketch-related functions.")
public class IntersectSketchUDAF extends AbstractGenericUDAFResolver {

  @Override
  public GenericUDAFEvaluator getEvaluator(final GenericUDAFParameterInfo info) throws SemanticException {
    final ObjectInspector[] inspectors = info.getParameterObjectInspectors();
    if (inspectors.length != 1) throw new UDFArgumentException("One argument expected");
    if (inspectors[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0, "Primitive argument expected, but "
          + inspectors[0].getTypeName() + " was recieved");
    }
    final PrimitiveObjectInspector inspector = (PrimitiveObjectInspector) inspectors[0];
    if (inspector.getPrimitiveCategory() != PrimitiveCategory.BINARY) {
      throw new UDFArgumentTypeException(0, "Argument must be a sketch, but "
          + inspector.getPrimitiveCategory().name() + " was received");
    }
    return new IntersectSketchUDAFEvaluator();
  }

  static class IntersectSketchUDAFEvaluator extends GenericUDAFEvaluator {

    private PrimitiveObjectInspector inputObjectInspector;

    @Override
    public ObjectInspector init(final Mode mode, final ObjectInspector[] parameters) throws HiveException {
      super.init(mode, parameters);
      inputObjectInspector = (PrimitiveObjectInspector) parameters[0];
      return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY);
    }

    @Override
    public void iterate(final @SuppressWarnings("deprecation") AggregationBuffer buf, final Object[] data) throws HiveException {
      merge(buf, data[0]);
    }

    @Override
    public Object terminatePartial(final @SuppressWarnings("deprecation") AggregationBuffer buf) throws HiveException {
      return terminate(buf);
    }

    @Override
    public void merge(final @SuppressWarnings("deprecation") AggregationBuffer buf, final Object data) throws HiveException {
      if (data == null) return;
      final IntersectionState state = (IntersectionState) buf;
      final BytesWritable serializedSketch = (BytesWritable) inputObjectInspector.getPrimitiveWritableObject(data);
      state.update(serializedSketch.getBytes());
    }

    @Override
    public Object terminate(final @SuppressWarnings("deprecation") AggregationBuffer buf) throws HiveException {
      final IntersectionState state = (IntersectionState) buf;
      final CompactSketch resultSketch = state.getResult();
      if (resultSketch == null) return null;
      return new BytesWritable(resultSketch.toByteArray());
    }

    @SuppressWarnings("deprecation")
    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      return new IntersectionState();
    }

    @Override
    public void reset(final @SuppressWarnings("deprecation") AggregationBuffer buf) throws HiveException {
      final IntersectionState state = (IntersectionState) buf;
      state.reset();
    }

    static class IntersectionState extends AbstractAggregationBuffer {

      private Intersection intersection;

      void update(final byte[] serializedSketch) {
        if (intersection == null) intersection = SetOperation.builder().buildIntersection();
        intersection.update(Sketches.wrapSketch(new NativeMemory(serializedSketch)));
      }

      CompactSketch getResult() {
        if (intersection == null) return null;
        return intersection.getResult();
      }

      void reset() {
        intersection = null;
      }

    }

  }

}
