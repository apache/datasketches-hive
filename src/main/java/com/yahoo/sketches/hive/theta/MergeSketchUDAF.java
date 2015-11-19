/*******************************************************************************
 * Copyright 2015, Yahoo Inc.
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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;
import java.util.List;

import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.CompactSketch;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Union;

/**
 * Hive Generic UDAF Resolver Class for MergeSketchUDAF.
 *
 */
@Description(
    name = "mergeSketch", 
    value = "_FUNC_(sketch, size) - Compute the union of sketches using a result with size 'size'", 
    extended = "Example:\n"
    + "> SELECT mergeSketch(sketch, 1024) FROM src;\n"
    + "The return value is a binary blob that contains a compact sketch which can "
    + "be operated on by the other sketch related operands. The sketch "
    + "size must be a power of 2 and controls the relative error of the expected "
    + "result. A size of 16384 (the default) can be expected to yeild errors of roughly +/- 1.5% "
    + "in the estimation of uniques.")
public class MergeSketchUDAF extends AbstractGenericUDAFResolver {
  public static final int DEFAULT_SKETCH_SIZE = 16384;

  /**
   * Perform argument count check and argument type checking, returns an
   * appropriate evaluator to perform based on input type (which should always
   * be BINARY sketch for now). Also update sketch_size if size argument is
   * passed in.
   * 
   * @see org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver#getEvaluator(org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo)
   * 
   * @param info
   *          The parameter info to validate
   * @return The GenericUDAFEvaluator to use to compute the function.
   */
  @Override
  public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
    ObjectInspector[] parameters = info.getParameterObjectInspectors();

    // Check number of arguments passed in, merge sketch only allow one or two
    // column/(int)
    if (parameters.length < 1) {
      throw new UDFArgumentException("Please specify at least 1 argument");
    }

    if (parameters.length > 2) {
      throw new UDFArgumentTypeException(parameters.length - 1, "Please specify no more than 2 arguments");
    }

    // validate first parameter type (sketch to merge)
    if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0, "Only primitive type arguments are accepted but "
          + parameters[0].getTypeName() + " was passed as parameter 1");
    }

    if (((PrimitiveObjectInspector) parameters[0]).getPrimitiveCategory() != PrimitiveCategory.BINARY) {
      throw new UDFArgumentTypeException(0, "First argument must be a sketch to merge");
    }

    if (parameters.length > 1) {
      if (parameters[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
        throw new UDFArgumentTypeException(1, "Only integral type arguments are accepted but "
            + parameters[1].getTypeName() + " was passed as parameter 1.");
      }
      switch (((PrimitiveObjectInspector) parameters[1]).getPrimitiveCategory()) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        break;
      // all other types are invalid
      default:
        throw new UDFArgumentTypeException(1, "Only integral type assignments are accepted but "
            + parameters[1].getTypeName() + " was passed as paramete 2");
      }
    }

    return new MergeSketchUDAFEvaluator();
  }

  /**
   * Evaluator class of Generic UDAF, main logic of our UDAF.
   * 
   */
  public static class MergeSketchUDAFEvaluator extends GenericUDAFEvaluator {

    private static final String SKETCH_FIELD = "sketch";
    private static final String SKETCH_SIZE_FIELD = "sketchSize";

    // FOR PARTIAL1 and COMPLETE modes, expect ObjectInspectors for the
    // prime api: incoming sketch to merge and sketch size
    private PrimitiveObjectInspector inputOI;
    private transient PrimitiveObjectInspector sketchSizeOI;

    // Intermediate reports
    private transient StandardStructObjectInspector intermediateOI;

    /**
     * Receives the passed in argument object inspectors and returns the desired
     * return type's object inspector to inform hive of return type of UDAF.
     * 
     * @param m
     *          Mode (i.e. PARTIAL 1, COMPLETE...) for determining input/output
     *          object inspector type.
     * @param parameters
     *          List of object inspectors for input arguments.
     * @return The object inspector type indicates the UDAF return type (i.e.
     *         returned type of terminate(...)).
     */
    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);

      if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
        inputOI = (PrimitiveObjectInspector) parameters[0];
        if (parameters.length > 1) {
          sketchSizeOI = (PrimitiveObjectInspector) parameters[1];
        }
        intermediateOI = null;
      } else {
        // mode = partial2 || final
        inputOI = null;
        sketchSizeOI = null;
        intermediateOI = (StandardStructObjectInspector) parameters[0];
      }

      if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
        // build list object representing intermediate result
        List<ObjectInspector> fields = new ArrayList<>(2);
        fields.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.INT));
        fields.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY));
        List<String> fieldNames = new ArrayList<>(2);
        fieldNames.add(SKETCH_SIZE_FIELD);
        fieldNames.add(SKETCH_FIELD);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fields);
      } else {
        return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY);
      }
    }

    /**
     * Aggregate incoming sketch into aggregation buffer.
     * 
     * @param agg
     *          aggregation buffer storing intermediate results.
     * @param parameters
     *          sketches in the form of Object passed in to be merged.
     */
    @Override
    public void iterate(@SuppressWarnings("deprecation") AggregationBuffer agg, Object[] parameters)
        throws HiveException {
      // don't bother continuing if we have a null.
      if (parameters[0] == null) {
        return;
      }

      MergeSketchAggBuffer buf = (MergeSketchAggBuffer) agg;

      if (buf.getUnion() == null) {
        int sketchSize = DEFAULT_SKETCH_SIZE;
        if (sketchSizeOI != null) {
          sketchSize = PrimitiveObjectInspectorUtils.getInt(parameters[1], sketchSizeOI);
        }
        buf.setSketchSize(sketchSize);

        buf.setUnion(SetOperation.builder().buildUnion(sketchSize));
      }

      byte[] serializedSketch = (byte[]) inputOI.getPrimitiveJavaObject(parameters[0]);

      // skip if input sketch is null
      if (serializedSketch == null) {
        return;
      }

      NativeMemory memorySketch = new NativeMemory(serializedSketch);

      buf.getUnion().update(memorySketch);
    }

    /**
     * Retrieves the intermediate result sketch from aggregation buffer for
     * merging with an other intermediate result.
     * 
     * @param agg
     *          aggregation buffer storing intermediate results.
     * @return Sketch serialized in a bytes writable object.
     */
    @Override
    public Object terminatePartial(@SuppressWarnings("deprecation") AggregationBuffer agg) throws HiveException {

      MergeSketchAggBuffer buf = (MergeSketchAggBuffer) agg;
      Union union = buf.getUnion();
      if (union == null) {
        return null;
      }
      CompactSketch intermediateSketch = union.getResult(true, null);

      if (intermediateSketch.getRetainedEntries(false) == 0) {
        return null;
      } else {
        byte[] bytes = intermediateSketch.toByteArray();

        ArrayList<Object> results = new ArrayList<>(2);
        results.add(new IntWritable(buf.getSketchSize()));
        results.add(new BytesWritable(bytes));

        return results;
      }
    }

    /**
     * Merge two intermediate into aggregation buffer.
     * 
     * @param agg
     *          aggregation buffer where intermediate sketch merge into.
     * @param partial
     *          intermediate sketch passed as bytes writable.
     */
    @Override
    public void merge(@SuppressWarnings("deprecation") AggregationBuffer agg, Object partial) throws HiveException {

      if (partial == null) {
        return;
      }

      MergeSketchAggBuffer buf = (MergeSketchAggBuffer) agg;

      if (buf.getUnion() == null) {
        // nothing has been included yet, so initialize the buffer
        IntWritable sketchSize = (IntWritable) intermediateOI.getStructFieldData(partial,
            intermediateOI.getStructFieldRef(SKETCH_SIZE_FIELD));

        buf.setSketchSize(sketchSize.get());
        buf.setUnion(SetOperation.builder().buildUnion(sketchSize.get()));
      }

      BytesWritable serializedSketch = ((BytesWritable) intermediateOI.getStructFieldData(partial,
          intermediateOI.getStructFieldRef(SKETCH_FIELD)));

      NativeMemory memorySketch = new NativeMemory(serializedSketch.getBytes());

      buf.getUnion().update(memorySketch);
    }

    /**
     * Retrieves the final merged sketch from aggregation buffer to be returned
     * as UDAF result and terminate UDAF.
     * 
     * @param agg
     *          buffer which contains the final merged sketch is stored.
     * @return final sketch in bytes writable form to be returned by the UDAF.
     */
    @Override
    public Object terminate(@SuppressWarnings("deprecation") AggregationBuffer agg) throws HiveException {
      MergeSketchAggBuffer buf = (MergeSketchAggBuffer) agg;

      Union union = buf.getUnion();
      if (union == null) {
        return null;
      }

      CompactSketch result = union.getResult(true, null);

      if (result.getRetainedEntries(false) == 0) {
        // no entries were ever inserted into the sketch, so null
        // should be returned.
        return null;
      } else {
        byte[] serializedSketch = result.toByteArray();
        BytesWritable finalResult = new BytesWritable(serializedSketch);

        return finalResult;
      }
    }

    /**
     * Aggregation buffer class stores the intermediate results of UDAF, which
     * is sketch buffered in SetOperations.
     * 
     * @author weijialuo
     */
    @AggregationType(estimable = true)
    public static class MergeSketchAggBuffer extends AbstractAggregationBuffer {
      private int sketchSize;
      private Union union;

      @Override
      public int estimate() {
        return SetOperation.getMaxUnionBytes(sketchSize);
      }

      int getSketchSize() {
        return sketchSize;
      }

      void setSketchSize(int sketchSize) {
        this.sketchSize = sketchSize;
      }

      Union getUnion() {
        return union;
      }

      void setUnion(Union union) {
        this.union = union;
      }

    }

    /**
     * Retrieves a new aggregation buffer and initialize it, called by hive.
     * 
     * @return a new initialized aggregation buffer.
     */
    @SuppressWarnings("deprecation")
    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      MergeSketchAggBuffer buf = new MergeSketchAggBuffer();
      reset(buf);
      return buf;
    }

    /**
     * Reinitializes an used aggregation buffer for further reuse.
     * 
     * @param agg
     *          old aggregation buffer to be reinitialized.
     */
    @Override
    public void reset(@SuppressWarnings("deprecation") AggregationBuffer agg) throws HiveException {
      MergeSketchAggBuffer buf = (MergeSketchAggBuffer) agg;
      buf.setSketchSize(DEFAULT_SKETCH_SIZE);
      buf.setUnion(null);
    }

    PrimitiveObjectInspector getInputOI() {
      return inputOI;
    }

    PrimitiveObjectInspector getSketchSizeOI() {
      return sketchSizeOI;
    }

    StandardStructObjectInspector getIntermediateOI() {
      return intermediateOI;
    }
  }
}
