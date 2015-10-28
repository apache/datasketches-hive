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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;
import java.util.List;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.CompactSketch;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Union;
import com.yahoo.sketches.theta.UpdateSketch;

/**
 * Hive Generic UDAF Resolver Class for MergeSketchUDAF.
 *
 */
@Description(name = "dataToSketch", value = "_FUNC_(expr, size, prob) - Compute a sketch of size 'size' and sampling probability 'prob' on data 'expr'", extended = "Example:\n"
    + "> SELECT dataToSketch(val, 1024, 1.0) FROM src;\n"
    + "The return value is a binary blob that can be operated on by other sketch related operands."
    + "The sketch size must be a power of 2 and controls the relative error expected from the sketch."
    + "A size of 16384 can be expected to yeild errors of roughly +/- 1.5% in the estimation of uniques.")
public class DataToSketchUDAF extends AbstractGenericUDAFResolver {
  public static final int DEFAULT_SKETCH_SIZE = 16384;
  public static final float DEFAULT_SAMPLING_PROBABILITY = 1.0f;

  /**
   * Performs argument number and type validation. DataToSketch expects
   * to recieve between one and three arguments. The first (required)
   * is the value to add to the sketch and must be a primitive. The second
   * (optional) is the sketch size to use. This must be an integral value
   * and should be constant. If not constant, the first row processed 
   * provides the size. The third (optional) is the sampling probablility
   * and is a floating point value between 0.0 and 1.0.
   *  
   * @see org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver#getEvaluator(org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo)
   * 
   * @param info Parameter info to validate
   * @return The GenericUDAFEvaluator that should be used to calculate the function.
   */
  @Override
  public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
    ObjectInspector[] parameters = info.getParameterObjectInspectors();

    // Validate the correct number of parameters
    if (parameters.length < 1) {
      throw new UDFArgumentException("Please specify at least 1 argument");
    }

    if (parameters.length > 3) {
      throw new UDFArgumentException("Please specify no more than 3 arguments");
    }

    // Validate first parameter type
    if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0, "Only primitive type arguments are accepted but "
          + parameters[0].getTypeName() + " was passed as parameter 1");
    }

    // Validate second argument if present
    if (parameters.length > 1) {
      if (parameters[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
        throw new UDFArgumentTypeException(1, "Only primitive integral arguments are accepted but "
            + parameters[1].getTypeName() + " was passed as parameter 2 (sketch size)");
      }
      switch (((PrimitiveObjectInspector) parameters[1]).getPrimitiveCategory()) {
      // supported integral types
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        break;
      // all other types are invalid
      default:
        throw new UDFArgumentTypeException(1, "Only integral type arguments are accepted but "
            + parameters[1].getTypeName() + " was passed as parameter 1.");
      }

      // Also make sure it is a constant.
      if (!ObjectInspectorUtils.isConstantObjectInspector(parameters[1])) {
        throw new UDFArgumentTypeException(1, "The second argument must be a constant, but "
            + parameters[1].getTypeName() + " was passed instead.");
      }
    }

    // Validate third argument if present
    if (parameters.length > 2) {
      if (parameters[2].getCategory() != ObjectInspector.Category.PRIMITIVE) {
        throw new UDFArgumentTypeException(2, "Only primitive floating point arguments are accepted but "
            + parameters[2].getTypeName() + " was passed as parameter 3 (sampling probablity)");
      }
      switch (((PrimitiveObjectInspector) parameters[2]).getPrimitiveCategory()) {
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
        break;
      default:
        throw new UDFArgumentTypeException(2, "Only floating point type arguments between 0 and 1 are accepted but "
            + parameters[2].getTypeName() + " was passed as parameter 2");
      }
      // Also make sure it is a constant.
      if (!ObjectInspectorUtils.isConstantObjectInspector(parameters[2])) {
        throw new UDFArgumentTypeException(1, "The third argument must be a constant, but "
            + parameters[2].getTypeName() + " was passed instead.");
      }
    }

    return new DataToSketchEvaluator();
  }

  public static class DataToSketchEvaluator extends GenericUDAFEvaluator {

    private static final String SKETCH_FIELD = "sketch";
    private static final String SAMPLING_PROBABLILTY_FIELD = "samplingProbability";
    private static final String SKETCH_SIZE_FIELD = "sketchSize";

    // FOR PARTIAL1 and COMPLETE modes: ObjectInspectors for original data
    private PrimitiveObjectInspector inputOI;
    private transient PrimitiveObjectInspector sketchSizeOI;
    private transient PrimitiveObjectInspector samplingProbOI;

    // FOR PARTIAL2 and FINAL modes: ObjectInspectors for partial aggregations
    private transient StandardStructObjectInspector intermediateOI;

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator#init(org.apache
     * .hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode,
     * org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector[])
     */
    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);

      if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
        // capture the inputs for partial and complete modes (standard
        // arguments)
        inputOI = (PrimitiveObjectInspector) parameters[0];
        if (parameters.length > 1) {
          sketchSizeOI = (PrimitiveObjectInspector) parameters[1];
        }
        if (parameters.length > 2) {
          samplingProbOI = (PrimitiveObjectInspector) parameters[2];
        }
        intermediateOI = null;
      } else {
        // capture the inputs for partial2 and FINAL modes
        // Inputs for partial2 and final should be the same as the output from
        // partial1 or complete
        inputOI = null;
        sketchSizeOI = null;
        samplingProbOI = null;
        intermediateOI = (StandardStructObjectInspector) parameters[0];
      }

      if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
        // intermediate results need to include the sketch as well as the size
        // and sampling probability
        List<ObjectInspector> fields = new ArrayList<>();
        fields.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.INT));
        fields.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.FLOAT));
        fields.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY));
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add(SKETCH_SIZE_FIELD);
        fieldNames.add(SAMPLING_PROBABLILTY_FIELD);
        fieldNames.add(SKETCH_FIELD);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fields);
      } else {
        // final results include just the sketch
        return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY);
      }
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
    public void iterate(@SuppressWarnings("deprecation") AggregationBuffer agg, Object[] parameters)
        throws HiveException {
      // don't enter null's into the sketch.
      if (parameters[0] == null) {
        return;
      }

      DataToSketchAggBuffer buf = (DataToSketchAggBuffer) agg;

      if (buf.getUpdateSketch() == null) {
        // need to initialize the sketch since nothing has been added yet
        int sketchSize = DEFAULT_SKETCH_SIZE;
        if (sketchSizeOI != null) {
          sketchSize = PrimitiveObjectInspectorUtils.getInt(parameters[1], sketchSizeOI);
        } 
        buf.setSketchSize(sketchSize);
        
        float samplingProbability = DEFAULT_SAMPLING_PROBABILITY;
        if (samplingProbOI != null) {
          samplingProbability = PrimitiveObjectInspectorUtils.getFloat(parameters[2], samplingProbOI);
        }
        buf.setSamplingProbability(samplingProbability);
        
        buf.setUpdateSketch(UpdateSketch.builder().setP(samplingProbability).setFamily(Family.QUICKSELECT)
            .build(sketchSize));
      }

      updateData(inputOI, parameters[0], buf.getUpdateSketch());
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator#terminatePartial
     * (
     * org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer
     * )
     */
    @Override
    public Object terminatePartial(@SuppressWarnings("deprecation") AggregationBuffer agg) throws HiveException {
      // return the bytes associated with this sketch, compacted for easier
      // merging
      DataToSketchAggBuffer buf = (DataToSketchAggBuffer) agg;
      
      CompactSketch intermediate = null;
      UpdateSketch update = buf.getUpdateSketch();
      Union union = buf.getUnion();
      
      if (update != null) {
        // PARTIAL1 case - hive calls iterate then terminatePartial
        intermediate = update.compact(true,null);
      } else if (union != null) {
        // PARTIAL2 case - hive calls merge then terminatePartial
        intermediate = union.getResult(true, null);
      }
      
      if (intermediate != null) {
        // we had results, so return them
        byte[] bytes = intermediate.toByteArray();

        ArrayList<Object> results = new ArrayList<>();
        results.add(new IntWritable(buf.getSketchSize()));
        results.add(new FloatWritable(buf.getSamplingProbability()));
        results.add(new BytesWritable(bytes));
        
        return results;
      } else {
        return null;
      }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator#merge(org.
     * apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer,
     * java.lang.Object)
     */
    @Override
    public void merge(@SuppressWarnings("deprecation") AggregationBuffer agg, Object partial) throws HiveException {
      if (partial == null) {
        return;
      }
      DataToSketchAggBuffer buf = (DataToSketchAggBuffer) agg;

      if (buf.getUnion() == null) {
        // nothing has been included yet, so initialize the buffer
        buf.setSketchSize(((IntWritable) intermediateOI.getStructFieldData(partial,
            intermediateOI.getStructFieldRef(SKETCH_SIZE_FIELD))).get());
        buf.setSamplingProbability(((FloatWritable) intermediateOI.getStructFieldData(partial,
            intermediateOI.getStructFieldRef(SAMPLING_PROBABLILTY_FIELD))).get());

        buf.setUnion(SetOperation.builder().setP(buf.getSamplingProbability()).buildUnion(buf.getSketchSize()));
      }

      BytesWritable serializedSketch = ((BytesWritable) intermediateOI.getStructFieldData(partial,
          intermediateOI.getStructFieldRef(SKETCH_FIELD)));
      Memory sketchMem = new NativeMemory(serializedSketch.getBytes());

      buf.getUnion().update(sketchMem);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator#terminate(
     * org.apache
     * .hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer)
     */
    @Override
    public Object terminate(@SuppressWarnings("deprecation") AggregationBuffer agg) throws HiveException {
      DataToSketchAggBuffer buf = (DataToSketchAggBuffer) agg;

      CompactSketch result;
      
      if (buf.getUnion() != null) {
        result = buf.getUnion().getResult(true, null);
      } else if (buf.getUpdateSketch() != null) {
        result = buf.getUpdateSketch().compact(true, null);
      } else {
        // sketch and union are both invalid. there was nothing merged
        return null;
      }

      if (result.getRetainedEntries(false) == 0) {
        // no entries in the sketch. This means that no entries were inserted
        // to be consistent with SQL, null should be returned.
        return null;
      } else {
        BytesWritable retVal = new BytesWritable(result.toByteArray());
        return retVal;
      }
    }

    /**
     * Aggregation buffer for DataToSketch operations. Contains an update sketch
     * that will be updated for each row.
     */
    @AggregationType(estimable = true)
    static class DataToSketchAggBuffer extends AbstractAggregationBuffer {
      private int sketchSize;
      private float samplingProbability;
      private UpdateSketch sketch;
      private Union union;

      @Override
      public int estimate() {
        int s = sketch != null ? sketch.getCurrentBytes(false) : 0;
        int u = SetOperation.getMaxUnionBytes(sketchSize);

        return s + u;
      }

      public int getSketchSize() {
        return sketchSize;
      }

      public void setSketchSize(int sketchSize) {
        this.sketchSize = sketchSize;
      }

      public float getSamplingProbability() {
        return samplingProbability;
      }

      public void setSamplingProbability(float samplingProbability) {
        this.samplingProbability = samplingProbability;
      }

      public UpdateSketch getUpdateSketch() {
        return sketch;
      }

      public void setUpdateSketch(UpdateSketch sketch) {
        this.sketch = sketch;
      }

      public Union getUnion() {
        return union;
      }

      public void setUnion(Union union) {
        this.union = union;
      }
    }

    /**
     * 
     * @see org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator#getNewAggregationBuffer()
     */
    @SuppressWarnings("deprecation")
    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      DataToSketchAggBuffer buf = new DataToSketchAggBuffer();
      reset(buf);

      return buf;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator#reset(org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer)
     */
    @Override
    public void reset(@SuppressWarnings("deprecation") AggregationBuffer agg) throws HiveException {
      DataToSketchAggBuffer buf = (DataToSketchAggBuffer) agg;
      buf.setSketchSize(DEFAULT_SKETCH_SIZE);
      buf.setSamplingProbability(DEFAULT_SAMPLING_PROBABILITY);
      buf.setUpdateSketch(null);
      buf.setUnion(null);
    }

    private static void updateData(PrimitiveObjectInspector oi, Object data, UpdateSketch sketch) {

      switch (oi.getPrimitiveCategory()) {
      case BINARY:
        sketch.update(PrimitiveObjectInspectorUtils.getBinary(data,
            PrimitiveObjectInspectorFactory.writableBinaryObjectInspector).getBytes());
        return;
      case BYTE:
        sketch.update(PrimitiveObjectInspectorUtils.getByte(data,
            PrimitiveObjectInspectorFactory.writableByteObjectInspector));
        return;
      case DOUBLE:
        sketch.update(PrimitiveObjectInspectorUtils.getDouble(data,
            PrimitiveObjectInspectorFactory.writableDoubleObjectInspector));
        return;
      case FLOAT:
        sketch.update(PrimitiveObjectInspectorUtils.getFloat(data,
            PrimitiveObjectInspectorFactory.writableFloatObjectInspector));
        return;
      case INT:
        sketch.update(PrimitiveObjectInspectorUtils.getInt(data,
            PrimitiveObjectInspectorFactory.writableIntObjectInspector));
        return;
      case LONG:
        sketch.update(PrimitiveObjectInspectorUtils.getLong(data,
            PrimitiveObjectInspectorFactory.writableLongObjectInspector));
        return;
      case STRING:
        sketch.update(PrimitiveObjectInspectorUtils.getString(data,
            PrimitiveObjectInspectorFactory.writableStringObjectInspector));
        return;
      default:
        throw new IllegalArgumentException(
            "Unrecongnized input data type, please use data of type binary, byte, double, float, int, long, or string only.");
      }
    }

    PrimitiveObjectInspector getInputOI() {
      return inputOI;
    }

    PrimitiveObjectInspector getSketchSizeOI() {
      return sketchSizeOI;
    }

    PrimitiveObjectInspector getSamplingProbOI() {
      return samplingProbOI;
    }

    StandardStructObjectInspector getIntermediateOI() {
      return intermediateOI;
    }
  }
}
