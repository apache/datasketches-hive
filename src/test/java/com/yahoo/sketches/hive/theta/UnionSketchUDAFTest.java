/*******************************************************************************
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 *******************************************************************************/
package com.yahoo.sketches.hive.theta;

import static com.yahoo.sketches.Util.DEFAULT_NOMINAL_ENTRIES;
import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.UpdateSketch;

/**
 * Unit tests for UnionSketch UDF
 */
@SuppressWarnings("deprecation")
public class UnionSketchUDAFTest {

  static final ObjectInspector intInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.INT);

  static final ObjectInspector longInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.LONG);

  static final ObjectInspector binaryInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY);

  static final ObjectInspector structInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
      Arrays.asList("nominalEntries", "seed", "sketch"),
      Arrays.asList(intInspector, longInspector, binaryInspector)
    );

  @Test(expectedExceptions = { UDFArgumentException.class })
  public void initTooFewArgs() throws SemanticException {
    UnionSketchUDAF udf = new UnionSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(new ObjectInspector[] {}, false, false);
    udf.getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentException.class })
  public void initTooManyArgs() throws SemanticException {
    UnionSketchUDAF udf = new UnionSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(new ObjectInspector[] {
        binaryInspector, intInspector, longInspector, longInspector }, false, false);
    udf.getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void initIvalidCategoryArg1() throws SemanticException {
    UnionSketchUDAF udf = new UnionSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(
        new ObjectInspector[] { structInspector }, false, false);
    udf.getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void initInvalidTypeArg1() throws SemanticException {
    UnionSketchUDAF udf = new UnionSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(
        new ObjectInspector[] { intInspector }, false, false);
    udf.getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void initIvalidCategoryArg2() throws SemanticException {
    UnionSketchUDAF udf = new UnionSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(
        new ObjectInspector[] { binaryInspector, structInspector }, false, false);
    udf.getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void initInvalidTypeArg2() throws SemanticException {
    UnionSketchUDAF udf = new UnionSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(
        new ObjectInspector[] { binaryInspector, binaryInspector }, false, false);
    udf.getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void initIvalidCategoryArg3() throws SemanticException {
    UnionSketchUDAF udf = new UnionSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(
        new ObjectInspector[] { binaryInspector, intInspector, structInspector }, false, false);
    udf.getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void initInvalidTypeArg3() throws SemanticException {
    UnionSketchUDAF udf = new UnionSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(
        new ObjectInspector[] { binaryInspector, intInspector, binaryInspector }, false, false);
    udf.getEvaluator(params);
  }

  // PARTIAL1 mode (Map phase in Map-Reduce): iterate + terminatePartial
  @Test
  public void partial1ModeDefaultParams() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    GenericUDAFEvaluator eval = new UnionSketchUDAF().getEvaluator(info);
    ObjectInspector resultInspector = eval.init(Mode.PARTIAL1, inspectors);
    DataToSketchUDAFTest.checkIntermediateResultInspector(resultInspector);

    State state = (State) eval.getNewAggregationBuffer();

    UpdateSketch sketch1 = UpdateSketch.builder().build();
    sketch1.update(1);
    eval.iterate(state, new Object[] {new BytesWritable(sketch1.compact().toByteArray())});

    UpdateSketch sketch2 = UpdateSketch.builder().build();
    sketch2.update(2);
    eval.iterate(state, new Object[] {new BytesWritable(sketch2.compact().toByteArray())});

    Object result = eval.terminatePartial(state);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof List);
    List<?> r = (List<?>) result;
    Assert.assertEquals(r.size(), 3);
    Assert.assertEquals(((IntWritable) (r.get(0))).get(), DEFAULT_NOMINAL_ENTRIES);
    Assert.assertEquals(((LongWritable) (r.get(1))).get(), DEFAULT_UPDATE_SEED);
    Sketch resultSketch = Sketches.heapifySketch(new NativeMemory(((BytesWritable) (r.get(2))).getBytes()));
    Assert.assertFalse(resultSketch.isEstimationMode());
    Assert.assertEquals(resultSketch.getEstimate(), 2.0);

    eval.close();
  }

  @Test
  public void partial1ModeExplicitParams() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector, intInspector, longInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    GenericUDAFEvaluator eval = new UnionSketchUDAF().getEvaluator(info);
    ObjectInspector resultInspector = eval.init(Mode.PARTIAL1, inspectors);
    DataToSketchUDAFTest.checkIntermediateResultInspector(resultInspector);

    final int nomEntries = 8;
    final long seed = 1;
    State state = (State) eval.getNewAggregationBuffer();

    UpdateSketch sketch1 = UpdateSketch.builder().setSeed(seed).build();
    sketch1.update(1);
    eval.iterate(state, new Object[] {new BytesWritable(sketch1.compact().toByteArray()), new IntWritable(nomEntries), new LongWritable(seed)});

    UpdateSketch sketch2 = UpdateSketch.builder().setSeed(seed).build();
    sketch2.update(2);
    eval.iterate(state, new Object[] {new BytesWritable(sketch2.compact().toByteArray()), new IntWritable(nomEntries), new LongWritable(seed)});

    Object result = eval.terminatePartial(state);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof List);
    List<?> r = (List<?>) result;
    Assert.assertEquals(r.size(), 3);
    Assert.assertEquals(((IntWritable) (r.get(0))).get(), nomEntries);
    Assert.assertEquals(((LongWritable) (r.get(1))).get(), seed);
    Sketch resultSketch = Sketches.heapifySketch(new NativeMemory(((BytesWritable) (r.get(2))).getBytes()), seed);
    Assert.assertFalse(resultSketch.isEstimationMode());
    Assert.assertEquals(resultSketch.getEstimate(), 2.0);

    eval.close();
  }

  //PARTIAL2 mode (Combine phase in Map-Reduce): merge + terminatePartial
  @Test
  public void partial2Mode() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    GenericUDAFEvaluator eval = new UnionSketchUDAF().getEvaluator(info);
    ObjectInspector resultInspector = eval.init(Mode.PARTIAL2, new ObjectInspector[] {structInspector});
    DataToSketchUDAFTest.checkIntermediateResultInspector(resultInspector);

    State state = (State) eval.getNewAggregationBuffer();

    UpdateSketch sketch1 = UpdateSketch.builder().build();
    sketch1.update(1);
    eval.merge(state, Arrays.asList(
      new IntWritable(DEFAULT_NOMINAL_ENTRIES),
      new LongWritable(DEFAULT_UPDATE_SEED),
      new BytesWritable(sketch1.compact().toByteArray()))
    );

    UpdateSketch sketch2 = UpdateSketch.builder().build();
    sketch2.update(2);
    eval.merge(state, Arrays.asList(
      new IntWritable(DEFAULT_NOMINAL_ENTRIES),
      new LongWritable(DEFAULT_UPDATE_SEED),
      new BytesWritable(sketch2.compact().toByteArray()))
    );

    Object result = eval.terminatePartial(state);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof List);
    List<?> r = (List<?>) result;
    Assert.assertEquals(r.size(), 3);
    Assert.assertEquals(((IntWritable) (r.get(0))).get(), DEFAULT_NOMINAL_ENTRIES);
    Assert.assertEquals(((LongWritable) (r.get(1))).get(), DEFAULT_UPDATE_SEED);
    Sketch resultSketch = Sketches.heapifySketch(new NativeMemory(((BytesWritable) (r.get(2))).getBytes()));
    Assert.assertEquals(resultSketch.getEstimate(), 2.0);

    eval.close();
  }

  // FINAL mode (Reduce phase in Map-Reduce): merge + terminate
  @Test
  public void finalMode() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    GenericUDAFEvaluator eval = new UnionSketchUDAF().getEvaluator(info);
    ObjectInspector resultInspector = eval.init(Mode.FINAL, new ObjectInspector[] {structInspector});
    DataToSketchUDAFTest.checkFinalResultInspector(resultInspector);

    State state = (State) eval.getNewAggregationBuffer();

    UpdateSketch sketch1 = UpdateSketch.builder().build();
    sketch1.update(1);
    eval.merge(state, Arrays.asList(
      new IntWritable(DEFAULT_NOMINAL_ENTRIES),
      new LongWritable(DEFAULT_UPDATE_SEED),
      new BytesWritable(sketch1.compact().toByteArray()))
    );

    UpdateSketch sketch2 = UpdateSketch.builder().build();
    sketch2.update(2);
    eval.merge(state, Arrays.asList(
      new IntWritable(DEFAULT_NOMINAL_ENTRIES),
      new LongWritable(DEFAULT_UPDATE_SEED),
      new BytesWritable(sketch2.compact().toByteArray()))
    );

    Object result = eval.terminate(state);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof BytesWritable);
    Sketch resultSketch = Sketches.heapifySketch(new NativeMemory(((BytesWritable) result).getBytes()));
    Assert.assertEquals(resultSketch.getEstimate(), 2.0);

    eval.close();
  }

  // COMPLETE mode (single mode, alternative to MapReduce): iterate + terminate
  @Test
  public void completeModeDefaultSizeAndSeed() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    GenericUDAFEvaluator eval = new UnionSketchUDAF().getEvaluator(info);
    ObjectInspector resultInspector = eval.init(Mode.COMPLETE, inspectors);
    DataToSketchUDAFTest.checkFinalResultInspector(resultInspector);

    State state = (State) eval.getNewAggregationBuffer();

    UpdateSketch sketch1 = UpdateSketch.builder().build();
    sketch1.update(1);
    eval.iterate(state, new Object[] {new BytesWritable(sketch1.compact().toByteArray())});

    UpdateSketch sketch2 = UpdateSketch.builder().build();
    sketch2.update(2);
    eval.iterate(state, new Object[] {new BytesWritable(sketch2.compact().toByteArray())});

    Object result = eval.terminate(state);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof BytesWritable);
    Sketch resultSketch = Sketches.heapifySketch(new NativeMemory(((BytesWritable) result).getBytes()));
    Assert.assertEquals(resultSketch.getEstimate(), 2.0);

    eval.reset(state);
    result = eval.terminate(state);
    Assert.assertNull(result);

    eval.close();
  }

  @Test
  public void completeModeExplicitSizeAndSeed() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector, intInspector, longInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    GenericUDAFEvaluator eval = new UnionSketchUDAF().getEvaluator(info);
    ObjectInspector resultInspector = eval.init(Mode.COMPLETE, inspectors);
    DataToSketchUDAFTest.checkFinalResultInspector(resultInspector);

    final int nomEntries = 8;
    final long seed = 1;
    State state = (State) eval.getNewAggregationBuffer();

    UpdateSketch sketch1 = UpdateSketch.builder().setSeed(seed).build();
    sketch1.update(1);
    eval.iterate(state, new Object[] {new BytesWritable(sketch1.compact().toByteArray()), new IntWritable(nomEntries), new LongWritable(seed)});

    UpdateSketch sketch2 = UpdateSketch.builder().setSeed(seed).build();
    sketch2.update(2);
    eval.iterate(state, new Object[] {new BytesWritable(sketch2.compact().toByteArray()), new IntWritable(nomEntries), new LongWritable(seed)});

    Object result = eval.terminate(state);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof BytesWritable);
    Sketch resultSketch = Sketches.heapifySketch(new NativeMemory(((BytesWritable) result).getBytes()), seed);
    Assert.assertEquals(resultSketch.getEstimate(), 2.0);

    eval.close();
  }

}
