/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.tuple;

import static com.yahoo.sketches.Util.DEFAULT_NOMINAL_ENTRIES;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.tuple.DoubleSummary;
import com.yahoo.sketches.tuple.DoubleSummaryFactory;
import com.yahoo.sketches.tuple.Sketch;
import com.yahoo.sketches.tuple.Sketches;
import com.yahoo.sketches.tuple.UpdatableSketch;
import com.yahoo.sketches.tuple.UpdatableSketchBuilder;

public class UnionDoubleSummarySketchUDAFTest {

  private static final ObjectInspector intInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.INT);

  private static final ObjectInspector floatInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.FLOAT);

  private static final ObjectInspector binaryInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY);

  private static final ObjectInspector structInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
      Arrays.asList("nominalEntries", "sketch"),
      Arrays.asList(intInspector, binaryInspector)
    );

  @Test(expectedExceptions = { UDFArgumentException.class })
  public void tooFewArguments() throws SemanticException {
    ObjectInspector[] inspectors = new ObjectInspector[] { };
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    new UnionDoubleSummarySketchUDAF().getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentException.class })
  public void tooManyArguments() throws SemanticException {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector, intInspector, intInspector };
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    new UnionDoubleSummarySketchUDAF().getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void invalidCategoryArg1() throws SemanticException {
    ObjectInspector[] inspectors = new ObjectInspector[] { structInspector };
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    new UnionDoubleSummarySketchUDAF().getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void invalidTypeArg1() throws SemanticException {
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector };
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    new UnionDoubleSummarySketchUDAF().getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void invalidCategoryArg2() throws SemanticException {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector, structInspector };
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    new UnionDoubleSummarySketchUDAF().getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void invalidTypeArg2() throws SemanticException {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector, floatInspector };
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    new UnionDoubleSummarySketchUDAF().getEvaluator(params);
  }

  // PARTIAL1 mode (Map phase in Map-Reduce): iterate + terminatePartial
  @Test
  public void partial1ModeDefaultParams() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    GenericUDAFEvaluator eval = new UnionDoubleSummarySketchUDAF().getEvaluator(info);
    ObjectInspector resultInspector = eval.init(Mode.PARTIAL1, inspectors);
    DataToDoubleSummarySketchUDAFTest.checkIntermediateResultInspector(resultInspector);

    @SuppressWarnings("unchecked")
    State<DoubleSummary> state = (State<DoubleSummary>) eval.getNewAggregationBuffer();

    UpdatableSketch<Double, DoubleSummary> sketch1 = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    sketch1.update(1, 1.0);
    eval.iterate(state, new Object[] {new BytesWritable(sketch1.compact().toByteArray())});

    UpdatableSketch<Double, DoubleSummary> sketch2 = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    sketch2.update(2, 1.0);
    eval.iterate(state, new Object[] {new BytesWritable(sketch2.compact().toByteArray())});

    Object result = eval.terminatePartial(state);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof List);
    List<?> r = (List<?>) result;
    Assert.assertEquals(r.size(), 2);
    Assert.assertEquals(((IntWritable) (r.get(0))).get(), DEFAULT_NOMINAL_ENTRIES);
    Sketch<DoubleSummary> resultSketch = Sketches.heapifySketch(new NativeMemory(((BytesWritable) (r.get(1))).getBytes()));
    Assert.assertEquals(resultSketch.getEstimate(), 2.0);

    eval.close();
  }

  @Test
  public void partial1ModeExplicitParams() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector, intInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    GenericUDAFEvaluator eval = new UnionDoubleSummarySketchUDAF().getEvaluator(info);
    ObjectInspector resultInspector = eval.init(Mode.PARTIAL1, inspectors);
    DataToDoubleSummarySketchUDAFTest.checkIntermediateResultInspector(resultInspector);

    final int nomNumEntries = 8;
    @SuppressWarnings("unchecked")
    State<DoubleSummary> state = (State<DoubleSummary>) eval.getNewAggregationBuffer();

    UpdatableSketch<Double, DoubleSummary> sketch1 =
        new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).setNominalEntries(nomNumEntries).build();
    sketch1.update(1, 1.0);
    eval.iterate(state, new Object[] {new BytesWritable(sketch1.compact().toByteArray()),
        new IntWritable(nomNumEntries)});

    UpdatableSketch<Double, DoubleSummary> sketch2 =
        new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).setNominalEntries(nomNumEntries).build();
    sketch2.update(2, 1.0);
    eval.iterate(state, new Object[] {new BytesWritable(sketch2.compact().toByteArray()),
        new IntWritable(nomNumEntries)});

    Object result = eval.terminatePartial(state);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof List);
    List<?> r = (List<?>) result;
    Assert.assertEquals(r.size(), 2);
    Assert.assertEquals(((IntWritable) (r.get(0))).get(), nomNumEntries);
    Sketch<DoubleSummary> resultSketch = Sketches.heapifySketch(new NativeMemory(((BytesWritable) (r.get(1))).getBytes()));
    Assert.assertEquals(resultSketch.getEstimate(), 2.0);

    eval.close();
  }

  // PARTIAL2 mode (Combine phase in Map-Reduce): merge + terminatePartial
  @Test
  public void partial2Mode() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    GenericUDAFEvaluator eval = new UnionDoubleSummarySketchUDAF().getEvaluator(info);
    ObjectInspector resultInspector = eval.init(Mode.PARTIAL2, new ObjectInspector[] {structInspector});
    DataToDoubleSummarySketchUDAFTest.checkIntermediateResultInspector(resultInspector);

    @SuppressWarnings("unchecked")
    State<DoubleSummary> state = (State<DoubleSummary>) eval.getNewAggregationBuffer();

    UpdatableSketch<Double, DoubleSummary> sketch1 = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    sketch1.update(1, 1.0);
    eval.merge(state, Arrays.asList(
      new IntWritable(DEFAULT_NOMINAL_ENTRIES),
      new BytesWritable(sketch1.compact().toByteArray()))
    );

    UpdatableSketch<Double, DoubleSummary> sketch2 = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    sketch2.update(2, 1.0);
    eval.merge(state, Arrays.asList(
      new IntWritable(DEFAULT_NOMINAL_ENTRIES),
      new BytesWritable(sketch2.compact().toByteArray()))
    );

    Object result = eval.terminatePartial(state);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof List);
    List<?> r = (List<?>) result;
    Assert.assertEquals(r.size(), 2);
    Assert.assertEquals(((IntWritable) (r.get(0))).get(), DEFAULT_NOMINAL_ENTRIES);
    Sketch<DoubleSummary> resultSketch = Sketches.heapifySketch(new NativeMemory(((BytesWritable) (r.get(1))).getBytes()));
    Assert.assertEquals(resultSketch.getEstimate(), 2.0);

    eval.reset(state);
    result = eval.terminate(state);
    Assert.assertNull(result);

    eval.close();
  }

  // FINAL mode (Reduce phase in Map-Reduce): merge + terminate
  @Test
  public void finalMode() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    GenericUDAFEvaluator eval = new UnionDoubleSummarySketchUDAF().getEvaluator(info);
    ObjectInspector resultInspector = eval.init(Mode.FINAL, new ObjectInspector[] {structInspector});
    DataToDoubleSummarySketchUDAFTest.checkFinalResultInspector(resultInspector);

    @SuppressWarnings("unchecked")
    State<DoubleSummary> state = (State<DoubleSummary>) eval.getNewAggregationBuffer();

    UpdatableSketch<Double, DoubleSummary> sketch1 = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    sketch1.update(1, 1.0);
    eval.merge(state, Arrays.asList(
      new IntWritable(DEFAULT_NOMINAL_ENTRIES),
      new BytesWritable(sketch1.compact().toByteArray()))
    );

    UpdatableSketch<Double, DoubleSummary> sketch2 = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    sketch2.update(2, 1.0);
    eval.merge(state, Arrays.asList(
      new IntWritable(DEFAULT_NOMINAL_ENTRIES),
      new BytesWritable(sketch2.compact().toByteArray()))
    );

    Object result = eval.terminate(state);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof BytesWritable);
    Sketch<DoubleSummary> resultSketch = Sketches.heapifySketch(new NativeMemory(((BytesWritable) result).getBytes()));
    Assert.assertEquals(resultSketch.getEstimate(), 2.0);

    eval.close();
  }

  // COMPLETE mode (single mode, alternative to MapReduce): iterate + terminate
  @Test
  public void completeModeDefaultParams() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    GenericUDAFEvaluator eval = new UnionDoubleSummarySketchUDAF().getEvaluator(info);
    ObjectInspector resultInspector = eval.init(Mode.COMPLETE, inspectors);
    DataToDoubleSummarySketchUDAFTest.checkFinalResultInspector(resultInspector);

    @SuppressWarnings("unchecked")
    State<DoubleSummary> state = (State<DoubleSummary>) eval.getNewAggregationBuffer();

    UpdatableSketch<Double, DoubleSummary> sketch1 = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    sketch1.update(1, 1.0);
    eval.iterate(state, new Object[] {new BytesWritable(sketch1.compact().toByteArray())});

    UpdatableSketch<Double, DoubleSummary> sketch2 = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).build();
    sketch2.update(2, 1.0);
    eval.iterate(state, new Object[] {new BytesWritable(sketch2.compact().toByteArray())});

    Object result = eval.terminate(state);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof BytesWritable);
    Sketch<DoubleSummary> resultSketch = Sketches.heapifySketch(new NativeMemory(((BytesWritable) result).getBytes()));
    Assert.assertEquals(resultSketch.getEstimate(), 2.0);

    eval.reset(state);
    result = eval.terminate(state);
    Assert.assertNull(result);

    eval.close();
  }

  @Test
  public void completeModeExplicitParams() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector, intInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    GenericUDAFEvaluator eval = new UnionDoubleSummarySketchUDAF().getEvaluator(info);
    ObjectInspector resultInspector = eval.init(Mode.COMPLETE, inspectors);
    DataToDoubleSummarySketchUDAFTest.checkFinalResultInspector(resultInspector);

    final int nomNumEntries = 8;
    @SuppressWarnings("unchecked")
    State<DoubleSummary> state = (State<DoubleSummary>) eval.getNewAggregationBuffer();

    UpdatableSketch<Double, DoubleSummary> sketch1 =
        new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).setNominalEntries(nomNumEntries).build();
    sketch1.update(1, 1.0);
    eval.iterate(state, new Object[] {new BytesWritable(sketch1.compact().toByteArray()),
        new IntWritable(nomNumEntries)});

    UpdatableSketch<Double, DoubleSummary> sketch2 =
        new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).setNominalEntries(nomNumEntries).build();
    sketch2.update(2, 1.0);
    eval.iterate(state, new Object[] {new BytesWritable(sketch2.compact().toByteArray()),
        new IntWritable(nomNumEntries)});

    Object result = eval.terminate(state);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof BytesWritable);
    Sketch<DoubleSummary> resultSketch = Sketches.heapifySketch(new NativeMemory(((BytesWritable) result).getBytes()));
    Assert.assertEquals(resultSketch.getEstimate(), 2.0, 0.05);

    eval.reset(state);
    result = eval.terminate(state);
    Assert.assertNull(result);

    eval.close();
  }

}
