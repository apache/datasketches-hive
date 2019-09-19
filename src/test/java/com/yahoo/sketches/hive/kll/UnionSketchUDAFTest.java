/*
 * Copyright 2018, Oath Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.kll;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
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
import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.kll.KllFloatsSketch;

@SuppressWarnings("javadoc")
public class UnionSketchUDAFTest {

  static final ObjectInspector binaryInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY);

  static final  ObjectInspector intInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.INT);

  static final ObjectInspector structInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
    Arrays.asList("a"),
    Arrays.asList(binaryInspector)
  );

  @Test(expectedExceptions = UDFArgumentException.class)
  public void getEvaluatorTooFewInspectors() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new UnionSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentException.class)
  public void getEvaluatorTooManyInspectors() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector, binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new UnionSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentTypeException.class)
  public void getEvaluatorWrongCategoryArg1() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { structInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new UnionSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentTypeException.class)
  public void getEvaluatorWrongTypeArg1() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new UnionSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentTypeException.class)
  public void getEvaluatorWrongCategoryArg2() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector, structInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new UnionSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentTypeException.class)
  public void getEvaluatorWrongTypeArg2() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector, binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new UnionSketchUDAF().getEvaluator(info);
  }

  // PARTIAL1 mode (Map phase in Map-Reduce): iterate + terminatePartial
  @Test
  public void partia1ModelDefaultKDowsizeInput() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new UnionSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.PARTIAL1, inspectors);
      DataToSketchUDAFTest.checkResultInspector(resultInspector);

      SketchState state = (SketchState) eval.getNewAggregationBuffer();

      KllFloatsSketch sketch1 = new KllFloatsSketch(400);
      sketch1.update(1);
      eval.iterate(state, new Object[] { new BytesWritable(sketch1.toByteArray()) });

      KllFloatsSketch sketch2 = new KllFloatsSketch(400);
      sketch2.update(2);
      eval.iterate(state, new Object[] { new BytesWritable(sketch2.toByteArray()) });

      BytesWritable bytes = (BytesWritable) eval.terminatePartial(state);
      KllFloatsSketch resultSketch = KllFloatsSketch.heapify(Memory.wrap(bytes.getBytes()));
      Assert.assertEquals(resultSketch.getNormalizedRankError(false), KllFloatsSketch.getNormalizedRankError(200, false));
      Assert.assertEquals(resultSketch.getNumRetained(), 2);
      Assert.assertEquals(resultSketch.getMinValue(), 1f);
      Assert.assertEquals(resultSketch.getMaxValue(), 2f);
    }
  }

  @Test
  public void partia1ModelGivenK() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector, intInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new UnionSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.PARTIAL1, inspectors);
      DataToSketchUDAFTest.checkResultInspector(resultInspector);

      SketchState state = (SketchState) eval.getNewAggregationBuffer();

      KllFloatsSketch sketch1 = new KllFloatsSketch(400);
      sketch1.update(1);
      eval.iterate(state, new Object[] { new BytesWritable(sketch1.toByteArray()), new IntWritable(400) });

      KllFloatsSketch sketch2 = new KllFloatsSketch(400);
      sketch2.update(2);
      eval.iterate(state, new Object[] { new BytesWritable(sketch2.toByteArray()), new IntWritable(400) });

      BytesWritable bytes = (BytesWritable) eval.terminatePartial(state);
      KllFloatsSketch resultSketch = KllFloatsSketch.heapify(Memory.wrap(bytes.getBytes()));
      Assert.assertEquals(resultSketch.getNormalizedRankError(false), KllFloatsSketch.getNormalizedRankError(400, false));
      Assert.assertEquals(resultSketch.getNumRetained(), 2);
      Assert.assertEquals(resultSketch.getMinValue(), 1f);
      Assert.assertEquals(resultSketch.getMaxValue(), 2f);
    }
  }

  // PARTIAL2 mode (Combine phase in Map-Reduce): merge + terminatePartial
  @Test
  public void partial2Mode() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new UnionSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.PARTIAL2, inspectors);
      DataToSketchUDAFTest.checkResultInspector(resultInspector);

      SketchState state = (SketchState) eval.getNewAggregationBuffer();

      KllFloatsSketch sketch1 = new KllFloatsSketch(400);
      sketch1.update(1);
      eval.merge(state, new BytesWritable(sketch1.toByteArray()));

      KllFloatsSketch sketch2 = new KllFloatsSketch(400);
      sketch2.update(2);
      eval.merge(state, new BytesWritable(sketch2.toByteArray()));

      BytesWritable bytes = (BytesWritable) eval.terminatePartial(state);
      KllFloatsSketch resultSketch = KllFloatsSketch.heapify(Memory.wrap(bytes.getBytes()));
      Assert.assertEquals(resultSketch.getNormalizedRankError(false), KllFloatsSketch.getNormalizedRankError(400, false));
      Assert.assertEquals(resultSketch.getNumRetained(), 2);
      Assert.assertEquals(resultSketch.getMinValue(), 1f);
      Assert.assertEquals(resultSketch.getMaxValue(), 2f);
    }
  }

  // FINAL mode (Reduce phase in Map-Reduce): merge + terminate
  @Test
  public void finalMode() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new UnionSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.FINAL, inspectors);
      DataToSketchUDAFTest.checkResultInspector(resultInspector);

      SketchState state = (SketchState) eval.getNewAggregationBuffer();

      KllFloatsSketch sketch1 = new KllFloatsSketch(400);
      sketch1.update(1);
      eval.merge(state, new BytesWritable(sketch1.toByteArray()));

      KllFloatsSketch sketch2 = new KllFloatsSketch(400);
      sketch2.update(2);
      eval.merge(state, new BytesWritable(sketch2.toByteArray()));

      BytesWritable bytes = (BytesWritable) eval.terminate(state);
      KllFloatsSketch resultSketch = KllFloatsSketch.heapify(Memory.wrap(bytes.getBytes()));
      Assert.assertEquals(resultSketch.getNormalizedRankError(false), KllFloatsSketch.getNormalizedRankError(400, false));
      Assert.assertEquals(resultSketch.getNumRetained(), 2);
      Assert.assertEquals(resultSketch.getMinValue(), 1f);
      Assert.assertEquals(resultSketch.getMaxValue(), 2f);
    }
  }

  // COMPLETE mode (single mode, alternative to MapReduce): iterate + terminate
  @Test
  public void completeModelDefaultK() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new UnionSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.COMPLETE, inspectors);
      DataToSketchUDAFTest.checkResultInspector(resultInspector);

      SketchState state = (SketchState) eval.getNewAggregationBuffer();

      KllFloatsSketch sketch1 = new KllFloatsSketch();
      sketch1.update(1);
      eval.iterate(state, new Object[] { new BytesWritable(sketch1.toByteArray()) });

      KllFloatsSketch sketch2 = new KllFloatsSketch();
      sketch2.update(2);
      eval.iterate(state, new Object[] { new BytesWritable(sketch2.toByteArray()) });

      BytesWritable bytes = (BytesWritable) eval.terminatePartial(state);
      KllFloatsSketch resultSketch = KllFloatsSketch.heapify(Memory.wrap(bytes.getBytes()));
      Assert.assertEquals(resultSketch.getNormalizedRankError(false), KllFloatsSketch.getNormalizedRankError(200, false));
      Assert.assertEquals(resultSketch.getNumRetained(), 2);
      Assert.assertEquals(resultSketch.getMinValue(), 1f);
      Assert.assertEquals(resultSketch.getMaxValue(), 2f);

      eval.reset(state);
      Assert.assertNull(eval.terminate(state));
    }
  }

}
