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
import java.util.List;

import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.UpdateSketch;
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

/**
 * Unit tests for UnionSketch UDF
 */
@SuppressWarnings({"javadoc","resource"})
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
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(new ObjectInspector[] {}, false, false, false);
    udf.getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentException.class })
  public void initTooManyArgs() throws SemanticException {
    UnionSketchUDAF udf = new UnionSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(new ObjectInspector[] {
        binaryInspector, intInspector, longInspector, longInspector }, false, false, false);
    udf.getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void initIvalidCategoryArg1() throws SemanticException {
    UnionSketchUDAF udf = new UnionSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(
        new ObjectInspector[] { structInspector }, false, false, false);
    udf.getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void initInvalidTypeArg1() throws SemanticException {
    UnionSketchUDAF udf = new UnionSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(
        new ObjectInspector[] { intInspector }, false, false, false);
    udf.getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void initIvalidCategoryArg2() throws SemanticException {
    UnionSketchUDAF udf = new UnionSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(
        new ObjectInspector[] { binaryInspector, structInspector }, false, false, false);
    udf.getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void initInvalidTypeArg2() throws SemanticException {
    UnionSketchUDAF udf = new UnionSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(
        new ObjectInspector[] { binaryInspector, binaryInspector }, false, false, false);
    udf.getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void initIvalidCategoryArg3() throws SemanticException {
    UnionSketchUDAF udf = new UnionSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(
        new ObjectInspector[] { binaryInspector, intInspector, structInspector }, false, false, false);
    udf.getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void initInvalidTypeArg3() throws SemanticException {
    UnionSketchUDAF udf = new UnionSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(
        new ObjectInspector[] { binaryInspector, intInspector, binaryInspector }, false, false, false);
    udf.getEvaluator(params);
  }

  // PARTIAL1 mode (Map phase in Map-Reduce): iterate + terminatePartial
  @Test
  public void partial1ModeDefaultParams() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new UnionSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.PARTIAL1, inspectors);
      DataToSketchUDAFTest.checkIntermediateResultInspector(resultInspector);

      UnionState state = (UnionState) eval.getNewAggregationBuffer();

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
      Assert.assertEquals(((IntWritable) r.get(0)).get(), DEFAULT_NOMINAL_ENTRIES);
      Assert.assertEquals(((LongWritable) r.get(1)).get(), DEFAULT_UPDATE_SEED);
      Sketch resultSketch = Sketches.wrapSketch(BytesWritableHelper.wrapAsMemory((BytesWritable) r.get(2)));
      Assert.assertFalse(resultSketch.isEstimationMode());
      Assert.assertEquals(resultSketch.getEstimate(), 2.0);
    }
  }

  @Test
  public void partial1ModeExplicitParams() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector, intInspector, longInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new UnionSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.PARTIAL1, inspectors);
      DataToSketchUDAFTest.checkIntermediateResultInspector(resultInspector);

      final int nomEntries = 16;
      final long seed = 1;
      UnionState state = (UnionState) eval.getNewAggregationBuffer();

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
      Assert.assertEquals(((IntWritable) r.get(0)).get(), nomEntries);
      Assert.assertEquals(((LongWritable) r.get(1)).get(), seed);
      Sketch resultSketch = Sketches.wrapSketch(BytesWritableHelper.wrapAsMemory((BytesWritable) r.get(2)), seed);
      Assert.assertFalse(resultSketch.isEstimationMode());
      Assert.assertEquals(resultSketch.getEstimate(), 2.0);
    }
  }

  //PARTIAL2 mode (Combine phase in Map-Reduce): merge + terminatePartial
  @Test
  public void partial2Mode() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new UnionSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.PARTIAL2, new ObjectInspector[] {structInspector});
      DataToSketchUDAFTest.checkIntermediateResultInspector(resultInspector);

      UnionState state = (UnionState) eval.getNewAggregationBuffer();

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
      Assert.assertEquals(((IntWritable) r.get(0)).get(), DEFAULT_NOMINAL_ENTRIES);
      Assert.assertEquals(((LongWritable) r.get(1)).get(), DEFAULT_UPDATE_SEED);
      Sketch resultSketch = Sketches.wrapSketch(BytesWritableHelper.wrapAsMemory((BytesWritable) r.get(2)));
      Assert.assertEquals(resultSketch.getEstimate(), 2.0);
    }
  }

  // FINAL mode (Reduce phase in Map-Reduce): merge + terminate
  @Test
  public void finalMode() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new UnionSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.FINAL, new ObjectInspector[] {structInspector});
      DataToSketchUDAFTest.checkFinalResultInspector(resultInspector);

      UnionState state = (UnionState) eval.getNewAggregationBuffer();

      UpdateSketch sketch1 = UpdateSketch.builder().build();
      sketch1.update(1);
      eval.merge(state, Arrays.asList(
        new IntWritable(DEFAULT_NOMINAL_ENTRIES),
        new LongWritable(DEFAULT_UPDATE_SEED),
        new BytesWritable(sketch1.compact().toByteArray())
      ));

      UpdateSketch sketch2 = UpdateSketch.builder().build();
      sketch2.update(2);
      eval.merge(state, Arrays.asList(
        new IntWritable(DEFAULT_NOMINAL_ENTRIES),
        new LongWritable(DEFAULT_UPDATE_SEED),
        new BytesWritable(sketch2.compact().toByteArray())
      ));

      Object result = eval.terminate(state);
      Assert.assertNotNull(result);
      Assert.assertTrue(result instanceof BytesWritable);
      Sketch resultSketch = Sketches.wrapSketch(BytesWritableHelper.wrapAsMemory((BytesWritable) result));
      Assert.assertEquals(resultSketch.getEstimate(), 2.0);
    }
  }

  // COMPLETE mode (single mode, alternative to MapReduce): iterate + terminate
  @Test
  public void completeModeDefaultSizeAndSeed() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new UnionSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.COMPLETE, inspectors);
      DataToSketchUDAFTest.checkFinalResultInspector(resultInspector);

      UnionState state = (UnionState) eval.getNewAggregationBuffer();

      UpdateSketch sketch1 = UpdateSketch.builder().build();
      sketch1.update(1);
      eval.iterate(state, new Object[] {new BytesWritable(sketch1.compact().toByteArray())});

      UpdateSketch sketch2 = UpdateSketch.builder().build();
      sketch2.update(2);
      eval.iterate(state, new Object[] {new BytesWritable(sketch2.compact().toByteArray())});

      Object result = eval.terminate(state);
      Assert.assertNotNull(result);
      Assert.assertTrue(result instanceof BytesWritable);
      Sketch resultSketch = Sketches.wrapSketch(BytesWritableHelper.wrapAsMemory((BytesWritable) result));
      Assert.assertEquals(resultSketch.getEstimate(), 2.0);

      eval.reset(state);
      result = eval.terminate(state);
      Assert.assertNull(result);
    }
  }

  @Test
  public void completeModeExplicitSizeAndSeed() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector, intInspector, longInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new UnionSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.COMPLETE, inspectors);
      DataToSketchUDAFTest.checkFinalResultInspector(resultInspector);

      final int nomEntries = 16;
      final long seed = 1;
      UnionState state = (UnionState) eval.getNewAggregationBuffer();

      UpdateSketch sketch1 = UpdateSketch.builder().setSeed(seed).build();
      sketch1.update(1);
      eval.iterate(state, new Object[] {new BytesWritable(sketch1.compact().toByteArray()), new IntWritable(nomEntries), new LongWritable(seed)});

      UpdateSketch sketch2 = UpdateSketch.builder().setSeed(seed).build();
      sketch2.update(2);
      eval.iterate(state, new Object[] {new BytesWritable(sketch2.compact().toByteArray()), new IntWritable(nomEntries), new LongWritable(seed)});

      Object result = eval.terminate(state);
      Assert.assertNotNull(result);
      Assert.assertTrue(result instanceof BytesWritable);
      Sketch resultSketch = Sketches.wrapSketch(BytesWritableHelper.wrapAsMemory((BytesWritable) result), seed);
      Assert.assertEquals(resultSketch.getEstimate(), 2.0);
    }
  }

}
