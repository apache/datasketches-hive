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
import java.util.List;

import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

@SuppressWarnings({"javadoc","resource"})
public class IntersectSketchUDAFTest {

  static final ObjectInspector longInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.LONG);

  static final ObjectInspector binaryInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY);

  static final ObjectInspector structInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
    Arrays.asList("seed", "sketch"),
    Arrays.asList(longInspector, binaryInspector)
  );

  @Test(expectedExceptions = UDFArgumentException.class)
  public void getEvaluatorTooFewInspectors() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new IntersectSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentException.class)
  public void getEvaluatorTooManyInspectors() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector, longInspector, longInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new IntersectSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentTypeException.class)
  public void getEvaluatorWrongCategoryArg1() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { structInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new IntersectSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentTypeException.class)
  public void getEvaluatorWrongTypeArg1() throws Exception {
    ObjectInspector intInspector =
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.INT);
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new IntersectSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentException.class)
  public void getEvaluatorWrongCategoryArg2() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector, structInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new IntersectSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentException.class)
  public void getEvaluatorWrongTypeArg2() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector, binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new IntersectSketchUDAF().getEvaluator(info);
  }

  // PARTIAL1 mode (Map phase in Map-Reduce): iterate + terminatePartial
  @Test
  public void partia1lDefaultSeed() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new IntersectSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.PARTIAL1, inspectors);
      checkIntermediateResultInspector(resultInspector);

      IntersectSketchUDAF.IntersectSketchUDAFEvaluator.IntersectionState state =
          (IntersectSketchUDAF.IntersectSketchUDAFEvaluator.IntersectionState) eval.getNewAggregationBuffer();

      UpdateSketch sketch1 = UpdateSketch.builder().build();
      sketch1.update(1);
      sketch1.update(2);
      sketch1.update(3);
      eval.iterate(state, new Object[] { new BytesWritable(sketch1.toByteArray()) });

      UpdateSketch sketch2 = UpdateSketch.builder().build();
      sketch2.update(2);
      sketch2.update(3);
      sketch2.update(4);
      eval.iterate(state, new Object[] { new BytesWritable(sketch2.toByteArray()) });

      Object result = eval.terminatePartial(state);
      Assert.assertNotNull(result);
      Assert.assertTrue(result instanceof List);
      List<?> r = (List<?>) result;
      Assert.assertEquals(r.size(), 2);
      Assert.assertEquals(((LongWritable) r.get(0)).get(), DEFAULT_UPDATE_SEED);
      Sketch resultSketch = Sketches.wrapSketch(BytesWritableHelper.wrapAsMemory((BytesWritable) r.get(1)));
      Assert.assertEquals(resultSketch.getRetainedEntries(true), 2);
      Assert.assertEquals(resultSketch.getEstimate(), 2.0);
    }
  }

  @Test
  public void partial1ModeExplicitSeed() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector, longInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new IntersectSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.PARTIAL1, inspectors);
      checkIntermediateResultInspector(resultInspector);

      final long seed = 1;
      IntersectSketchUDAF.IntersectSketchUDAFEvaluator.IntersectionState state =
          (IntersectSketchUDAF.IntersectSketchUDAFEvaluator.IntersectionState) eval.getNewAggregationBuffer();

      UpdateSketch sketch1 = UpdateSketch.builder().setSeed(seed).build();
      sketch1.update(1);
      sketch1.update(2);
      sketch1.update(3);
      eval.iterate(state, new Object[] {new BytesWritable(sketch1.toByteArray()), new LongWritable(seed)});

      UpdateSketch sketch2 = UpdateSketch.builder().setSeed(seed).build();
      sketch2.update(2);
      sketch2.update(3);
      sketch2.update(4);
      eval.iterate(state, new Object[] {new BytesWritable(sketch2.toByteArray()), new LongWritable(seed) });

      Object result = eval.terminatePartial(state);
      Assert.assertNotNull(result);
      Assert.assertTrue(result instanceof List);
      List<?> r = (List<?>) result;
      Assert.assertEquals(r.size(), 2);
      Assert.assertEquals(((LongWritable) r.get(0)).get(), seed);
      Sketch resultSketch = Sketches.wrapSketch(BytesWritableHelper.wrapAsMemory((BytesWritable) r.get(1)), seed);
      Assert.assertEquals(resultSketch.getRetainedEntries(true), 2);
      Assert.assertEquals(resultSketch.getEstimate(), 2.0);
    }
  }

  // PARTIAL2 mode (Combine phase in Map-Reduce): merge + terminatePartial
  @Test
  public void partial2Mode() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new IntersectSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.PARTIAL2, new ObjectInspector[] {structInspector});
      checkIntermediateResultInspector(resultInspector);

      IntersectSketchUDAF.IntersectSketchUDAFEvaluator.IntersectionState state =
          (IntersectSketchUDAF.IntersectSketchUDAFEvaluator.IntersectionState) eval.getNewAggregationBuffer();

      UpdateSketch sketch1 = UpdateSketch.builder().build();
      sketch1.update(1);
      sketch1.update(2);
      sketch1.update(3);
      eval.merge(state, Arrays.asList(
        new LongWritable(DEFAULT_UPDATE_SEED),
        new BytesWritable(sketch1.compact().toByteArray()))
      );

      UpdateSketch sketch2 = UpdateSketch.builder().build();
      sketch2.update(2);
      sketch2.update(3);
      sketch2.update(4);
      eval.merge(state, Arrays.asList(
        new LongWritable(DEFAULT_UPDATE_SEED),
        new BytesWritable(sketch2.compact().toByteArray()))
      );

      Object result = eval.terminatePartial(state);
      Assert.assertNotNull(result);
      Assert.assertTrue(result instanceof List);
      List<?> r = (List<?>) result;
      Assert.assertEquals(r.size(), 2);
      Assert.assertEquals(((LongWritable) r.get(0)).get(), DEFAULT_UPDATE_SEED);
      Sketch resultSketch = Sketches.wrapSketch(BytesWritableHelper.wrapAsMemory((BytesWritable) r.get(1)));
      Assert.assertEquals(resultSketch.getEstimate(), 2.0);
    }
  }

  // FINAL mode (Reduce phase in Map-Reduce): merge + terminate
  @Test
  public void finalMode() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new IntersectSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.FINAL, new ObjectInspector[] {structInspector});
      DataToSketchUDAFTest.checkFinalResultInspector(resultInspector);

      IntersectSketchUDAF.IntersectSketchUDAFEvaluator.IntersectionState state =
          (IntersectSketchUDAF.IntersectSketchUDAFEvaluator.IntersectionState) eval.getNewAggregationBuffer();

      UpdateSketch sketch1 = UpdateSketch.builder().build();
      sketch1.update(1);
      sketch1.update(2);
      sketch1.update(3);
      eval.merge(state, Arrays.asList(
        new LongWritable(DEFAULT_UPDATE_SEED),
        new BytesWritable(sketch1.compact().toByteArray())
      ));

      UpdateSketch sketch2 = UpdateSketch.builder().build();
      sketch2.update(2);
      sketch2.update(3);
      sketch2.update(4);
      eval.merge(state, Arrays.asList(
        new LongWritable(DEFAULT_UPDATE_SEED),
        new BytesWritable(sketch2.compact().toByteArray())
      ));

      BytesWritable bytes = (BytesWritable) eval.terminate(state);
      Sketch resultSketch = Sketches.wrapSketch(BytesWritableHelper.wrapAsMemory(bytes));
      Assert.assertEquals(resultSketch.getRetainedEntries(true), 2);
      Assert.assertEquals(resultSketch.getEstimate(), 2.0);
    }
  }

  // COMPLETE mode (single mode, alternative to MapReduce): iterate + terminate
  @Test
  public void completeModeDefaultSeed() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new IntersectSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.COMPLETE, inspectors);
      DataToSketchUDAFTest.checkFinalResultInspector(resultInspector);

      IntersectSketchUDAF.IntersectSketchUDAFEvaluator.IntersectionState state =
          (IntersectSketchUDAF.IntersectSketchUDAFEvaluator.IntersectionState) eval.getNewAggregationBuffer();

      UpdateSketch sketch1 = UpdateSketch.builder().build();
      sketch1.update(1);
      sketch1.update(2);
      sketch1.update(3);
      eval.iterate(state, new Object[] {new BytesWritable(sketch1.compact().toByteArray())});

      UpdateSketch sketch2 = UpdateSketch.builder().build();
      sketch2.update(2);
      sketch2.update(3);
      sketch2.update(4);
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

  private static void checkIntermediateResultInspector(ObjectInspector resultInspector) {
    Assert.assertNotNull(resultInspector);
    Assert.assertEquals(resultInspector.getCategory(), ObjectInspector.Category.STRUCT);
    StructObjectInspector structResultInspector = (StructObjectInspector) resultInspector;
    List<?> fields = structResultInspector.getAllStructFieldRefs();
    Assert.assertEquals(fields.size(), 2);

    ObjectInspector inspector1 = ((StructField) fields.get(0)).getFieldObjectInspector();
    Assert.assertEquals(inspector1.getCategory(), ObjectInspector.Category.PRIMITIVE);
    PrimitiveObjectInspector primitiveInspector1 = (PrimitiveObjectInspector) inspector1;
    Assert.assertEquals(primitiveInspector1.getPrimitiveCategory(), PrimitiveCategory.LONG);

    ObjectInspector inspector2 = ((StructField) fields.get(1)).getFieldObjectInspector();
    Assert.assertEquals(inspector2.getCategory(), ObjectInspector.Category.PRIMITIVE);
    PrimitiveObjectInspector primitiveInspector2 = (PrimitiveObjectInspector) inspector2;
    Assert.assertEquals(primitiveInspector2.getPrimitiveCategory(), PrimitiveCategory.BINARY);
  }

}
