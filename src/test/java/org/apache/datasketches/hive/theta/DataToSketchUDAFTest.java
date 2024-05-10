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

import static org.apache.datasketches.thetacommon.ThetaUtil.DEFAULT_NOMINAL_ENTRIES;
import static org.apache.datasketches.thetacommon.ThetaUtil.DEFAULT_UPDATE_SEED;

import java.util.Arrays;
import java.util.List;

import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.Union;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit tests for DataToSketch UDF
 */
@SuppressWarnings({"javadoc","resource"})
public class DataToSketchUDAFTest {

  static final ObjectInspector intInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.INT);

  static final ObjectInspector doubleInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.DOUBLE);

  static final ObjectInspector stringInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.STRING);

  static final ObjectInspector intConstantInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.intTypeInfo, null);

  static final ObjectInspector floatConstantInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.floatTypeInfo, null);

  static final ObjectInspector longConstantInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.longTypeInfo, null);

  static final ObjectInspector binaryInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY);

  static final ObjectInspector structInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
      Arrays.asList("nominalEntries", "seed", "sketch"),
      Arrays.asList(intConstantInspector, longConstantInspector, binaryInspector)
    );

  @Test(expectedExceptions = { UDFArgumentException.class })
  public void initTooFewArguments() throws SemanticException {
    DataToSketchUDAF udf = new DataToSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(new ObjectInspector[] {}, false, false, false);

    udf.getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentException.class })
  public void initTooManyArguments() throws SemanticException {
    DataToSketchUDAF udf = new DataToSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(new ObjectInspector[] {
      intInspector, intConstantInspector, floatConstantInspector, longConstantInspector, longConstantInspector
    }, false, false, false);
    udf.getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void initInvalidCategoryArg1() throws SemanticException {
    DataToSketchUDAF udf = new DataToSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(
        new ObjectInspector[] { structInspector }, false, false, false);
    udf.getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void initInvalidCategoryArg2() throws SemanticException {
    DataToSketchUDAF udf = new DataToSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(
        new ObjectInspector[] { intInspector, structInspector }, false, false, false);
    udf.getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void initInvalidTypeArg2() throws SemanticException {
    DataToSketchUDAF udf = new DataToSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(
        new ObjectInspector[] { intInspector, floatConstantInspector }, false, false, false);
    udf.getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void initInvalidCategoryArg3() throws SemanticException {
    DataToSketchUDAF udf = new DataToSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(
        new ObjectInspector[] { stringInspector, intConstantInspector, structInspector }, false, false, false);
    udf.getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void initInvalidTypeArg3() throws SemanticException {
    DataToSketchUDAF udf = new DataToSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(
        new ObjectInspector[] { stringInspector, intConstantInspector, intConstantInspector }, false, false, false);
    udf.getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void initInvalidCategoryArg4() throws SemanticException {
    DataToSketchUDAF udf = new DataToSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(
        new ObjectInspector[] { stringInspector, intConstantInspector, floatConstantInspector, structInspector }, false, false, false);
    udf.getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void initInvalidTypeArg4() throws SemanticException {
    DataToSketchUDAF udf = new DataToSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(
        new ObjectInspector[] { stringInspector, intConstantInspector, floatConstantInspector, floatConstantInspector }, false, false, false);
    udf.getEvaluator(params);
  }

  // PARTIAL1 mode (Map phase in Map-Reduce): iterate + terminatePartial
  @Test
  public void partial1ModeIntValuesDefaultParams() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new DataToSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.PARTIAL1, inspectors);
      checkIntermediateResultInspector(resultInspector);

      UnionState state = (UnionState) eval.getNewAggregationBuffer();
      eval.iterate(state, new Object[] {new IntWritable(1)});
      eval.iterate(state, new Object[] {new IntWritable(2)});

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
  public void partial1ModeStringValuesExplicitParameters() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { stringInspector, intConstantInspector, floatConstantInspector, longConstantInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new DataToSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.PARTIAL1, inspectors);
      checkIntermediateResultInspector(resultInspector);

      final long seed = 1;
      UnionState state = (UnionState) eval.getNewAggregationBuffer();
      eval.iterate(state, new Object[] {new Text("a"), new IntWritable(16), new FloatWritable(0.99f), new LongWritable(seed)});
      eval.iterate(state, new Object[] {new Text("b"), new IntWritable(16), new FloatWritable(0.99f), new LongWritable(seed)});

      Object result = eval.terminatePartial(state);
      Assert.assertNotNull(result);
      Assert.assertTrue(result instanceof List);
      List<?> r = (List<?>) result;
      Assert.assertEquals(r.size(), 3);
      Assert.assertEquals(((IntWritable) r.get(0)).get(), 16);
      Assert.assertEquals(((LongWritable) r.get(1)).get(), seed);
      Sketch resultSketch = Sketches.wrapSketch(BytesWritableHelper.wrapAsMemory((BytesWritable) r.get(2)), seed);
      // because of sampling probability < 1
      Assert.assertTrue(resultSketch.isEstimationMode());
      Assert.assertEquals(resultSketch.getEstimate(), 2.0, 0.05);

      // check if seed is correct in the result
      Union union = SetOperation.builder().setSeed(seed).buildUnion();
      // this must fail if the seed is incompatible
      union.union(resultSketch);
    }
  }

  // PARTIAL2 mode (Combine phase in Map-Reduce): merge + terminatePartial
  @Test
  public void partial2Mode() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new DataToSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.PARTIAL2, new ObjectInspector[] {structInspector});
      checkIntermediateResultInspector(resultInspector);

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
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new DataToSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.FINAL, new ObjectInspector[] {structInspector});
      checkFinalResultInspector(resultInspector);

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

      Object result = eval.terminate(state);
      Assert.assertNotNull(result);
      Assert.assertTrue(result instanceof BytesWritable);
      Sketch resultSketch = Sketches.wrapSketch(BytesWritableHelper.wrapAsMemory((BytesWritable) result));
      Assert.assertEquals(resultSketch.getEstimate(), 2.0);
    }
  }

  // COMPLETE mode (single mode, alternative to MapReduce): iterate + terminate
  @Test
  public void completeModeIntValuesDefaultParams() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new DataToSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.COMPLETE, inspectors);
      checkFinalResultInspector(resultInspector);

      UnionState state = (UnionState) eval.getNewAggregationBuffer();
      eval.iterate(state, new Object[] {new IntWritable(1)});
      eval.iterate(state, new Object[] {new IntWritable(2)});

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
  public void completeModeDoubleValuesExplicitParameters() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { doubleInspector, intConstantInspector, floatConstantInspector, longConstantInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new DataToSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.COMPLETE, inspectors);
      checkFinalResultInspector(resultInspector);

      final long seed = 2;
      UnionState state = (UnionState) eval.getNewAggregationBuffer();
      eval.iterate(state, new Object[] {new DoubleWritable(1), new IntWritable(16), new FloatWritable(0.99f), new LongWritable(seed)});
      eval.iterate(state, new Object[] {new DoubleWritable(2), new IntWritable(16), new FloatWritable(0.99f), new LongWritable(seed)});

      Object result = eval.terminate(state);
      Assert.assertNotNull(result);
      Assert.assertTrue(result instanceof BytesWritable);
      Sketch resultSketch = Sketches.wrapSketch(BytesWritableHelper.wrapAsMemory((BytesWritable) result), seed);
      // because of sampling probability < 1
      Assert.assertTrue(resultSketch.isEstimationMode());
      Assert.assertEquals(resultSketch.getEstimate(), 2.0, 0.05);
    }
  }

  static void checkIntermediateResultInspector(ObjectInspector resultInspector) {
    Assert.assertNotNull(resultInspector);
    Assert.assertEquals(resultInspector.getCategory(), ObjectInspector.Category.STRUCT);
    StructObjectInspector structResultInspector = (StructObjectInspector) resultInspector;
    List<?> fields = structResultInspector.getAllStructFieldRefs();
    Assert.assertEquals(fields.size(), 3);

    ObjectInspector inspector1 = ((StructField) fields.get(0)).getFieldObjectInspector();
    Assert.assertEquals(inspector1.getCategory(), ObjectInspector.Category.PRIMITIVE);
    PrimitiveObjectInspector primitiveInspector1 = (PrimitiveObjectInspector) inspector1;
    Assert.assertEquals(primitiveInspector1.getPrimitiveCategory(), PrimitiveCategory.INT);

    ObjectInspector inspector2 = ((StructField) fields.get(1)).getFieldObjectInspector();
    Assert.assertEquals(inspector2.getCategory(), ObjectInspector.Category.PRIMITIVE);
    PrimitiveObjectInspector primitiveInspector2 = (PrimitiveObjectInspector) inspector2;
    Assert.assertEquals(primitiveInspector2.getPrimitiveCategory(), PrimitiveCategory.LONG);

    ObjectInspector inspector3 = ((StructField) fields.get(2)).getFieldObjectInspector();
    Assert.assertEquals(inspector3.getCategory(), ObjectInspector.Category.PRIMITIVE);
    PrimitiveObjectInspector primitiveInspector3 = (PrimitiveObjectInspector) inspector3;
    Assert.assertEquals(primitiveInspector3.getPrimitiveCategory(), PrimitiveCategory.BINARY);
}

  static void checkFinalResultInspector(ObjectInspector resultInspector) {
    Assert.assertNotNull(resultInspector);
    Assert.assertEquals(resultInspector.getCategory(), ObjectInspector.Category.PRIMITIVE);
    Assert.assertEquals(
      ((PrimitiveObjectInspector) resultInspector).getPrimitiveCategory(),
      PrimitiveObjectInspector.PrimitiveCategory.BINARY
    );
  }

}
