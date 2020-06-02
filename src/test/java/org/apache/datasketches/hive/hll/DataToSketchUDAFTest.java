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

package org.apache.datasketches.hive.hll;

import java.util.Arrays;
import java.util.List;

import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.testng.Assert;
import org.testng.annotations.Test;

@SuppressWarnings({"javadoc","resource"})
public class DataToSketchUDAFTest {

  private static final ObjectInspector intInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.INT);

  private static final ObjectInspector doubleInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.DOUBLE);

  private static final ObjectInspector stringInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.STRING);

  private static final ObjectInspector binaryInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY);

  private static final ObjectInspector structInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
      Arrays.asList("lgK", "type", "sketch"),
      Arrays.asList(intInspector, stringInspector, binaryInspector)
    );

  static final ObjectInspector intConstantInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.intTypeInfo, null);

  static final ObjectInspector stringConstantInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, null);

  @Test(expectedExceptions = { UDFArgumentException.class })
  public void tooFewArguments() throws SemanticException {
    ObjectInspector[] inspectors = new ObjectInspector[] { };
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new DataToSketchUDAF().getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentException.class })
  public void tooManyArguments() throws SemanticException {
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector, intConstantInspector, stringConstantInspector, stringConstantInspector };
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new DataToSketchUDAF().getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void invalidCategoryArg1() throws SemanticException {
    ObjectInspector[] inspectors = new ObjectInspector[] { structInspector };
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new DataToSketchUDAF().getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentException.class })
  public void invalidCategoryArg2() throws SemanticException {
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector, structInspector };
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new DataToSketchUDAF().getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentException.class })
  public void invalidTypeArg2() throws SemanticException {
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector, stringInspector };
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new DataToSketchUDAF().getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentException.class })
  public void arg2notConst() throws SemanticException {
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector, intInspector };
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new DataToSketchUDAF().getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void invalidCategoryArg3() throws SemanticException {
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector, intConstantInspector, structInspector };
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new DataToSketchUDAF().getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void invalidTypeArg3() throws SemanticException {
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector, intConstantInspector, doubleInspector };
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new DataToSketchUDAF().getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void arg3notConst() throws SemanticException {
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector, intConstantInspector, stringInspector };
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new DataToSketchUDAF().getEvaluator(params);
  }

  // PARTIAL1 mode (Map phase in Map-Reduce): iterate + terminatePartial
  @Test
  public void partial1ModeIntKeysDefaultParams() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new DataToSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.PARTIAL1, inspectors);
      checkIntermediateResultInspector(resultInspector);

      State state = (State) eval.getNewAggregationBuffer();
      eval.iterate(state, new Object[] {new IntWritable(1)});
      eval.iterate(state, new Object[] {new IntWritable(2)});

      Object result = eval.terminatePartial(state);
      Assert.assertNotNull(result);
      Assert.assertTrue(result instanceof List);
      List<?> r = (List<?>) result;
      Assert.assertEquals(r.size(), 3);
      Assert.assertEquals(((IntWritable) r.get(0)).get(), SketchEvaluator.DEFAULT_LG_K);
      Assert.assertEquals(((Text) r.get(1)).toString(), SketchEvaluator.DEFAULT_HLL_TYPE.toString());
      HllSketch resultSketch = HllSketch.heapify(BytesWritableHelper.wrapAsMemory((BytesWritable) r.get(2)));
      Assert.assertEquals(resultSketch.getEstimate(), 2.0, 0.01);
    }
  }

  @Test
  public void partial1ModeStringKeysExplicitParams() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { stringInspector, intConstantInspector, stringConstantInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new DataToSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.PARTIAL1, inspectors);
      checkIntermediateResultInspector(resultInspector);

      final int lgK = 10;
      final TgtHllType hllType = TgtHllType.HLL_8;

      State state = (State) eval.getNewAggregationBuffer();
      eval.iterate(state, new Object[] {new Text("a"), new IntWritable(lgK), new Text(hllType.toString())});
      eval.iterate(state, new Object[] {new Text("b"), new IntWritable(lgK), new Text(hllType.toString())});

      Object result = eval.terminatePartial(state);
      Assert.assertNotNull(result);
      Assert.assertTrue(result instanceof List);
      List<?> r = (List<?>) result;
      Assert.assertEquals(r.size(), 3);
      Assert.assertEquals(((IntWritable) r.get(0)).get(), lgK);
      Assert.assertEquals(((Text) r.get(1)).toString(), hllType.toString());
      HllSketch resultSketch = HllSketch.heapify(BytesWritableHelper.wrapAsMemory((BytesWritable) r.get(2)));
      Assert.assertEquals(resultSketch.getLgConfigK(), lgK);
      Assert.assertEquals(resultSketch.getTgtHllType(), hllType);
      Assert.assertEquals(resultSketch.getEstimate(), 2.0, 0.01);
    }
  }

  // A user reported a problem, which seems to be a case of Hive calling getNewAggregationBuffer() before init()
  @Test
  public void partial1ModeGetStateBeforeInit() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new DataToSketchUDAF().getEvaluator(info)) {
      State state = (State) eval.getNewAggregationBuffer();
      ObjectInspector resultInspector = eval.init(Mode.PARTIAL1, inspectors);
      checkIntermediateResultInspector(resultInspector);

      eval.iterate(state, new Object[] {new IntWritable(1)});
      eval.iterate(state, new Object[] {new IntWritable(2)});

      Object result = eval.terminatePartial(state);
      Assert.assertNotNull(result);
      Assert.assertTrue(result instanceof List);
      List<?> r = (List<?>) result;
      Assert.assertEquals(r.size(), 3);
      Assert.assertEquals(((IntWritable) r.get(0)).get(), SketchEvaluator.DEFAULT_LG_K);
      Assert.assertEquals(((Text) r.get(1)).toString(), SketchEvaluator.DEFAULT_HLL_TYPE.toString());
      HllSketch resultSketch = HllSketch.wrap(BytesWritableHelper.wrapAsMemory((BytesWritable) r.get(2)));
      Assert.assertEquals(resultSketch.getEstimate(), 2.0, 0.01);
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

      State state = (State) eval.getNewAggregationBuffer();

      HllSketch sketch1 = new HllSketch(SketchEvaluator.DEFAULT_LG_K, SketchEvaluator.DEFAULT_HLL_TYPE);
      sketch1.update(1);
      eval.merge(state, Arrays.asList(
        new IntWritable(SketchEvaluator.DEFAULT_LG_K),
        new Text(SketchEvaluator.DEFAULT_HLL_TYPE.toString()),
        new BytesWritable(sketch1.toCompactByteArray()))
      );

      HllSketch sketch2 = new HllSketch(SketchEvaluator.DEFAULT_LG_K, SketchEvaluator.DEFAULT_HLL_TYPE);
      sketch2.update(2);
      eval.merge(state, Arrays.asList(
          new IntWritable(SketchEvaluator.DEFAULT_LG_K),
          new Text(SketchEvaluator.DEFAULT_HLL_TYPE.toString()),
          new BytesWritable(sketch2.toCompactByteArray()))
      );

      Object result = eval.terminatePartial(state);
      Assert.assertNotNull(result);
      Assert.assertTrue(result instanceof List);
      List<?> r = (List<?>) result;
      Assert.assertEquals(r.size(), 3);
      Assert.assertEquals(((IntWritable) r.get(0)).get(), SketchEvaluator.DEFAULT_LG_K);
      Assert.assertEquals(((Text) r.get(1)).toString(), SketchEvaluator.DEFAULT_HLL_TYPE.toString());
      HllSketch resultSketch = HllSketch.heapify(BytesWritableHelper.wrapAsMemory((BytesWritable) r.get(2)));
      Assert.assertEquals(resultSketch.getEstimate(), 2.0, 0.01);

      eval.reset(state);
      result = eval.terminate(state);
      Assert.assertNull(result);
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

      State state = (State) eval.getNewAggregationBuffer();

      HllSketch sketch1 = new HllSketch(SketchEvaluator.DEFAULT_LG_K);
      sketch1.update(1);
      eval.merge(state, Arrays.asList(
        new IntWritable(SketchEvaluator.DEFAULT_LG_K),
        new Text(SketchEvaluator.DEFAULT_HLL_TYPE.toString()),
        new BytesWritable(sketch1.toCompactByteArray()))
      );

      HllSketch sketch2 = new HllSketch(SketchEvaluator.DEFAULT_LG_K);
      sketch2.update(2);
      eval.merge(state, Arrays.asList(
          new IntWritable(SketchEvaluator.DEFAULT_LG_K),
          new Text(SketchEvaluator.DEFAULT_HLL_TYPE.toString()),
          new BytesWritable(sketch2.toCompactByteArray()))
      );

      Object result = eval.terminate(state);
      Assert.assertNotNull(result);
      Assert.assertTrue(result instanceof BytesWritable);
      HllSketch resultSketch = HllSketch.heapify(BytesWritableHelper.wrapAsMemory((BytesWritable) result));
      Assert.assertEquals(resultSketch.getEstimate(), 2.0, 0.01);
    }
  }

  // COMPLETE mode (single mode, alternative to MapReduce): iterate + terminate
  @Test
  public void completeModeIntKeysDefaultParams() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new DataToSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.COMPLETE, inspectors);
      checkFinalResultInspector(resultInspector);

      State state = (State) eval.getNewAggregationBuffer();
      eval.iterate(state, new Object[] {new IntWritable(1)});
      eval.iterate(state, new Object[] {new IntWritable(2)});

      Object result = eval.terminate(state);
      Assert.assertNotNull(result);
      Assert.assertTrue(result instanceof BytesWritable);
      HllSketch resultSketch = HllSketch.heapify(BytesWritableHelper.wrapAsMemory((BytesWritable) result));
      Assert.assertEquals(resultSketch.getEstimate(), 2.0, 0.01);

      eval.reset(state);
      result = eval.terminate(state);
      Assert.assertNull(result);
    }
  }

  // COMPLETE mode (single mode, alternative to MapReduce): iterate + terminate
  @Test
  public void completeModeDoubleKeysExplicitParams() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { doubleInspector, intConstantInspector, stringConstantInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new DataToSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.COMPLETE, inspectors);
      checkFinalResultInspector(resultInspector);

      final int lgK = 4;
      final TgtHllType hllType = TgtHllType.HLL_6;

      State state = (State) eval.getNewAggregationBuffer();
      eval.iterate(state, new Object[] {new DoubleWritable(1), new IntWritable(lgK), new Text(hllType.toString())});
      eval.iterate(state, new Object[] {new DoubleWritable(2), new IntWritable(lgK), new Text(hllType.toString())});

      Object result = eval.terminate(state);
      Assert.assertNotNull(result);
      Assert.assertTrue(result instanceof BytesWritable);
      HllSketch resultSketch = HllSketch.heapify(BytesWritableHelper.wrapAsMemory((BytesWritable) result));
      Assert.assertEquals(resultSketch.getLgConfigK(), lgK);
      Assert.assertEquals(resultSketch.getTgtHllType(), hllType);
      Assert.assertEquals(resultSketch.getEstimate(), 2.0, 0.01);

      eval.reset(state);
      result = eval.terminate(state);
      Assert.assertNull(result);
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
    Assert.assertEquals(primitiveInspector2.getPrimitiveCategory(), PrimitiveCategory.STRING);

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
