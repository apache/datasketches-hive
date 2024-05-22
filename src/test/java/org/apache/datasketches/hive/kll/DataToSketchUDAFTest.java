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

package org.apache.datasketches.hive.kll;

import java.util.Arrays;

import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

@SuppressWarnings({"javadoc","resource"})
public class DataToSketchUDAFTest {

  static final ObjectInspector floatInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.FLOAT);

  static final ObjectInspector intInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.INT);

  static final ObjectInspector binaryInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY);

  static final ObjectInspector structInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
      Arrays.asList("a"),
      Arrays.asList(intInspector)
    );

  @Test(expectedExceptions = UDFArgumentException.class)
  public void getEvaluatorTooFewInspectors() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new DataToSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentException.class)
  public void getEvaluatorTooManyInspectors() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { floatInspector, intInspector, intInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new DataToSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentException.class)
  public void getEvaluatorWrongCategoryArg1() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { structInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new DataToSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentException.class)
  public void getEvaluatorWrongTypeArg1() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new DataToSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentException.class)
  public void getEvaluatorWrongCategoryArg2() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { floatInspector, structInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new DataToSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentException.class)
  public void getEvaluatorWrongTypeArg2() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { floatInspector, floatInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new DataToSketchUDAF().getEvaluator(info);
  }

  // PARTIAL1 mode (Map phase in Map-Reduce): iterate + terminatePartial
  @Test
  public void partial1ModeDefaultK() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { floatInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new DataToSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.PARTIAL1, inspectors);
      checkResultInspector(resultInspector);

      SketchState state = (SketchState) eval.getNewAggregationBuffer();
      eval.iterate(state, new Object[] { new FloatWritable(1) });
      eval.iterate(state, new Object[] { new FloatWritable(2) });

      BytesWritable bytes = (BytesWritable) eval.terminatePartial(state);
      KllFloatsSketch resultSketch = KllFloatsSketch.heapify(BytesWritableHelper.wrapAsMemory(bytes));
      Assert.assertEquals(resultSketch.getNormalizedRankError(false), KllFloatsSketch.getNormalizedRankError(200, false));
      Assert.assertEquals(resultSketch.getNumRetained(), 2);
      Assert.assertEquals(resultSketch.getMinItem(), 1f);
      Assert.assertEquals(resultSketch.getMaxItem(), 2f);
    }
  }

  @Test
  public void partial1ModeGivenK() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { floatInspector, intInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new DataToSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.PARTIAL1, inspectors);
      checkResultInspector(resultInspector);

      SketchState state = (SketchState) eval.getNewAggregationBuffer();
      eval.iterate(state, new Object[] { new FloatWritable(1), new IntWritable(400) });
      eval.iterate(state, new Object[] { new FloatWritable(2), new IntWritable(400) });

      BytesWritable bytes = (BytesWritable) eval.terminatePartial(state);
      KllFloatsSketch resultSketch = KllFloatsSketch.heapify(BytesWritableHelper.wrapAsMemory(bytes));
      Assert.assertEquals(resultSketch.getNormalizedRankError(false), KllFloatsSketch.getNormalizedRankError(400, false));
      Assert.assertEquals(resultSketch.getNumRetained(), 2);
      Assert.assertEquals(resultSketch.getMinItem(), 1f);
      Assert.assertEquals(resultSketch.getMaxItem(), 2f);
    }
  }

  // PARTIAL2 mode (Combine phase in Map-Reduce): merge + terminatePartial
  @Test
  public void partial2Mode() throws Exception {
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(new ObjectInspector[] { floatInspector }, false, false, false);
    try (GenericUDAFEvaluator eval = new DataToSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.PARTIAL2, new ObjectInspector[] {binaryInspector});
      checkResultInspector(resultInspector);

      SketchState state = (SketchState) eval.getNewAggregationBuffer();

      KllFloatsSketch sketch1 = KllFloatsSketch.newHeapInstance();
      sketch1.update(1);
      eval.merge(state, new BytesWritable(sketch1.toByteArray()));

      KllFloatsSketch sketch2 = KllFloatsSketch.newHeapInstance();
      sketch2.update(2);
      eval.merge(state, new BytesWritable(sketch2.toByteArray()));

      BytesWritable bytes = (BytesWritable) eval.terminate(state);
      KllFloatsSketch resultSketch = KllFloatsSketch.heapify(BytesWritableHelper.wrapAsMemory(bytes));
      Assert.assertEquals(resultSketch.getNumRetained(), 2);
      Assert.assertEquals(resultSketch.getMinItem(), 1f);
      Assert.assertEquals(resultSketch.getMaxItem(), 2f);
    }
  }

  // FINAL mode (Reduce phase in Map-Reduce): merge + terminate
  @Test
  public void finalMode() throws Exception {
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(new ObjectInspector[] { floatInspector }, false, false, false);
    try (GenericUDAFEvaluator eval = new DataToSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.FINAL, new ObjectInspector[] {binaryInspector});
      checkResultInspector(resultInspector);

      SketchState state = (SketchState) eval.getNewAggregationBuffer();

      KllFloatsSketch sketch1 = KllFloatsSketch.newHeapInstance(400); // to check if K is preserved
      sketch1.update(1);
      eval.merge(state, new BytesWritable(sketch1.toByteArray()));

      KllFloatsSketch sketch2 = KllFloatsSketch.newHeapInstance(400);
      sketch2.update(2);
      eval.merge(state, new BytesWritable(sketch2.toByteArray()));

      BytesWritable bytes = (BytesWritable) eval.terminate(state);
      KllFloatsSketch resultSketch = KllFloatsSketch.heapify(BytesWritableHelper.wrapAsMemory(bytes));
      Assert.assertEquals(resultSketch.getNormalizedRankError(false), KllFloatsSketch.getNormalizedRankError(400, false));
      Assert.assertEquals(resultSketch.getNumRetained(), 2);
      Assert.assertEquals(resultSketch.getMinItem(), 1f);
      Assert.assertEquals(resultSketch.getMaxItem(), 2f);
    }
  }

  // COMPLETE mode (single mode, alternative to MapReduce): iterate + terminate
  @Test
  public void completeModeDefaultK() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { floatInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new DataToSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.COMPLETE, inspectors);
      checkResultInspector(resultInspector);

      SketchState state = (SketchState) eval.getNewAggregationBuffer();
      eval.iterate(state, new Object[] { new FloatWritable(1) });
      eval.iterate(state, new Object[] { new FloatWritable(2) });

      BytesWritable bytes = (BytesWritable) eval.terminate(state);
      KllFloatsSketch resultSketch = KllFloatsSketch.heapify(BytesWritableHelper.wrapAsMemory(bytes));
      Assert.assertEquals(resultSketch.getNormalizedRankError(false), KllFloatsSketch.getNormalizedRankError(200, false));
      Assert.assertEquals(resultSketch.getNumRetained(), 2);
      Assert.assertEquals(resultSketch.getMinItem(), 1f);
      Assert.assertEquals(resultSketch.getMaxItem(), 2f);
    }
  }

  @Test
  public void completeModeGivenK() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { floatInspector, intInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new DataToSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.COMPLETE, inspectors);
      checkResultInspector(resultInspector);

      SketchState state = (SketchState) eval.getNewAggregationBuffer();
      eval.iterate(state, new Object[] { new FloatWritable(1), new IntWritable(400) });
      eval.iterate(state, new Object[] { new FloatWritable(2), new IntWritable(400) });

      BytesWritable bytes = (BytesWritable) eval.terminate(state);
      KllFloatsSketch resultSketch = KllFloatsSketch.heapify(BytesWritableHelper.wrapAsMemory(bytes));
      Assert.assertEquals(resultSketch.getNormalizedRankError(false), KllFloatsSketch.getNormalizedRankError(400, false));
      Assert.assertEquals(resultSketch.getNumRetained(), 2);
      Assert.assertEquals(resultSketch.getMinItem(), 1f);
      Assert.assertEquals(resultSketch.getMaxItem(), 2f);
    }
  }

  static void checkResultInspector(ObjectInspector resultInspector) {
    Assert.assertNotNull(resultInspector);
    Assert.assertEquals(resultInspector.getCategory(), ObjectInspector.Category.PRIMITIVE);
    Assert.assertEquals(
      ((PrimitiveObjectInspector) resultInspector).getPrimitiveCategory(),
      PrimitiveObjectInspector.PrimitiveCategory.BINARY
    );
  }

}
