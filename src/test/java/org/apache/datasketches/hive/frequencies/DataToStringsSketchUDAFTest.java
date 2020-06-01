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

package org.apache.datasketches.hive.frequencies;

import java.util.Arrays;

import org.apache.datasketches.ArrayOfItemsSerDe;
import org.apache.datasketches.ArrayOfStringsSerDe;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.hive.common.BytesWritableHelper;
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
import org.apache.hadoop.io.IntWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

@SuppressWarnings({"javadoc","resource"})
public class DataToStringsSketchUDAFTest {

  static final ArrayOfItemsSerDe<String> serDe = new ArrayOfStringsSerDe();

  static final ObjectInspector stringInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.STRING);

  static final ObjectInspector intInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.INT);

  static final ObjectInspector binaryInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY);

  @Test(expectedExceptions = UDFArgumentException.class)
  public void getEvaluatorTooFewInspectors() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { stringInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new DataToStringsSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentException.class)
  public void getEvaluatorTooManyInspectors() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { stringInspector, intInspector, intInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new DataToStringsSketchUDAF().getEvaluator(info);
  }

  static final ObjectInspector structInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
    Arrays.asList("a"),
    Arrays.asList(intInspector)
  );

  @Test(expectedExceptions = UDFArgumentException.class)
  public void getEvaluatorWrongCategoryArg1() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { structInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new DataToStringsSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentException.class)
  public void getEvaluatorWrongCategoryArg2() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { stringInspector, structInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new DataToStringsSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentException.class)
  public void getEvaluatorWrongTypeArg2() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { stringInspector, stringInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new DataToStringsSketchUDAF().getEvaluator(info);
  }

  @Test
  public void iterateTerminatePartial() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { stringInspector, intInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new DataToStringsSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.PARTIAL1, inspectors);
      checkResultInspector(resultInspector);

      @SuppressWarnings("unchecked")
      ItemsState<String> state = (ItemsState<String>) eval.getNewAggregationBuffer();
      eval.iterate(state, new Object[] { new org.apache.hadoop.io.Text("a"), new IntWritable(256) });
      eval.iterate(state, new Object[] { new org.apache.hadoop.io.Text("b"), new IntWritable(256) });

      BytesWritable bytes = (BytesWritable) eval.terminatePartial(state);
      ItemsSketch<String> resultSketch = ItemsSketch.getInstance(BytesWritableHelper.wrapAsMemory(bytes), serDe);
      Assert.assertEquals(resultSketch.getStreamLength(), 2);
      Assert.assertEquals(resultSketch.getNumActiveItems(), 2);
      Assert.assertEquals(resultSketch.getEstimate("a"), 1);
      Assert.assertEquals(resultSketch.getEstimate("b"), 1);
    }
  }

  @Test
  public void mergeTerminateEmptyState() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { stringInspector, intInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new DataToStringsSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.PARTIAL2, new ObjectInspector[] { binaryInspector });
      checkResultInspector(resultInspector);

      @SuppressWarnings("unchecked")
      ItemsState<String> state = (ItemsState<String>) eval.getNewAggregationBuffer();

      ItemsSketch<String> sketch1 = new ItemsSketch<>(256);
      sketch1.update("a");
      eval.merge(state, new BytesWritable(sketch1.toByteArray(serDe)));

      ItemsSketch<String> sketch2 = new ItemsSketch<>(256);
      sketch2.update("b");
      eval.merge(state, new BytesWritable(sketch2.toByteArray(serDe)));

      BytesWritable bytes = (BytesWritable) eval.terminate(state);
      ItemsSketch<String> resultSketch = ItemsSketch.getInstance(BytesWritableHelper.wrapAsMemory(bytes), serDe);
      Assert.assertEquals(resultSketch.getStreamLength(), 2);
      Assert.assertEquals(resultSketch.getNumActiveItems(), 2);
      Assert.assertEquals(resultSketch.getEstimate("a"), 1);
      Assert.assertEquals(resultSketch.getEstimate("b"), 1);
    }
  }

  @Test
  public void mergeTerminate() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { stringInspector, intInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new DataToStringsSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.PARTIAL2, new ObjectInspector[] { binaryInspector });
      checkResultInspector(resultInspector);

      @SuppressWarnings("unchecked")
      ItemsState<String> state = (ItemsState<String>) eval.getNewAggregationBuffer();
      state.init(256);
      state.update("a");

      ItemsSketch<String> sketch = new ItemsSketch<>(256);
      sketch.update("b");

      eval.merge(state, new BytesWritable(sketch.toByteArray(serDe)));

      BytesWritable bytes = (BytesWritable) eval.terminate(state);
      ItemsSketch<String> resultSketch = ItemsSketch.getInstance(BytesWritableHelper.wrapAsMemory(bytes), serDe);
      Assert.assertEquals(resultSketch.getStreamLength(), 2);
      Assert.assertEquals(resultSketch.getNumActiveItems(), 2);
      Assert.assertEquals(resultSketch.getEstimate("a"), 1);
      Assert.assertEquals(resultSketch.getEstimate("b"), 1);
    }
  }

  private static void checkResultInspector(ObjectInspector resultInspector) {
    Assert.assertNotNull(resultInspector);
    Assert.assertEquals(resultInspector.getCategory(), ObjectInspector.Category.PRIMITIVE);
    Assert.assertEquals(
      ((PrimitiveObjectInspector) resultInspector).getPrimitiveCategory(),
      PrimitiveObjectInspector.PrimitiveCategory.BINARY
    );
  }

}
