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
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
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
import org.testng.Assert;
import org.testng.annotations.Test;

@SuppressWarnings({"javadoc","resource"})
public class UnionStringsSketchUDAFTest {

  static final ArrayOfItemsSerDe<String> serDe = new ArrayOfStringsSerDe();

  static final ObjectInspector binaryInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY);

  @Test(expectedExceptions = UDFArgumentException.class)
  public void getEvaluatorTooFewInspectors() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new UnionStringsSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentException.class)
  public void getEvaluatorTooManyInspectors() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector, binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new UnionStringsSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentTypeException.class)
  public void getEvaluatorWrongType() throws Exception {
    ObjectInspector intInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.INT);

    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new UnionStringsSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentTypeException.class)
  public void getEvaluatorWrongCategory() throws Exception {
    ObjectInspector structInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
      Arrays.asList("a"),
      Arrays.asList(binaryInspector)
    );
    ObjectInspector[] inspectors = new ObjectInspector[] { structInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new UnionStringsSketchUDAF().getEvaluator(info);
  }

  @Test
  public void iterateTerminatePartial() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new UnionStringsSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.PARTIAL1, inspectors);
      checkResultInspector(resultInspector);

      @SuppressWarnings("unchecked")
      ItemsState<String> state = (ItemsState<String>) eval.getNewAggregationBuffer();
      state.init(256);
      state.update("a");

      ItemsSketch<String> sketch = new ItemsSketch<>(256);
      sketch.update("b");

      eval.iterate(state, new Object[] { new BytesWritable(sketch.toByteArray(serDe)) });

      BytesWritable bytes = (BytesWritable) eval.terminatePartial(state);
      ItemsSketch<String> resultSketch = ItemsSketch.getInstance(BytesWritableHelper.wrapAsMemory(bytes), serDe);
      Assert.assertEquals(resultSketch.getStreamLength(), 2);
      Assert.assertEquals(resultSketch.getNumActiveItems(), 2);
      Assert.assertEquals(resultSketch.getEstimate("a"), 1);
      Assert.assertEquals(resultSketch.getEstimate("b"), 1);
    }
  }

  @Test
  public void mergeTerminate() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new UnionStringsSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.PARTIAL2, inspectors);
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
