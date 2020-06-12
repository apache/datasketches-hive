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

package org.apache.datasketches.hive.quantiles;

import java.util.Arrays;

import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;
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

@SuppressWarnings({"javadoc","resource"})
public class UnionDoublesSketchUDAFTest {

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
    new UnionDoublesSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentException.class)
  public void getEvaluatorTooManyInspectors() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector, binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new UnionDoublesSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentTypeException.class)
  public void getEvaluatorWrongCategoryArg1() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { structInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new UnionDoublesSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentTypeException.class)
  public void getEvaluatorWrongTypeArg1() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new UnionDoublesSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentTypeException.class)
  public void getEvaluatorWrongCategoryArg2() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector, structInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new UnionDoublesSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentTypeException.class)
  public void getEvaluatorWrongTypeArg2() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector, binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new UnionDoublesSketchUDAF().getEvaluator(info);
  }

  // PARTIAL1 mode (Map phase in Map-Reduce): iterate + terminatePartial
  @Test
  public void partia1ModelDefaultKDowsizeInput() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new UnionDoublesSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.PARTIAL1, inspectors);
      DataToDoublesSketchUDAFTest.checkResultInspector(resultInspector);

      DoublesUnionState state = (DoublesUnionState) eval.getNewAggregationBuffer();

      UpdateDoublesSketch sketch1 = DoublesSketch.builder().setK(256).build();
      sketch1.update(1.0);
      eval.iterate(state, new Object[] { new BytesWritable(sketch1.toByteArray()) });

      UpdateDoublesSketch sketch2 = DoublesSketch.builder().setK(256).build();
      sketch2.update(2.0);
      eval.iterate(state, new Object[] { new BytesWritable(sketch2.toByteArray()) });

      BytesWritable bytes = (BytesWritable) eval.terminatePartial(state);
      DoublesSketch resultSketch = DoublesSketch.wrap(BytesWritableHelper.wrapAsMemory(bytes));
      Assert.assertEquals(resultSketch.getK(), 128);
      Assert.assertEquals(resultSketch.getRetainedItems(), 2);
      Assert.assertEquals(resultSketch.getMinValue(), 1.0);
      Assert.assertEquals(resultSketch.getMaxValue(), 2.0);
    }
  }

  @Test
  public void partia1ModelGivenK() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector, intInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new UnionDoublesSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.PARTIAL1, inspectors);
      DataToDoublesSketchUDAFTest.checkResultInspector(resultInspector);

      DoublesUnionState state = (DoublesUnionState) eval.getNewAggregationBuffer();

      UpdateDoublesSketch sketch1 = DoublesSketch.builder().setK(256).build();
      sketch1.update(1.0);
      eval.iterate(state, new Object[] { new BytesWritable(sketch1.toByteArray()), new IntWritable(256) });

      UpdateDoublesSketch sketch2 = DoublesSketch.builder().setK(256).build();
      sketch2.update(2.0);
      eval.iterate(state, new Object[] { new BytesWritable(sketch2.toByteArray()), new IntWritable(256) });

      BytesWritable bytes = (BytesWritable) eval.terminatePartial(state);
      DoublesSketch resultSketch = DoublesSketch.wrap(BytesWritableHelper.wrapAsMemory(bytes));
      Assert.assertEquals(resultSketch.getK(), 256);
      Assert.assertEquals(resultSketch.getRetainedItems(), 2);
      Assert.assertEquals(resultSketch.getMinValue(), 1.0);
      Assert.assertEquals(resultSketch.getMaxValue(), 2.0);
    }
  }

  // PARTIAL2 mode (Combine phase in Map-Reduce): merge + terminatePartial
  @Test
  public void partial2Mode() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new UnionDoublesSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.PARTIAL2, inspectors);
      DataToDoublesSketchUDAFTest.checkResultInspector(resultInspector);

      DoublesUnionState state = (DoublesUnionState) eval.getNewAggregationBuffer();

      UpdateDoublesSketch sketch1 = DoublesSketch.builder().setK(256).build();
      sketch1.update(1.0);
      eval.merge(state, new BytesWritable(sketch1.toByteArray()));

      UpdateDoublesSketch sketch2 = DoublesSketch.builder().setK(256).build();
      sketch2.update(2.0);
      eval.merge(state, new BytesWritable(sketch2.toByteArray()));

      BytesWritable bytes = (BytesWritable) eval.terminatePartial(state);
      DoublesSketch resultSketch = DoublesSketch.wrap(BytesWritableHelper.wrapAsMemory(bytes));
      Assert.assertEquals(resultSketch.getK(), 256);
      Assert.assertEquals(resultSketch.getRetainedItems(), 2);
      Assert.assertEquals(resultSketch.getMinValue(), 1.0);
      Assert.assertEquals(resultSketch.getMaxValue(), 2.0);
    }
  }

  // FINAL mode (Reduce phase in Map-Reduce): merge + terminate
  @Test
  public void finalMode() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new UnionDoublesSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.FINAL, inspectors);
      DataToDoublesSketchUDAFTest.checkResultInspector(resultInspector);

      DoublesUnionState state = (DoublesUnionState) eval.getNewAggregationBuffer();

      UpdateDoublesSketch sketch1 = DoublesSketch.builder().setK(256).build();
      sketch1.update(1.0);
      eval.merge(state, new BytesWritable(sketch1.toByteArray()));

      UpdateDoublesSketch sketch2 = DoublesSketch.builder().setK(256).build();
      sketch2.update(2.0);
      eval.merge(state, new BytesWritable(sketch2.toByteArray()));

      BytesWritable bytes = (BytesWritable) eval.terminate(state);
      DoublesSketch resultSketch = DoublesSketch.wrap(BytesWritableHelper.wrapAsMemory(bytes));
      Assert.assertEquals(resultSketch.getK(), 256);
      Assert.assertEquals(resultSketch.getRetainedItems(), 2);
      Assert.assertEquals(resultSketch.getMinValue(), 1.0);
      Assert.assertEquals(resultSketch.getMaxValue(), 2.0);
    }
  }

  // COMPLETE mode (single mode, alternative to MapReduce): iterate + terminate
  @Test
  public void completeModelDefaultK() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new UnionDoublesSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.COMPLETE, inspectors);
      DataToDoublesSketchUDAFTest.checkResultInspector(resultInspector);

      DoublesUnionState state = (DoublesUnionState) eval.getNewAggregationBuffer();

      UpdateDoublesSketch sketch1 = DoublesSketch.builder().build();
      sketch1.update(1.0);
      eval.iterate(state, new Object[] { new BytesWritable(sketch1.toByteArray()) });

      UpdateDoublesSketch sketch2 = DoublesSketch.builder().build();
      sketch2.update(2.0);
      eval.iterate(state, new Object[] { new BytesWritable(sketch2.toByteArray()) });

      BytesWritable bytes = (BytesWritable) eval.terminatePartial(state);
      DoublesSketch resultSketch = DoublesSketch.wrap(BytesWritableHelper.wrapAsMemory(bytes));
      Assert.assertEquals(resultSketch.getK(), 128);
      Assert.assertEquals(resultSketch.getRetainedItems(), 2);
      Assert.assertEquals(resultSketch.getMinValue(), 1.0);
      Assert.assertEquals(resultSketch.getMaxValue(), 2.0);

      eval.reset(state);
      Assert.assertNull(eval.terminate(state));
    }
  }

}
