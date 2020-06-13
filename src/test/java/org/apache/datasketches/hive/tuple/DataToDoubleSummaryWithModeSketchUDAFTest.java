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

package org.apache.datasketches.hive.tuple;

import static org.apache.datasketches.Util.DEFAULT_NOMINAL_ENTRIES;

import java.util.Arrays;
import java.util.List;

import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.datasketches.tuple.Sketch;
import org.apache.datasketches.tuple.SketchIterator;
import org.apache.datasketches.tuple.Sketches;
import org.apache.datasketches.tuple.UpdatableSketch;
import org.apache.datasketches.tuple.UpdatableSketchBuilder;
import org.apache.datasketches.tuple.adouble.DoubleSummary;
import org.apache.datasketches.tuple.adouble.DoubleSummaryDeserializer;
import org.apache.datasketches.tuple.adouble.DoubleSummaryFactory;
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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.testng.Assert;
import org.testng.annotations.Test;

@SuppressWarnings({"javadoc","resource"})
public class DataToDoubleSummaryWithModeSketchUDAFTest {

  private static final ObjectInspector intInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.INT);

  private static final ObjectInspector doubleInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.DOUBLE);

  private static final ObjectInspector stringInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.STRING);

  private static final ObjectInspector floatInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.FLOAT);

  private static final ObjectInspector binaryInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY);

  private static final ObjectInspector structInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
      Arrays.asList("nominalEntries", "summaryMode", "sketch"),
      Arrays.asList(intInspector, stringInspector, binaryInspector)
    );

  @Test(expectedExceptions = { UDFArgumentException.class })
  public void tooFewArguments() throws SemanticException {
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector };
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new DataToDoubleSummaryWithModeSketchUDAF().getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentException.class })
  public void tooManyArguments() throws SemanticException {
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector, doubleInspector, intInspector, floatInspector, stringInspector, intInspector };
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new DataToDoubleSummaryWithModeSketchUDAF().getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void invalidCategoryArg1() throws SemanticException {
    ObjectInspector[] inspectors = new ObjectInspector[] { structInspector, doubleInspector };
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new DataToDoubleSummaryWithModeSketchUDAF().getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentException.class })
  public void invalidCategoryArg2() throws SemanticException {
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector, structInspector };
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new DataToDoubleSummaryWithModeSketchUDAF().getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentException.class })
  public void invalidTypeArg2() throws SemanticException {
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector, intInspector };
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new DataToDoubleSummaryWithModeSketchUDAF().getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void invalidCategoryArg3() throws SemanticException {
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector, doubleInspector, structInspector };
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new DataToDoubleSummaryWithModeSketchUDAF().getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void invalidTypeArg3() throws SemanticException {
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector, doubleInspector, doubleInspector };
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new DataToDoubleSummaryWithModeSketchUDAF().getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void invalidCategoryArg4() throws SemanticException {
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector, doubleInspector, intInspector, structInspector };
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new DataToDoubleSummaryWithModeSketchUDAF().getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void invalidTypeArg4() throws SemanticException {
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector, doubleInspector, intInspector, intInspector };
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new DataToDoubleSummaryWithModeSketchUDAF().getEvaluator(params);
  }

  // PARTIAL1 mode (Map phase in Map-Reduce): iterate + terminatePartial
  @Test
  public void partial1ModeIntKeysDefaultParams() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector, doubleInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new DataToDoubleSummaryWithModeSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.PARTIAL1, inspectors);
      checkIntermediateResultInspector(resultInspector);

      @SuppressWarnings("unchecked")
      State<DoubleSummary> state = (State<DoubleSummary>) eval.getNewAggregationBuffer();
      eval.iterate(state, new Object[] {new IntWritable(1), new DoubleWritable(1)});
      eval.iterate(state, new Object[] {new IntWritable(2), new DoubleWritable(2)});
      eval.iterate(state, new Object[] {new IntWritable(1), new DoubleWritable(2)});
      eval.iterate(state, new Object[] {new IntWritable(2), new DoubleWritable(1)});

      Object result = eval.terminatePartial(state);
      Assert.assertNotNull(result);
      Assert.assertTrue(result instanceof List);
      List<?> r = (List<?>) result;
      Assert.assertEquals(r.size(), 3);
      Assert.assertEquals(((IntWritable) r.get(0)).get(), DEFAULT_NOMINAL_ENTRIES);
      Assert.assertEquals(((Text) r.get(1)).toString(), DoubleSummary.Mode.Sum.toString());
      Sketch<DoubleSummary> resultSketch = Sketches.heapifySketch(
          BytesWritableHelper.wrapAsMemory((BytesWritable) r.get(2)), new DoubleSummaryDeserializer());
      Assert.assertFalse(resultSketch.isEstimationMode());
      Assert.assertEquals(resultSketch.getEstimate(), 2.0);
      SketchIterator<DoubleSummary> it = resultSketch.iterator();
      while (it.next()) {
        Assert.assertEquals(it.getSummary().getValue(), 3.0);
      }
    }
  }

  @Test
  public void partial1ModeStringKeysExplicitParams() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { stringInspector, doubleInspector, intInspector, floatInspector, stringInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new DataToDoubleSummaryWithModeSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.PARTIAL1, inspectors);
      checkIntermediateResultInspector(resultInspector);

      @SuppressWarnings("unchecked")
      State<DoubleSummary> state = (State<DoubleSummary>) eval.getNewAggregationBuffer();
      eval.iterate(state, new Object[] {new Text("a"), new DoubleWritable(1), new IntWritable(32), new FloatWritable(0.99f), new Text("Min")});
      eval.iterate(state, new Object[] {new Text("b"), new DoubleWritable(2), new IntWritable(32), new FloatWritable(0.99f), new Text("Min")});
      eval.iterate(state, new Object[] {new Text("a"), new DoubleWritable(2), new IntWritable(32), new FloatWritable(0.99f), new Text("Min")});
      eval.iterate(state, new Object[] {new Text("b"), new DoubleWritable(1), new IntWritable(32), new FloatWritable(0.99f), new Text("Min")});

      Object result = eval.terminatePartial(state);
      Assert.assertNotNull(result);
      Assert.assertTrue(result instanceof List);
      List<?> r = (List<?>) result;
      Assert.assertEquals(r.size(), 3);
      Assert.assertEquals(((IntWritable) r.get(0)).get(), 32);
      Assert.assertEquals(((Text) r.get(1)).toString(), DoubleSummary.Mode.Min.toString());
      Sketch<DoubleSummary> resultSketch = Sketches.heapifySketch(
          BytesWritableHelper.wrapAsMemory((BytesWritable) r.get(2)), new DoubleSummaryDeserializer());
      // because of sampling probability < 1
      Assert.assertTrue(resultSketch.isEstimationMode());
      Assert.assertEquals(resultSketch.getEstimate(), 2.0, 0.05);
      SketchIterator<DoubleSummary> it = resultSketch.iterator();
      while (it.next()) {
        Assert.assertEquals(it.getSummary().getValue(), 1.0);
      }
    }
  }

  // PARTIAL2 mode (Combine phase in Map-Reduce): merge + terminatePartial
  @Test
  public void partial2Mode() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector, doubleInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new DataToDoubleSummaryWithModeSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.PARTIAL2, new ObjectInspector[] {structInspector});
      checkIntermediateResultInspector(resultInspector);

      @SuppressWarnings("unchecked")
      State<DoubleSummary> state = (State<DoubleSummary>) eval.getNewAggregationBuffer();

      UpdatableSketch<Double, DoubleSummary> sketch1 =
          new UpdatableSketchBuilder<>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).build();
      sketch1.update(1, 1.0);
      sketch1.update(2, 2.0);
      eval.merge(state, Arrays.asList(
        new IntWritable(DEFAULT_NOMINAL_ENTRIES),
        new Text("Sum"),
        new BytesWritable(sketch1.compact().toByteArray()))
      );

      UpdatableSketch<Double, DoubleSummary> sketch2 =
          new UpdatableSketchBuilder<>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).build();
      sketch2.update(1, 2.0);
      sketch2.update(2, 1.0);
      eval.merge(state, Arrays.asList(
        new IntWritable(DEFAULT_NOMINAL_ENTRIES),
        new Text("Sum"),
        new BytesWritable(sketch2.compact().toByteArray()))
      );

      Object result = eval.terminatePartial(state);
      Assert.assertNotNull(result);
      Assert.assertTrue(result instanceof List);
      List<?> r = (List<?>) result;
      Assert.assertEquals(r.size(), 3);
      Assert.assertEquals(((IntWritable) r.get(0)).get(), DEFAULT_NOMINAL_ENTRIES);
      Assert.assertEquals(((Text) r.get(1)).toString(), DoubleSummary.Mode.Sum.toString());
      Sketch<DoubleSummary> resultSketch = Sketches.heapifySketch(
          BytesWritableHelper.wrapAsMemory((BytesWritable) r.get(2)), new DoubleSummaryDeserializer());
      Assert.assertEquals(resultSketch.getEstimate(), 2.0);
      SketchIterator<DoubleSummary> it = resultSketch.iterator();
      while (it.next()) {
        Assert.assertEquals(it.getSummary().getValue(), 3.0);
      }

      eval.reset(state);
      result = eval.terminate(state);
      Assert.assertNull(result);
    }
  }

  // FINAL mode (Reduce phase in Map-Reduce): merge + terminate
  @Test
  public void finalMode() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector, doubleInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new DataToDoubleSummaryWithModeSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.FINAL, new ObjectInspector[] {structInspector});
      checkFinalResultInspector(resultInspector);

      @SuppressWarnings("unchecked")
      State<DoubleSummary> state = (State<DoubleSummary>) eval.getNewAggregationBuffer();

      UpdatableSketch<Double, DoubleSummary> sketch1 =
          new UpdatableSketchBuilder<>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).build();
      sketch1.update(1, 1.0);
      sketch1.update(2, 2.0);
      eval.merge(state, Arrays.asList(
        new IntWritable(DEFAULT_NOMINAL_ENTRIES),
        new Text("Sum"),
        new BytesWritable(sketch1.compact().toByteArray()))
      );

      UpdatableSketch<Double, DoubleSummary> sketch2 =
          new UpdatableSketchBuilder<>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).build();
      sketch2.update(1, 2.0);
      sketch2.update(2, 1.0);
      eval.merge(state, Arrays.asList(
        new IntWritable(DEFAULT_NOMINAL_ENTRIES),
        new Text("Sum"),
        new BytesWritable(sketch2.compact().toByteArray()))
      );

      Object result = eval.terminate(state);
      Assert.assertNotNull(result);
      Assert.assertTrue(result instanceof BytesWritable);
      Sketch<DoubleSummary> resultSketch = Sketches.heapifySketch(
          BytesWritableHelper.wrapAsMemory((BytesWritable) result), new DoubleSummaryDeserializer());
      Assert.assertEquals(resultSketch.getEstimate(), 2.0);
      SketchIterator<DoubleSummary> it = resultSketch.iterator();
      while (it.next()) {
        Assert.assertEquals(it.getSummary().getValue(), 3.0);
      }
    }
  }

  // COMPLETE mode (single mode, alternative to MapReduce): iterate + terminate
  @Test
  public void completeModeIntKeysDefaultParams() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector, doubleInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new DataToDoubleSummaryWithModeSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.COMPLETE, inspectors);
      checkFinalResultInspector(resultInspector);

      @SuppressWarnings("unchecked")
      State<DoubleSummary> state = (State<DoubleSummary>) eval.getNewAggregationBuffer();
      eval.iterate(state, new Object[] {new IntWritable(1), new DoubleWritable(1)});
      eval.iterate(state, new Object[] {new IntWritable(2), new DoubleWritable(2)});
      eval.iterate(state, new Object[] {new IntWritable(1), new DoubleWritable(2)});
      eval.iterate(state, new Object[] {new IntWritable(2), new DoubleWritable(1)});

      Object result = eval.terminate(state);
      Assert.assertNotNull(result);
      Assert.assertTrue(result instanceof BytesWritable);
      Sketch<DoubleSummary> resultSketch = Sketches.heapifySketch(
          BytesWritableHelper.wrapAsMemory((BytesWritable) result), new DoubleSummaryDeserializer());
      Assert.assertEquals(resultSketch.getEstimate(), 2.0);
      SketchIterator<DoubleSummary> it = resultSketch.iterator();
      while (it.next()) {
        Assert.assertEquals(it.getSummary().getValue(), 3.0);
      }

      eval.reset(state);
      result = eval.terminate(state);
      Assert.assertNull(result);
    }
  }

  // COMPLETE mode (single mode, alternative to MapReduce): iterate + terminate
  @Test
  public void completeModeDoubleKeysExplicitParams() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { doubleInspector, doubleInspector, intInspector, floatInspector, stringInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new DataToDoubleSummaryWithModeSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.COMPLETE, inspectors);
      checkFinalResultInspector(resultInspector);

      @SuppressWarnings("unchecked")
      State<DoubleSummary> state = (State<DoubleSummary>) eval.getNewAggregationBuffer();
      eval.iterate(state, new Object[] {new DoubleWritable(1), new DoubleWritable(1), new IntWritable(32), new FloatWritable(0.99f), new Text("Max")});
      eval.iterate(state, new Object[] {new DoubleWritable(2), new DoubleWritable(2), new IntWritable(32), new FloatWritable(0.99f), new Text("Max")});
      eval.iterate(state, new Object[] {new DoubleWritable(1), new DoubleWritable(2), new IntWritable(32), new FloatWritable(0.99f), new Text("Max")});
      eval.iterate(state, new Object[] {new DoubleWritable(2), new DoubleWritable(1), new IntWritable(32), new FloatWritable(0.99f), new Text("Max")});

      Object result = eval.terminate(state);
      Assert.assertNotNull(result);
      Assert.assertTrue(result instanceof BytesWritable);
      Sketch<DoubleSummary> resultSketch = Sketches.heapifySketch(
          BytesWritableHelper.wrapAsMemory((BytesWritable) result), new DoubleSummaryDeserializer());
      // because of sampling probability < 1
      Assert.assertTrue(resultSketch.isEstimationMode());
      Assert.assertEquals(resultSketch.getEstimate(), 2.0, 0.05);
      SketchIterator<DoubleSummary> it = resultSketch.iterator();
      while (it.next()) {
        Assert.assertEquals(it.getSummary().getValue(), 2.0);
      }

      eval.reset(state);
      result = eval.terminate(state);
      Assert.assertNull(result);
    }
  }

  @Test
  public void completeModeCheckTrimmingToNominal() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector, doubleInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new DataToDoubleSummaryWithModeSketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.COMPLETE, inspectors);
      checkFinalResultInspector(resultInspector);

      @SuppressWarnings("unchecked")
      State<DoubleSummary> state = (State<DoubleSummary>) eval.getNewAggregationBuffer();
      for (int i = 0; i < 10000; i++) {
        eval.iterate(state, new Object[] {new IntWritable(i), new DoubleWritable(1)});
      }

      Object result = eval.terminate(state);
      Assert.assertNotNull(result);
      Assert.assertTrue(result instanceof BytesWritable);
      Sketch<DoubleSummary> resultSketch = Sketches.heapifySketch(
          BytesWritableHelper.wrapAsMemory((BytesWritable) result), new DoubleSummaryDeserializer());
      Assert.assertEquals(resultSketch.getEstimate(), 10000.0, 10000 * 0.03);
      Assert.assertTrue(resultSketch.getRetainedEntries() <= 4096, "retained entries: " + resultSketch.getRetainedEntries());

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
