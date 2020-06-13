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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

@SuppressWarnings({"javadoc","resource"})
public class UnionDoubleSummarySketchUDAFTest {

  private static final ObjectInspector intInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.INT);

  private static final ObjectInspector floatInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.FLOAT);

  private static final ObjectInspector binaryInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY);

  private static final ObjectInspector structInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
      Arrays.asList("nominalEntries", "sketch"),
      Arrays.asList(intInspector, binaryInspector)
    );

  @Test(expectedExceptions = { UDFArgumentException.class })
  public void tooFewArguments() throws SemanticException {
    ObjectInspector[] inspectors = new ObjectInspector[] { };
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new UnionDoubleSummarySketchUDAF().getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentException.class })
  public void tooManyArguments() throws SemanticException {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector, intInspector, intInspector };
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new UnionDoubleSummarySketchUDAF().getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void invalidCategoryArg1() throws SemanticException {
    ObjectInspector[] inspectors = new ObjectInspector[] { structInspector };
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new UnionDoubleSummarySketchUDAF().getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void invalidTypeArg1() throws SemanticException {
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector };
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new UnionDoubleSummarySketchUDAF().getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void invalidCategoryArg2() throws SemanticException {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector, structInspector };
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new UnionDoubleSummarySketchUDAF().getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void invalidTypeArg2() throws SemanticException {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector, floatInspector };
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    new UnionDoubleSummarySketchUDAF().getEvaluator(params);
  }

  // PARTIAL1 mode (Map phase in Map-Reduce): iterate + terminatePartial
  @Test
  public void partial1ModeDefaultParams() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new UnionDoubleSummarySketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.PARTIAL1, inspectors);
      DataToDoubleSummarySketchUDAFTest.checkIntermediateResultInspector(resultInspector);

      @SuppressWarnings("unchecked")
      State<DoubleSummary> state = (State<DoubleSummary>) eval.getNewAggregationBuffer();

      UpdatableSketch<Double, DoubleSummary> sketch1 = new UpdatableSketchBuilder<>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).build();
      sketch1.update(1, 1.0);
      eval.iterate(state, new Object[] {new BytesWritable(sketch1.compact().toByteArray())});

      UpdatableSketch<Double, DoubleSummary> sketch2 = new UpdatableSketchBuilder<>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).build();
      sketch2.update(2, 1.0);
      eval.iterate(state, new Object[] {new BytesWritable(sketch2.compact().toByteArray())});

      Object result = eval.terminatePartial(state);
      Assert.assertNotNull(result);
      Assert.assertTrue(result instanceof List);
      List<?> r = (List<?>) result;
      Assert.assertEquals(r.size(), 2);
      Assert.assertEquals(((IntWritable) r.get(0)).get(), DEFAULT_NOMINAL_ENTRIES);
      Sketch<DoubleSummary> resultSketch = Sketches.heapifySketch(
          BytesWritableHelper.wrapAsMemory((BytesWritable) r.get(1)), new DoubleSummaryDeserializer());
      Assert.assertEquals(resultSketch.getEstimate(), 2.0);
    }
  }

  @Test
  public void partial1ModeExplicitParams() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector, intInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new UnionDoubleSummarySketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.PARTIAL1, inspectors);
      DataToDoubleSummarySketchUDAFTest.checkIntermediateResultInspector(resultInspector);

      final int nomNumEntries = 16;
      @SuppressWarnings("unchecked")
      State<DoubleSummary> state = (State<DoubleSummary>) eval.getNewAggregationBuffer();

      UpdatableSketch<Double, DoubleSummary> sketch1 =
          new UpdatableSketchBuilder<>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum))
            .setNominalEntries(nomNumEntries).build();
      sketch1.update(1, 1.0);
      eval.iterate(state, new Object[] {new BytesWritable(sketch1.compact().toByteArray()),
          new IntWritable(nomNumEntries)});

      UpdatableSketch<Double, DoubleSummary> sketch2 =
          new UpdatableSketchBuilder<>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum))
            .setNominalEntries(nomNumEntries).build();
      sketch2.update(2, 1.0);
      eval.iterate(state, new Object[] {new BytesWritable(sketch2.compact().toByteArray()),
          new IntWritable(nomNumEntries)});

      Object result = eval.terminatePartial(state);
      Assert.assertNotNull(result);
      Assert.assertTrue(result instanceof List);
      List<?> r = (List<?>) result;
      Assert.assertEquals(r.size(), 2);
      Assert.assertEquals(((IntWritable) r.get(0)).get(), nomNumEntries);
      Sketch<DoubleSummary> resultSketch = Sketches.heapifySketch(
          BytesWritableHelper.wrapAsMemory((BytesWritable) r.get(1)), new DoubleSummaryDeserializer());
      Assert.assertEquals(resultSketch.getEstimate(), 2.0);
    }
  }

  // PARTIAL2 mode (Combine phase in Map-Reduce): merge + terminatePartial
  @Test
  public void partial2Mode() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new UnionDoubleSummarySketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.PARTIAL2, new ObjectInspector[] {structInspector});
      DataToDoubleSummarySketchUDAFTest.checkIntermediateResultInspector(resultInspector);

      @SuppressWarnings("unchecked")
      State<DoubleSummary> state = (State<DoubleSummary>) eval.getNewAggregationBuffer();

      UpdatableSketch<Double, DoubleSummary> sketch1 =
          new UpdatableSketchBuilder<>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).build();
      sketch1.update(1, 1.0);
      eval.merge(state, Arrays.asList(
        new IntWritable(DEFAULT_NOMINAL_ENTRIES),
        new BytesWritable(sketch1.compact().toByteArray()))
      );

      UpdatableSketch<Double, DoubleSummary> sketch2 =
          new UpdatableSketchBuilder<>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).build();
      sketch2.update(2, 1.0);
      eval.merge(state, Arrays.asList(
        new IntWritable(DEFAULT_NOMINAL_ENTRIES),
        new BytesWritable(sketch2.compact().toByteArray()))
      );

      Object result = eval.terminatePartial(state);
      Assert.assertNotNull(result);
      Assert.assertTrue(result instanceof List);
      List<?> r = (List<?>) result;
      Assert.assertEquals(r.size(), 2);
      Assert.assertEquals(((IntWritable) r.get(0)).get(), DEFAULT_NOMINAL_ENTRIES);
      Sketch<DoubleSummary> resultSketch = Sketches.heapifySketch(
          BytesWritableHelper.wrapAsMemory((BytesWritable) r.get(1)), new DoubleSummaryDeserializer());
      Assert.assertEquals(resultSketch.getEstimate(), 2.0);

      eval.reset(state);
      result = eval.terminate(state);
      Assert.assertNull(result);
    }
  }

  // FINAL mode (Reduce phase in Map-Reduce): merge + terminate
  @Test
  public void finalMode() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new UnionDoubleSummarySketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.FINAL, new ObjectInspector[] {structInspector});
      DataToDoubleSummarySketchUDAFTest.checkFinalResultInspector(resultInspector);

      @SuppressWarnings("unchecked")
      State<DoubleSummary> state = (State<DoubleSummary>) eval.getNewAggregationBuffer();

      UpdatableSketch<Double, DoubleSummary> sketch1 =
          new UpdatableSketchBuilder<>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).build();
      sketch1.update(1, 1.0);
      eval.merge(state, Arrays.asList(
        new IntWritable(DEFAULT_NOMINAL_ENTRIES),
        new BytesWritable(sketch1.compact().toByteArray()))
      );

      UpdatableSketch<Double, DoubleSummary> sketch2 =
          new UpdatableSketchBuilder<>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).build();
      sketch2.update(2, 1.0);
      eval.merge(state, Arrays.asList(
        new IntWritable(DEFAULT_NOMINAL_ENTRIES),
        new BytesWritable(sketch2.compact().toByteArray()))
      );

      Object result = eval.terminate(state);
      Assert.assertNotNull(result);
      Assert.assertTrue(result instanceof BytesWritable);
      Sketch<DoubleSummary> resultSketch = Sketches.heapifySketch(
          BytesWritableHelper.wrapAsMemory((BytesWritable) result), new DoubleSummaryDeserializer());
      Assert.assertEquals(resultSketch.getEstimate(), 2.0);
    }
  }

  // COMPLETE mode (single mode, alternative to MapReduce): iterate + terminate
  @Test
  public void completeModeDefaultParams() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new UnionDoubleSummarySketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.COMPLETE, inspectors);
      DataToDoubleSummarySketchUDAFTest.checkFinalResultInspector(resultInspector);

      @SuppressWarnings("unchecked")
      State<DoubleSummary> state = (State<DoubleSummary>) eval.getNewAggregationBuffer();

      UpdatableSketch<Double, DoubleSummary> sketch1 =
          new UpdatableSketchBuilder<>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).build();
      sketch1.update(1, 1.0);
      eval.iterate(state, new Object[] {new BytesWritable(sketch1.compact().toByteArray())});

      UpdatableSketch<Double, DoubleSummary> sketch2 =
          new UpdatableSketchBuilder<>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum)).build();
      sketch2.update(2, 1.0);
      eval.iterate(state, new Object[] {new BytesWritable(sketch2.compact().toByteArray())});

      Object result = eval.terminate(state);
      Assert.assertNotNull(result);
      Assert.assertTrue(result instanceof BytesWritable);
      Sketch<DoubleSummary> resultSketch = Sketches.heapifySketch(
          BytesWritableHelper.wrapAsMemory((BytesWritable) result), new DoubleSummaryDeserializer());
      Assert.assertEquals(resultSketch.getEstimate(), 2.0);

      eval.reset(state);
      result = eval.terminate(state);
      Assert.assertNull(result);
    }
  }

  @Test
  public void completeModeExplicitParams() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector, intInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false, false);
    try (GenericUDAFEvaluator eval = new UnionDoubleSummarySketchUDAF().getEvaluator(info)) {
      ObjectInspector resultInspector = eval.init(Mode.COMPLETE, inspectors);
      DataToDoubleSummarySketchUDAFTest.checkFinalResultInspector(resultInspector);

      final int nomNumEntries = 16;
      @SuppressWarnings("unchecked")
      State<DoubleSummary> state = (State<DoubleSummary>) eval.getNewAggregationBuffer();

      UpdatableSketch<Double, DoubleSummary> sketch1 =
          new UpdatableSketchBuilder<>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum))
            .setNominalEntries(nomNumEntries).build();
      sketch1.update(1, 1.0);
      eval.iterate(state, new Object[] {new BytesWritable(sketch1.compact().toByteArray()),
          new IntWritable(nomNumEntries)});

      UpdatableSketch<Double, DoubleSummary> sketch2 =
          new UpdatableSketchBuilder<>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum))
            .setNominalEntries(nomNumEntries).build();
      sketch2.update(2, 1.0);
      eval.iterate(state, new Object[] {new BytesWritable(sketch2.compact().toByteArray()),
          new IntWritable(nomNumEntries)});

      Object result = eval.terminate(state);
      Assert.assertNotNull(result);
      Assert.assertTrue(result instanceof BytesWritable);
      Sketch<DoubleSummary> resultSketch = Sketches.heapifySketch(
          BytesWritableHelper.wrapAsMemory((BytesWritable) result), new DoubleSummaryDeserializer());
      Assert.assertEquals(resultSketch.getEstimate(), 2.0, 0.05);

      eval.reset(state);
      result = eval.terminate(state);
      Assert.assertNull(result);
    }
  }

}
