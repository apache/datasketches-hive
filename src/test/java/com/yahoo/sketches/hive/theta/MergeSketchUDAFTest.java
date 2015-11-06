/*******************************************************************************
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 *******************************************************************************/
package com.yahoo.sketches.hive.theta;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.hamcrest.core.IsInstanceOf;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.yahoo.sketches.hive.theta.MergeSketchUDAF.MergeSketchUDAFEvaluator;
import com.yahoo.sketches.hive.theta.MergeSketchUDAF.MergeSketchUDAFEvaluator.MergeSketchAggBuffer;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.theta.CompactSketch;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Union;
import com.yahoo.sketches.theta.UpdateSketch;

import static org.easymock.EasyMock.and;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.strictMock;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for DataToSketch UDF
 */
@SuppressWarnings("deprecation")
public class MergeSketchUDAFTest {
  private static PrimitiveTypeInfo binaryType;
  private static PrimitiveTypeInfo intType;
  private static PrimitiveTypeInfo doubleType;
  private static ObjectInspector intermediateStructType;
  private static ObjectInspector inputOI;
  private static ObjectInspector sketchSizeOI;

  @BeforeClass
  public static void setupClass() {
    binaryType = new PrimitiveTypeInfo();
    binaryType.setTypeName("binary");
    intType = new PrimitiveTypeInfo();
    intType.setTypeName("int");
    doubleType = new PrimitiveTypeInfo();
    doubleType.setTypeName("double");

    inputOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY);
    sketchSizeOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(intType,
        new IntWritable(1024));

    List<ObjectInspector> fields = new ArrayList<>();
    fields.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.INT));
    fields.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY));
    List<String> fieldNames = new ArrayList<>();
    fieldNames.add("sketchSize");
    fieldNames.add("sketch");

    intermediateStructType = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fields);
  }

  @Test(expectedExceptions = { UDFArgumentException.class })
  public void testParametersTooFew() throws SemanticException {
    MergeSketchUDAF udf = new MergeSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(new ObjectInspector[] {}, false, false);

    udf.getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentException.class })
  public void testParametersTooMany() throws SemanticException {
    MergeSketchUDAF udf = new MergeSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(new ObjectInspector[] { inputOI, sketchSizeOI,
        sketchSizeOI }, false, false);

    udf.getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void testParametersInvalidInputArgType() throws SemanticException {
    MergeSketchUDAF udf = new MergeSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(
        new ObjectInspector[] { ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory
            .getPrimitiveWritableObjectInspector(PrimitiveCategory.INT)) }, false, false);

    udf.getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void testParametersInvalidSizeArgType() throws SemanticException {
    MergeSketchUDAF udf = new MergeSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(
        new ObjectInspector[] {
            inputOI,
            PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(doubleType, new DoubleWritable(
                1.0)) }, false, false);

    udf.getEvaluator(params);
  }

  @Test
  public void testGetEvaluator() throws SemanticException, IOException {
    MergeSketchUDAF udf = new MergeSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(new ObjectInspector[] { inputOI }, false,
        false);

    try (GenericUDAFEvaluator eval = udf.getEvaluator(params)) {
      assertNotNull(eval);
      assertThat(eval, IsInstanceOf.instanceOf(MergeSketchUDAFEvaluator.class));
    }
  }

  @Test(expectedExceptions = { SemanticException.class })
  public void testGetEvaluatorDeprecated() throws SemanticException, IOException {
    MergeSketchUDAF udf = new MergeSketchUDAF();
    try (GenericUDAFEvaluator eval = udf.getEvaluator(new TypeInfo[] { binaryType, intType })) {
      assertNotNull(eval);
      assertThat(eval, IsInstanceOf.instanceOf(MergeSketchUDAFEvaluator.class));
    }
  }

  @Test
  public void testInit() throws HiveException, IOException {
    try (MergeSketchUDAFEvaluator eval = new MergeSketchUDAFEvaluator()) {

      ObjectInspector[] initialParams = new ObjectInspector[] { inputOI, sketchSizeOI };

      ObjectInspector retType = eval.init(Mode.COMPLETE, initialParams);

      assertSame(eval.getInputOI(), inputOI);
      assertEquals(eval.getSketchSizeOI(), sketchSizeOI);
      assertEquals(eval.getIntermediateOI(), null);
      assertTrue(ObjectInspectorUtils.compareTypes(retType,
          PrimitiveObjectInspectorFactory.writableBinaryObjectInspector));

      retType = eval.init(Mode.PARTIAL1, initialParams);
      assertSame(eval.getInputOI(), inputOI);
      assertEquals(eval.getSketchSizeOI(), sketchSizeOI);
      assertEquals(eval.getIntermediateOI(), null);
      assertTrue(ObjectInspectorUtils.compareTypes(retType, intermediateStructType));

      retType = eval.init(Mode.PARTIAL2, new ObjectInspector[] { intermediateStructType });
      assertSame(eval.getIntermediateOI(), intermediateStructType);
      assertEquals(eval.getSketchSizeOI(), null);
      assertEquals(eval.getInputOI(), null);
      assertTrue(ObjectInspectorUtils.compareTypes(retType, intermediateStructType));

      retType = eval.init(Mode.FINAL, new ObjectInspector[] { intermediateStructType });
      assertSame(eval.getIntermediateOI(), intermediateStructType);
      assertEquals(eval.getSketchSizeOI(), null);
      assertEquals(eval.getInputOI(), null);
      assertTrue(ObjectInspectorUtils.compareTypes(retType,
          PrimitiveObjectInspectorFactory.writableBinaryObjectInspector));

    }
  }

  @Test
  public void testGetNewAggregationBuffer() throws HiveException, IOException {
    try (MergeSketchUDAFEvaluator eval = new MergeSketchUDAFEvaluator()) {
      AggregationBuffer buffer = eval.getNewAggregationBuffer();

      assertThat(buffer, IsInstanceOf.instanceOf(MergeSketchAggBuffer.class));
      MergeSketchAggBuffer sketchBuffer = (MergeSketchAggBuffer) buffer;
      assertNull(sketchBuffer.getUnion());
      assertEquals(sketchBuffer.getSketchSize(), MergeSketchUDAF.DEFAULT_SKETCH_SIZE);
    }
  }

  @Test
  public void testReset() throws IOException, HiveException {

    Union union = mock(Union.class);
    UpdateSketch sketch = mock(UpdateSketch.class);

    replay(union, sketch);

    try (MergeSketchUDAFEvaluator eval = new MergeSketchUDAFEvaluator()) {
      MergeSketchAggBuffer buf = new MergeSketchAggBuffer();
      buf.setUnion(union);
      buf.setSketchSize(1024);

      eval.reset(buf);

      assertNull(buf.getUnion());
      assertEquals(buf.getSketchSize(), MergeSketchUDAF.DEFAULT_SKETCH_SIZE);
    }

    verify(union, sketch);
  }

  @Test
  public void testIterate() throws IOException, HiveException {
    MergeSketchAggBuffer buf = strictMock(MergeSketchAggBuffer.class);
    Union union = strictMock(Union.class);

    byte[] sketchBytes = SetOperation.builder().buildUnion(512).getResult(true, null).toByteArray();

    expect(buf.getUnion()).andReturn(null);
    buf.setSketchSize(512);
    buf.setUnion(isA(Union.class));
    expect(buf.getUnion()).andReturn(union);
    union.update(isA(Sketch.class));

    expect(buf.getUnion()).andReturn(union).times(2);
    union.update(isA(Sketch.class));

    replay(buf, union);

    try (MergeSketchUDAFEvaluator eval = new MergeSketchUDAFEvaluator()) {
      eval.init(Mode.COMPLETE, new ObjectInspector[] { inputOI, sketchSizeOI });

      eval.iterate(buf, new Object[] { new BytesWritable(sketchBytes), new IntWritable(512) });
      eval.iterate(buf, new Object[] { new BytesWritable(sketchBytes), new IntWritable(512) });

    }

    verify(buf, union);
  }

  /**
   * testing terminatePartial
   * 
   * @throws IOException
   * @throws HiveException
   */
  @Test
  public void testTerminatePartial() throws IOException, HiveException {
    MergeSketchAggBuffer buf = mock(MergeSketchAggBuffer.class);
    Union union = mock(Union.class);
    CompactSketch compact = mock(CompactSketch.class);
    byte[] bytes = new byte[0];

    expect(buf.getUnion()).andReturn(union);
    expect(union.getResult(true, null)).andReturn(compact);
    expect(compact.getRetainedEntries(false)).andReturn(512);
    expect(compact.toByteArray()).andReturn(bytes);
    expect(buf.getSketchSize()).andReturn(512);

    replay(buf, union, compact);

    try (MergeSketchUDAFEvaluator eval = new MergeSketchUDAFEvaluator()) {
      Object result = eval.terminatePartial(buf);

      assertThat(result, IsInstanceOf.instanceOf(ArrayList.class));
      ArrayList<?> r = (ArrayList<?>) result;
      assertEquals(r.size(), 2);
      assertEquals(((IntWritable) (r.get(0))).get(), 512);
      assertEquals(((BytesWritable) (r.get(1))).getBytes(), bytes);
    }

    verify(buf, union, compact);
  }

  @Test
  public void testMerge() throws IOException, HiveException {
    MergeSketchAggBuffer buf = mock(MergeSketchAggBuffer.class);
    Union union = mock(Union.class);

    replay(buf, union);

    // test null partial
    try (MergeSketchUDAFEvaluator eval = new MergeSketchUDAFEvaluator()) {
      eval.init(Mode.PARTIAL2, new ObjectInspector[] { intermediateStructType });

      eval.merge(buf, null);
    }

    verify(buf, union);

    // test initial merge
    reset(buf, union);

    expect(buf.getUnion()).andReturn(null);
    buf.setSketchSize(1024);
    buf.setUnion(isA(Union.class));
    expect(buf.getUnion()).andReturn(union);

    Capture<Memory> c = EasyMock.newCapture();
    union.update(and(isA(Memory.class), capture(c)));

    replay(buf, union);

    try (MergeSketchUDAFEvaluator eval = new MergeSketchUDAFEvaluator()) {
      eval.init(Mode.PARTIAL2, new ObjectInspector[] { intermediateStructType });

      ArrayList<Object> struct = new ArrayList<>(3);
      struct.add(new IntWritable(1024));
      struct.add(new BytesWritable(new byte[0]));

      eval.merge(buf, struct);
    }

    verify(buf, union);
    assertEquals(0, c.getValue().getCapacity());

    // test subsequent merge
    reset(buf, union);

    expect(buf.getUnion()).andReturn(union).times(2);
    c = EasyMock.newCapture();
    union.update(and(isA(Memory.class), capture(c)));

    replay(buf, union);

    try (MergeSketchUDAFEvaluator eval = new MergeSketchUDAFEvaluator()) {
      eval.init(Mode.PARTIAL2, new ObjectInspector[] { intermediateStructType });

      ArrayList<Object> struct = new ArrayList<>(3);
      struct.add(new IntWritable(1024));
      struct.add(new BytesWritable(new byte[0]));

      eval.merge(buf, struct);
    }

    verify(buf, union);

    assertEquals(0, c.getValue().getCapacity());
  }

  @Test
  public void testTerminate() throws IOException, HiveException {
    MergeSketchAggBuffer buf = mock(MergeSketchAggBuffer.class);
    Union union = mock(Union.class);
    CompactSketch compact = mock(CompactSketch.class);
    byte[] bytes = new byte[0];

    // mode = final. merged unions, but no update sketches
    expect(buf.getUnion()).andReturn(union);
    expect(union.getResult(true, null)).andReturn(compact);
    expect(compact.getRetainedEntries(false)).andReturn(1024);
    expect(compact.toByteArray()).andReturn(bytes);

    replay(buf, union, compact);
    try (MergeSketchUDAFEvaluator eval = new MergeSketchUDAFEvaluator()) {
      Object retVal = eval.terminate(buf);

      assertThat(retVal, IsInstanceOf.instanceOf(BytesWritable.class));
      BytesWritable retValBytes = (BytesWritable) retVal;
      assertEquals(retValBytes.getBytes(), bytes);
    }

    verify(buf, union, compact);
  }

}
