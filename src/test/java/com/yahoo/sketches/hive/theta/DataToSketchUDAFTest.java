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
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.hamcrest.core.IsInstanceOf;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.yahoo.sketches.theta.SetOpReturnState;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.theta.CompactSketch;
import com.yahoo.sketches.theta.UpdateReturnState;
import com.yahoo.sketches.hive.theta.DataToSketchUDAF.DataToSketchEvaluator;
import com.yahoo.sketches.hive.theta.DataToSketchUDAF.DataToSketchEvaluator.DataToSketchAggBuffer;
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
public class DataToSketchUDAFTest {
  private static PrimitiveTypeInfo binaryType;
  private static PrimitiveTypeInfo intType;
  private static PrimitiveTypeInfo doubleType;
  private static ObjectInspector intermediateStructType;
  private static ObjectInspector inputOI;
  private static ObjectInspector sketchSizeOI;
  private static ObjectInspector samplingOI;

  @BeforeClass
  public static void setupClass() {
    binaryType = new PrimitiveTypeInfo();
    binaryType.setTypeName("binary");
    intType = new PrimitiveTypeInfo();
    intType.setTypeName("int");
    doubleType = new PrimitiveTypeInfo();
    doubleType.setTypeName("double");

    inputOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.INT);
    sketchSizeOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(intType,
        new IntWritable(1024));
    samplingOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(doubleType,
        new DoubleWritable(1.0));

    List<ObjectInspector> fields = new ArrayList<>();
    fields.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.INT));
    fields.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.FLOAT));
    fields.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY));
    List<String> fieldNames = new ArrayList<>();
    fieldNames.add("sketchSize");
    fieldNames.add("samplingProbability");
    fieldNames.add("sketch");

    intermediateStructType = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fields);
  }

  @Test(expectedExceptions = { UDFArgumentException.class })
  public void testParametersTooFew() throws SemanticException {
    DataToSketchUDAF udf = new DataToSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(new ObjectInspector[] {}, false, false);

    udf.getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentException.class })
  public void testParametersTooMany() throws SemanticException {
    DataToSketchUDAF udf = new DataToSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(new ObjectInspector[] { inputOI, sketchSizeOI,
        samplingOI, samplingOI }, false, false);

    udf.getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void testParametersInvalidInputArgType() throws SemanticException {
    DataToSketchUDAF udf = new DataToSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(
        new ObjectInspector[] { ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory
            .getPrimitiveWritableObjectInspector(PrimitiveCategory.INT)) }, false, false);

    udf.getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void testParametersInvalidSizeArgType() throws SemanticException {
    DataToSketchUDAF udf = new DataToSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(
        new ObjectInspector[] {
            inputOI,
            PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(doubleType, new DoubleWritable(
                1.0)), samplingOI }, false, false);

    udf.getEvaluator(params);
  }

  @Test(expectedExceptions = { UDFArgumentTypeException.class })
  public void testParametersInvalidSamplingArgType() throws SemanticException {
    DataToSketchUDAF udf = new DataToSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(new ObjectInspector[] { inputOI, sketchSizeOI,
        PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(intType, new IntWritable(1)) },
        false, false);

    udf.getEvaluator(params);
  }

  @Test
  public void testGetEvaluator() throws SemanticException {
    DataToSketchUDAF udf = new DataToSketchUDAF();
    GenericUDAFParameterInfo params = new SimpleGenericUDAFParameterInfo(new ObjectInspector[] { inputOI }, false,
        false);

    GenericUDAFEvaluator eval = udf.getEvaluator(params);
    assertNotNull(eval);
    assertThat(eval, IsInstanceOf.instanceOf(DataToSketchEvaluator.class));
  }

  @Test(expectedExceptions = { SemanticException.class })
  public void testGetEvaluatorDeprecated() throws SemanticException {
    DataToSketchUDAF udf = new DataToSketchUDAF();
    GenericUDAFEvaluator eval = udf.getEvaluator(new TypeInfo[] { binaryType, intType, doubleType });
    assertNotNull(eval);
    assertThat(eval, IsInstanceOf.instanceOf(DataToSketchEvaluator.class));
  }

  @Test
  public void testInit() throws HiveException, IOException {
    try (DataToSketchEvaluator eval = new DataToSketchEvaluator()) {

      ObjectInspector[] initialParams = new ObjectInspector[] { inputOI, sketchSizeOI, samplingOI };

      ObjectInspector retType = eval.init(Mode.COMPLETE, initialParams);

      assertSame(eval.getInputOI(), inputOI);
      assertEquals(eval.getSketchSizeOI(), sketchSizeOI);
      assertEquals(eval.getSamplingProbOI(), samplingOI);
      assertEquals(eval.getIntermediateOI(), null);
      assertTrue(ObjectInspectorUtils.compareTypes(retType,
          PrimitiveObjectInspectorFactory.writableBinaryObjectInspector));

      retType = eval.init(Mode.PARTIAL1, initialParams);
      assertSame(eval.getInputOI(), inputOI);
      assertEquals(eval.getSketchSizeOI(), sketchSizeOI);
      assertEquals(eval.getSamplingProbOI(), samplingOI);
      assertEquals(eval.getIntermediateOI(), null);
      assertTrue(ObjectInspectorUtils.compareTypes(retType, intermediateStructType));

      retType = eval.init(Mode.PARTIAL2, new ObjectInspector[] { intermediateStructType });
      assertSame(eval.getIntermediateOI(), intermediateStructType);
      assertEquals(eval.getSketchSizeOI(), null);
      assertEquals(eval.getSamplingProbOI(), null);
      assertEquals(eval.getInputOI(), null);
      assertTrue(ObjectInspectorUtils.compareTypes(retType, intermediateStructType));

      retType = eval.init(Mode.FINAL, new ObjectInspector[] { intermediateStructType });
      assertSame(eval.getIntermediateOI(), intermediateStructType);
      assertEquals(eval.getSketchSizeOI(), null);
      assertEquals(eval.getSamplingProbOI(), null);
      assertEquals(eval.getInputOI(), null);
      assertTrue(ObjectInspectorUtils.compareTypes(retType,
          PrimitiveObjectInspectorFactory.writableBinaryObjectInspector));

    }
  }

  @Test
  public void testGetNewAggregationBuffer() throws HiveException, IOException {
    try (DataToSketchEvaluator eval = new DataToSketchEvaluator()) {
      AggregationBuffer buffer = eval.getNewAggregationBuffer();

      assertThat(buffer, IsInstanceOf.instanceOf(DataToSketchAggBuffer.class));
      DataToSketchAggBuffer sketchBuffer = (DataToSketchAggBuffer) buffer;
      assertNull(sketchBuffer.getUnion());
      assertNull(sketchBuffer.getUpdateSketch());
      assertEquals(sketchBuffer.getSketchSize(), DataToSketchUDAF.DEFAULT_SKETCH_SIZE);
      assertEquals(sketchBuffer.getSamplingProbability(), DataToSketchUDAF.DEFAULT_SAMPLING_PROBABILITY);
    }
    ;
  }

  @Test
  public void testReset() throws IOException, HiveException {

    Union union = mock(Union.class);
    UpdateSketch sketch = mock(UpdateSketch.class);

    replay(union, sketch);

    try (DataToSketchEvaluator eval = new DataToSketchEvaluator()) {
      DataToSketchAggBuffer buf = new DataToSketchAggBuffer();
      buf.setUnion(union);
      buf.setUpdateSketch(sketch);
      buf.setSamplingProbability(.5f);
      buf.setSketchSize(1024);

      eval.reset(buf);

      assertNull(buf.getUnion());
      assertNull(buf.getUpdateSketch());
      assertEquals(buf.getSketchSize(), DataToSketchUDAF.DEFAULT_SKETCH_SIZE);
      assertEquals(buf.getSamplingProbability(), DataToSketchUDAF.DEFAULT_SAMPLING_PROBABILITY);
    }

    verify(union, sketch);
  }

  @Test
  public void testIterate() throws IOException, HiveException {
    DataToSketchAggBuffer buf = strictMock(DataToSketchAggBuffer.class);
    UpdateSketch sketch = strictMock(UpdateSketch.class);
    
    expect(buf.getUpdateSketch()).andReturn(null);
    buf.setSketchSize(512);
    buf.setSamplingProbability(DataToSketchUDAF.DEFAULT_SAMPLING_PROBABILITY);
    buf.setUpdateSketch(isA(UpdateSketch.class)); 
    expect(buf.getUpdateSketch()).andReturn(sketch);
    expect(sketch.update(1234)).andReturn(UpdateReturnState.InsertedCountIncremented);
    
    expect(buf.getUpdateSketch()).andReturn(sketch).times(2);
    expect(sketch.update(234)).andReturn(UpdateReturnState.InsertedCountIncremented);
    
    replay(buf, sketch);
    
    try (DataToSketchEvaluator eval = new DataToSketchEvaluator()) {
      eval.init(Mode.COMPLETE, new ObjectInspector[] {inputOI, sketchSizeOI});
      
      eval.iterate(buf, new Object[] {new IntWritable(1234), new IntWritable(512)});
      eval.iterate(buf, new Object[] {new IntWritable(234), new IntWritable(512)});

    }
    
    verify(buf, sketch);
  }
  
  /**
   * testing terminatePartial when in Mode.PARTIAL1, which should merge
   * values generated by iterate.
   * 
   * @throws IOException
   * @throws HiveException
   */
  @Test
  public void testTerminatePartial1() throws IOException, HiveException {
    DataToSketchAggBuffer buf = mock(DataToSketchAggBuffer.class);
    UpdateSketch sketch = mock(UpdateSketch.class);
    CompactSketch compact = mock(CompactSketch.class);
    byte[] bytes = new byte[0];
    
    expect(buf.getUpdateSketch()).andReturn(sketch);
    expect(buf.getUnion()).andReturn(null);
    expect(buf.getSketchSize()).andReturn(512);
    expect(buf.getSamplingProbability()).andReturn(0.5f);
    
    expect(sketch.compact(true,null)).andReturn(compact);
    
    expect(compact.toByteArray()).andReturn(bytes);
    
    replay(buf, sketch, compact);
    
    try (DataToSketchEvaluator eval = new DataToSketchEvaluator()) {
      Object result = eval.terminatePartial(buf);
      
      assertThat(result, IsInstanceOf.instanceOf(ArrayList.class));
      ArrayList<?> r = (ArrayList<?>) result;
      assertEquals(r.size(), 3);
      assertEquals(((IntWritable)(r.get(0))).get(), 512);
      assertEquals(((FloatWritable)(r.get(1))).get(), 0.5f);
      assertEquals(((BytesWritable)(r.get(2))).getBytes(), bytes);
    }
    
    verify(buf, sketch, compact);
  }
  
  /**
   * testing terminatePartial when in Mode.PARTIAL2, which should merge
   * values generated by merge.
   * 
   * @throws IOException
   * @throws HiveException
   */
  @Test
  public void testTerminatePartial2() throws IOException, HiveException {
    DataToSketchAggBuffer buf = mock(DataToSketchAggBuffer.class);
    Union union = mock(Union.class);
    CompactSketch compact = mock(CompactSketch.class);
    byte[] bytes = new byte[0];
    
    expect(buf.getUpdateSketch()).andReturn(null);
    expect(buf.getUnion()).andReturn(union);
    expect(buf.getSketchSize()).andReturn(512);
    expect(buf.getSamplingProbability()).andReturn(0.5f);
    
    expect(union.getResult(true,null)).andReturn(compact);
    
    expect(compact.toByteArray()).andReturn(bytes);
    
    replay(buf, union, compact);
    
    try (DataToSketchEvaluator eval = new DataToSketchEvaluator()) {
      Object result = eval.terminatePartial(buf);
      
      assertThat(result, IsInstanceOf.instanceOf(ArrayList.class));
      ArrayList<?> r = (ArrayList<?>) result;
      assertEquals(r.size(), 3);
      assertEquals(((IntWritable)(r.get(0))).get(), 512);
      assertEquals(((FloatWritable)(r.get(1))).get(), 0.5f);
      assertEquals(((BytesWritable)(r.get(2))).getBytes(), bytes);
    }
    
    verify(buf, union, compact);
  }

  @Test
  public void testMerge () throws IOException, HiveException {
    DataToSketchAggBuffer buf = mock(DataToSketchAggBuffer.class);
    Union union = mock(Union.class);
    
    replay(buf, union);
    
    // test null partial
    try (DataToSketchEvaluator eval = new DataToSketchEvaluator()) {
      eval.init(Mode.PARTIAL2, new ObjectInspector[] {intermediateStructType});
      
      eval.merge(buf, null);
    }
    
    verify(buf, union);
    
    // test initial merge
    reset(buf, union);
    
    expect(buf.getUnion()).andReturn(null);
    buf.setSketchSize(1024);
    buf.setSamplingProbability(1.0f);
    expect(buf.getSamplingProbability()).andReturn(1.0f);
    expect(buf.getSketchSize()).andReturn(1024);
    buf.setUnion(isA(Union.class));
    expect(buf.getUnion()).andReturn(union);
    
    Capture<Memory> c = EasyMock.newCapture();
    expect(union.update(and(isA(Memory.class), capture(c)))).andReturn(SetOpReturnState.Success);
    
    replay(buf, union);
    
    try (DataToSketchEvaluator eval = new DataToSketchEvaluator()) {
      eval.init(Mode.PARTIAL2, new ObjectInspector[] {intermediateStructType});
      
      ArrayList<Object> struct = new ArrayList<>(3);
      struct.add(new IntWritable(1024));
      struct.add(new FloatWritable(1.0f));
      struct.add(new BytesWritable(new byte[0]));
      
      eval.merge(buf, struct);
    }
    
    verify(buf, union);
    assertEquals(0, c.getValue().getCapacity());
    
    // test subsequent merge
    reset(buf, union);
    
    expect(buf.getUnion()).andReturn(union).times(2);
    c = EasyMock.newCapture();
    expect(union.update(and(isA(Memory.class), capture(c)))).andReturn(SetOpReturnState.Success);
    
    replay(buf, union);
    
    try (DataToSketchEvaluator eval = new DataToSketchEvaluator()) {
      eval.init(Mode.PARTIAL2, new ObjectInspector[] {intermediateStructType});
      
      ArrayList<Object> struct = new ArrayList<>(3);
      struct.add(new IntWritable(1024));
      struct.add(new FloatWritable(1.0f));
      struct.add(new BytesWritable(new byte[0]));
      
      eval.merge(buf, struct);
    }

    verify(buf, union);
    
    assertEquals(0, c.getValue().getCapacity());
  }
  
  @Test
  public void testTerminate () throws IOException, HiveException {
    DataToSketchAggBuffer buf = mock(DataToSketchAggBuffer.class);
    Union union = mock(Union.class);
    UpdateSketch sketch = mock(UpdateSketch.class);
    CompactSketch compact = mock(CompactSketch.class);
    byte[] bytes = new byte[0];
    
    // mode = complete. initial update sketches but no unions
    
    expect(buf.getUnion()).andReturn(null);
    expect(buf.getUpdateSketch()).andReturn(sketch).times(2);
    expect(sketch.compact(true,null)).andReturn(compact);
    expect(compact.getRetainedEntries(false)).andReturn(1024);
    expect(compact.toByteArray()).andReturn(bytes);
    replay(buf, union, sketch, compact);
    
    try (DataToSketchEvaluator eval = new DataToSketchEvaluator()) {
      Object retVal = eval.terminate(buf);
      
      assertThat(retVal, IsInstanceOf.instanceOf(BytesWritable.class));
      BytesWritable retValBytes = (BytesWritable) retVal;
      assertEquals(retValBytes.getBytes(), bytes);
    }
    
    verify(buf, union, sketch, compact);
    
    // mode = final. merged unions, but no update sketches
    reset(buf, union, sketch, compact);

    expect(buf.getUnion()).andReturn(union).times(2);
    expect(union.getResult(true,null)).andReturn(compact);
    expect(compact.getRetainedEntries(false)).andReturn(1024);
    expect(compact.toByteArray()).andReturn(bytes);

    replay(buf, union, sketch, compact);
    try (DataToSketchEvaluator eval = new DataToSketchEvaluator()) {
      Object retVal = eval.terminate(buf);
      
      assertThat(retVal, IsInstanceOf.instanceOf(BytesWritable.class));
      BytesWritable retValBytes = (BytesWritable) retVal;
      assertEquals(retValBytes.getBytes(), bytes);
    }
    
    verify(buf, union, sketch, compact);
  }
  
}
