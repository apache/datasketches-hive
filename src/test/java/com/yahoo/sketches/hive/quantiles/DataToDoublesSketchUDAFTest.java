package com.yahoo.sketches.hive.quantiles;

import java.util.Arrays;

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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.UpdateDoublesSketch;

public class DataToDoublesSketchUDAFTest {

  static final ObjectInspector doubleInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.DOUBLE);

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
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    new DataToDoublesSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentException.class)
  public void getEvaluatorTooManyInspectors() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { doubleInspector, intInspector, intInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    new DataToDoublesSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentException.class)
  public void getEvaluatorWrongCategoryArg1() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { structInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    new DataToDoublesSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentException.class)
  public void getEvaluatorWrongTypeArg1() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    new DataToDoublesSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentException.class)
  public void getEvaluatorWrongCategoryArg2() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { doubleInspector, structInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    new DataToDoublesSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentException.class)
  public void getEvaluatorWrongTypeArg2() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { doubleInspector, doubleInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    new DataToDoublesSketchUDAF().getEvaluator(info);
  }

  // PARTIAL1 mode (Map phase in Map-Reduce): iterate + terminatePartial
  @Test
  public void partial1ModeDefaultK() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { doubleInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    GenericUDAFEvaluator eval = new DataToDoublesSketchUDAF().getEvaluator(info);
    ObjectInspector resultInspector = eval.init(Mode.PARTIAL1, inspectors);
    checkResultInspector(resultInspector);

    DoublesUnionState state = (DoublesUnionState) eval.getNewAggregationBuffer();
    eval.iterate(state, new Object[] { new DoubleWritable(1.0) });
    eval.iterate(state, new Object[] { new DoubleWritable(2.0) });

    BytesWritable bytes = (BytesWritable) eval.terminatePartial(state);
    DoublesSketch resultSketch = DoublesSketch.wrap(Memory.wrap(bytes.getBytes()));
    Assert.assertEquals(resultSketch.getK(), 128);
    Assert.assertEquals(resultSketch.getRetainedItems(), 2);
    Assert.assertEquals(resultSketch.getMinValue(), 1.0);
    Assert.assertEquals(resultSketch.getMaxValue(), 2.0);
    eval.close();
  }

  @Test
  public void partial1ModeGivenK() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { doubleInspector, intInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    GenericUDAFEvaluator eval = new DataToDoublesSketchUDAF().getEvaluator(info);
    ObjectInspector resultInspector = eval.init(Mode.PARTIAL1, inspectors);
    checkResultInspector(resultInspector);

    DoublesUnionState state = (DoublesUnionState) eval.getNewAggregationBuffer();
    eval.iterate(state, new Object[] { new DoubleWritable(1.0), new IntWritable(256) });
    eval.iterate(state, new Object[] { new DoubleWritable(2.0), new IntWritable(256) });

    BytesWritable bytes = (BytesWritable) eval.terminatePartial(state);
    DoublesSketch resultSketch = DoublesSketch.wrap(Memory.wrap(bytes.getBytes()));
    Assert.assertEquals(resultSketch.getK(), 256);
    Assert.assertEquals(resultSketch.getRetainedItems(), 2);
    Assert.assertEquals(resultSketch.getMinValue(), 1.0);
    Assert.assertEquals(resultSketch.getMaxValue(), 2.0);
    eval.close();
  }

  // PARTIAL2 mode (Combine phase in Map-Reduce): merge + terminatePartial
  @Test
  public void partial2Mode() throws Exception {
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(new ObjectInspector[] { doubleInspector }, false, false);
    GenericUDAFEvaluator eval = new DataToDoublesSketchUDAF().getEvaluator(info);
    ObjectInspector resultInspector = eval.init(Mode.PARTIAL2, new ObjectInspector[] {binaryInspector});
    checkResultInspector(resultInspector);

    DoublesUnionState state = (DoublesUnionState) eval.getNewAggregationBuffer();

    UpdateDoublesSketch sketch1 = DoublesSketch.builder().build();
    sketch1.update(1.0);
    eval.merge(state, new BytesWritable(sketch1.toByteArray()));

    UpdateDoublesSketch sketch2 = DoublesSketch.builder().build();
    sketch2.update(2.0);
    eval.merge(state, new BytesWritable(sketch2.toByteArray()));

    BytesWritable bytes = (BytesWritable) eval.terminate(state);
    DoublesSketch resultSketch = DoublesSketch.wrap(Memory.wrap(bytes.getBytes()));
    Assert.assertEquals(resultSketch.getRetainedItems(), 2);
    Assert.assertEquals(resultSketch.getMinValue(), 1.0);
    Assert.assertEquals(resultSketch.getMaxValue(), 2.0);
    eval.close();
  }

  // FINAL mode (Reduce phase in Map-Reduce): merge + terminate
  @Test
  public void finalMode() throws Exception {
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(new ObjectInspector[] { doubleInspector }, false, false);
    GenericUDAFEvaluator eval = new DataToDoublesSketchUDAF().getEvaluator(info);
    ObjectInspector resultInspector = eval.init(Mode.FINAL, new ObjectInspector[] {binaryInspector});
    checkResultInspector(resultInspector);

    DoublesUnionState state = (DoublesUnionState) eval.getNewAggregationBuffer();

    UpdateDoublesSketch sketch1 = DoublesSketch.builder().setK(256).build();
    sketch1.update(1.0);
    eval.merge(state, new BytesWritable(sketch1.toByteArray()));

    UpdateDoublesSketch sketch2 = DoublesSketch.builder().setK(256).build();
    sketch2.update(2.0);
    eval.merge(state, new BytesWritable(sketch2.toByteArray()));

    BytesWritable bytes = (BytesWritable) eval.terminate(state);
    DoublesSketch resultSketch = DoublesSketch.wrap(Memory.wrap(bytes.getBytes()));
    Assert.assertEquals(resultSketch.getK(), 256);
    Assert.assertEquals(resultSketch.getRetainedItems(), 2);
    Assert.assertEquals(resultSketch.getMinValue(), 1.0);
    Assert.assertEquals(resultSketch.getMaxValue(), 2.0);
    eval.close();
  }

  // COMPLETE mode (single mode, alternative to MapReduce): iterate + terminate
  @Test
  public void completeModeDefaultK() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { doubleInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    GenericUDAFEvaluator eval = new DataToDoublesSketchUDAF().getEvaluator(info);
    ObjectInspector resultInspector = eval.init(Mode.COMPLETE, inspectors);
    checkResultInspector(resultInspector);

    DoublesUnionState state = (DoublesUnionState) eval.getNewAggregationBuffer();
    eval.iterate(state, new Object[] { new DoubleWritable(1.0) });
    eval.iterate(state, new Object[] { new DoubleWritable(2.0) });

    BytesWritable bytes = (BytesWritable) eval.terminate(state);
    DoublesSketch resultSketch = DoublesSketch.wrap(Memory.wrap(bytes.getBytes()));
    Assert.assertEquals(resultSketch.getK(), 128);
    Assert.assertEquals(resultSketch.getRetainedItems(), 2);
    Assert.assertEquals(resultSketch.getMinValue(), 1.0);
    Assert.assertEquals(resultSketch.getMaxValue(), 2.0);
    eval.close();
  }

  @Test
  public void completeModeGivenK() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { doubleInspector, intInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    GenericUDAFEvaluator eval = new DataToDoublesSketchUDAF().getEvaluator(info);
    ObjectInspector resultInspector = eval.init(Mode.COMPLETE, inspectors);
    checkResultInspector(resultInspector);

    DoublesUnionState state = (DoublesUnionState) eval.getNewAggregationBuffer();
    eval.iterate(state, new Object[] { new DoubleWritable(1.0), new IntWritable(256) });
    eval.iterate(state, new Object[] { new DoubleWritable(2.0), new IntWritable(256) });

    BytesWritable bytes = (BytesWritable) eval.terminate(state);
    DoublesSketch resultSketch = DoublesSketch.wrap(Memory.wrap(bytes.getBytes()));
    Assert.assertEquals(resultSketch.getK(), 256);
    Assert.assertEquals(resultSketch.getRetainedItems(), 2);
    Assert.assertEquals(resultSketch.getMinValue(), 1.0);
    Assert.assertEquals(resultSketch.getMaxValue(), 2.0);
    eval.close();
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
