/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.tez;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import org.apache.hadoop.hive.llap.LlapUtil;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.exec.ObjectCache;
import org.apache.hadoop.hive.ql.exec.ghive.*;
import org.apache.hadoop.hive.ql.exec.vector.VectorFileSinkOperator;
import org.apache.hadoop.hive.ql.exec.vector.reducesink.VectorReduceSinkCommonOperator;
import org.apache.hadoop.hive.ql.plan.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper.ReportStats;
import org.apache.hadoop.hive.ql.exec.tez.DynamicValueRegistryTez.RegistryConfTez;
import org.apache.hadoop.hive.ql.exec.tez.TezProcessor.TezKVOutputCollector;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.tez.mapreduce.processor.MRTaskReporter;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.api.Reader;

import com.google.common.collect.Lists;

/**
 * Process input from tez LogicalInput and write output - for a map plan
 * Just pump the records through the query plan.
 */
public class ReduceRecordProcessor extends RecordProcessor {

  private static final String REDUCE_PLAN_KEY = "__REDUCE_PLAN__";

  private ObjectCache cache, dynamicValueCache;

  public static final Logger l4j = LoggerFactory.getLogger(ReduceRecordProcessor.class);

  private ReduceWork reduceWork;

  List<BaseWork> mergeWorkList = null;
  List<String> cacheKeys, dynamicValueCacheKeys;

  private final Map<Integer, DummyStoreOperator> connectOps =
          new TreeMap<Integer, DummyStoreOperator>();
  private final Map<Integer, ReduceWork> tagToReducerMap = new HashMap<Integer, ReduceWork>();

  private Operator<?> reducer;

  private ReduceRecordSource[] sources;

  private byte bigTablePosition = 0;

  public ReduceRecordProcessor(final JobConf jconf, final ProcessorContext context) throws Exception {
    super(jconf, context);

    String queryId = HiveConf.getVar(jconf, HiveConf.ConfVars.HIVEQUERYID);
    cache = ObjectCacheFactory.getCache(jconf, queryId, true);
    dynamicValueCache = ObjectCacheFactory.getCache(jconf, queryId, false, true);

    String cacheKey = processorContext.getTaskVertexName() + REDUCE_PLAN_KEY;
    cacheKeys = Lists.newArrayList(cacheKey);
    dynamicValueCacheKeys = new ArrayList<String>();
    reduceWork = Utilities.getReduceWork(jconf);
//    reduceWork = (ReduceWork) cache.retrieve(cacheKey, new Callable<Object>() {
//      @Override
//      public Object call() {
//        return Utilities.getReduceWork(jconf);
//      }
//    });

    Utilities.setReduceWork(jconf, reduceWork);
    mergeWorkList = getMergeWorkList(jconf, cacheKey, queryId, cache, cacheKeys);
  }

  @Override
  void init(
          MRTaskReporter mrReporter, Map<String, LogicalInput> inputs,
          Map<String, LogicalOutput> outputs) throws Exception {
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_INIT_OPERATORS);
    super.init(mrReporter, inputs, outputs);

    MapredContext.init(false, new JobConf(jconf));
    List<LogicalInput> shuffleInputs = getShuffleInputs(inputs);
    // TODO HIVE-14042. Move to using a loop and a timed wait once TEZ-3302 is fixed.
    checkAbortCondition();
    if (shuffleInputs != null) {
      l4j.info("Waiting for ShuffleInputs to become ready");
      processorContext.waitForAllInputsReady(new ArrayList<Input>(shuffleInputs));
    }

    connectOps.clear();
    ReduceWork redWork = reduceWork;
    l4j.info("Main work is " + reduceWork.getName());
    List<HashTableDummyOperator> workOps = reduceWork.getDummyOps();
    HashSet<HashTableDummyOperator> dummyOps = workOps == null ? null : new HashSet<>(workOps);
    tagToReducerMap.put(redWork.getTag(), redWork);
    if (mergeWorkList != null) {
      for (BaseWork mergeWork : mergeWorkList) {
        if (l4j.isDebugEnabled()) {
          l4j.debug("Additional work " + mergeWork.getName());
        }
        workOps = mergeWork.getDummyOps();
        if (workOps != null) {
          if (dummyOps == null) {
            dummyOps = new HashSet<>(workOps);
          } else {
            dummyOps.addAll(workOps);
          }
        }
        ReduceWork mergeReduceWork = (ReduceWork) mergeWork;
        reducer = mergeReduceWork.getReducer();
        // Check immediately after reducer is assigned, in cae the abort came in during
        checkAbortCondition();
        DummyStoreOperator dummyStoreOp = getJoinParentOp(reducer);
        connectOps.put(mergeReduceWork.getTag(), dummyStoreOp);
        tagToReducerMap.put(mergeReduceWork.getTag(), mergeReduceWork);
      }

      ((TezContext) MapredContext.get()).setDummyOpsMap(connectOps);
    }
    checkAbortCondition();

    bigTablePosition = (byte) reduceWork.getTag();

    ObjectInspector[] mainWorkOIs = null;
    ((TezContext) MapredContext.get()).setInputs(inputs);
    ((TezContext) MapredContext.get()).setTezProcessorContext(processorContext);
    int numTags = reduceWork.getTagToValueDesc().size();
    reducer = reduceWork.getReducer();

    /**
     * initilize dataflows coming to ReduceRecordProcessor.
     */
    if (InfoCollector.isGPU) {
      InfoCollector.recordProcessorDataFlows = new DataFlow[numTags];
      InfoCollector.recordProcessorInput = new GTable[numTags];
      for (int i = 0; i < numTags; i++) {
        InfoCollector.recordProcessorDataFlows[i] = new DataFlow(redWork.getTagToInput().get(i));
        InfoCollector.getAllDataFlows().add(InfoCollector.recordProcessorDataFlows[i]);
        InfoCollector.recordProcessorInput[i] = new GTable(redWork.getTagToInput().get(i));
        InfoCollector.allInputs.add(InfoCollector.recordProcessorInput[i]);
      }
    }

    // Check immediately after reducer is assigned, in cae the abort came in during
    checkAbortCondition();
    // set memory available for operators
    long memoryAvailableToTask = processorContext.getTotalMemoryAvailableToTask();
    if (reducer.getConf() != null) {
      reducer.getConf().setMaxMemoryAvailable(memoryAvailableToTask);
      l4j.info("Memory available for operators set to {}", LlapUtil.humanReadableByteCount(memoryAvailableToTask));
    }
    OperatorUtils.setMemoryAvailable(reducer.getChildOperators(), memoryAvailableToTask);

    // Setup values registry
    String valueRegistryKey = DynamicValue.DYNAMIC_VALUE_REGISTRY_CACHE_KEY;
    DynamicValueRegistryTez registryTez = dynamicValueCache.retrieve(valueRegistryKey,
            new Callable<DynamicValueRegistryTez>() {
              @Override
              public DynamicValueRegistryTez call() {
                return new DynamicValueRegistryTez();
              }
            });
    dynamicValueCacheKeys.add(valueRegistryKey);
    RegistryConfTez registryConf = new RegistryConfTez(jconf, reduceWork, processorContext, inputs);
    registryTez.init(registryConf);
    checkAbortCondition();

    if (numTags > 1) {
      sources = new ReduceRecordSource[numTags];
      mainWorkOIs = new ObjectInspector[numTags];
      initializeMultipleSources(reduceWork, numTags, mainWorkOIs, sources);
      ((TezContext) MapredContext.get()).setRecordSources(sources);
      reducer.initialize(jconf, mainWorkOIs);
    } else {
      numTags = tagToReducerMap.keySet().size();
      sources = new ReduceRecordSource[numTags];
      mainWorkOIs = new ObjectInspector[numTags];
      for (int i : tagToReducerMap.keySet()) {
        redWork = tagToReducerMap.get(i);
        reducer = redWork.getReducer();
        // Check immediately after reducer is assigned, in cae the abort came in during
        checkAbortCondition();
        initializeSourceForTag(redWork, i, mainWorkOIs, sources,
                redWork.getTagToValueDesc().get(0), redWork.getTagToInput().get(0));
        reducer.initializeLocalWork(jconf);
      }
      reducer = reduceWork.getReducer();
      // Check immediately after reducer is assigned, in cae the abort came in during
      checkAbortCondition();
      ((TezContext) MapredContext.get()).setRecordSources(sources);
      reducer.initialize(jconf, new ObjectInspector[] { mainWorkOIs[bigTablePosition] });
      for (int i : tagToReducerMap.keySet()) {
        if (i == bigTablePosition) {
          continue;
        }
        redWork = tagToReducerMap.get(i);
        reducer = redWork.getReducer();
        // Check immediately after reducer is assigned, in cae the abort came in during
        checkAbortCondition();
        reducer.initialize(jconf, new ObjectInspector[] { mainWorkOIs[i] });
      }
    }
    checkAbortCondition();

    reducer = reduceWork.getReducer();

    // initialize reduce operator tree
    try {
      l4j.info(reducer.dump(0));

      // Initialization isn't finished until all parents of all operators
      // are initialized. For broadcast joins that means initializing the
      // dummy parent operators as well.
      if (dummyOps != null) {
        for (HashTableDummyOperator dummyOp : dummyOps) {
          // TODO HIVE-14042. Propagating abort to dummyOps.
          dummyOp.initialize(jconf, null);
          checkAbortCondition();
        }
      }

      // set output collector for any reduce sink operators in the pipeline.
      List<Operator<?>> children = new LinkedList<Operator<?>>();
      children.add(reducer);
      if (dummyOps != null) {
        children.addAll(dummyOps);
      }
      createOutputMap();
      OperatorUtils.setChildrenCollector(children, outMap);

      checkAbortCondition();
      reducer.setReporter(reporter);
      MapredContext.get().setReporter(reporter);

    } catch (Throwable e) {
      super.setAborted(true);
      if (e instanceof OutOfMemoryError) {
        // Don't create a new object if we are already out of memory
        throw (OutOfMemoryError) e;
      } else if (e instanceof InterruptedException) {
        l4j.info("Hit an interrupt while initializing ReduceRecordProcessor. Message={}",
            e.getMessage());
        throw (InterruptedException) e;
      } else {
        throw new RuntimeException("Reduce operator initialization failed", e);
      }
    }

    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.TEZ_INIT_OPERATORS);
  }

  private void initializeMultipleSources(ReduceWork redWork, int numTags, ObjectInspector[] ois,
      ReduceRecordSource[] sources) throws Exception {
    for (int tag = 0; tag < redWork.getTagToValueDesc().size(); tag++) {
      if (redWork.getTagToValueDesc().get(tag) == null) {
        continue;
      }
      checkAbortCondition();
      initializeSourceForTag(redWork, tag, ois, sources, redWork.getTagToValueDesc().get(tag),
          redWork.getTagToInput().get(tag));
    }
  }

  private void initializeSourceForTag(ReduceWork redWork, int tag, ObjectInspector[] ois,
      ReduceRecordSource[] sources, TableDesc valueTableDesc, String inputName)
      throws Exception {
    reducer = redWork.getReducer();
    reducer.getParentOperators().clear();
    reducer.setParentOperators(null); // clear out any parents as reducer is the root

    TableDesc keyTableDesc = redWork.getKeyDesc();
    Reader reader = inputs.get(inputName).getReader();

    sources[tag] = new ReduceRecordSource();
    // Only the big table input source should be vectorized (if applicable)
    // Note this behavior may have to change if we ever implement a vectorized merge join
    boolean vectorizedRecordSource = (tag == bigTablePosition) && redWork.getVectorMode();
    sources[tag].init(jconf, redWork.getReducer(), vectorizedRecordSource, keyTableDesc,
        valueTableDesc, reader, tag == bigTablePosition, (byte) tag,
        redWork.getVectorizedRowBatchCtx(), redWork.getVectorizedVertexNum(),
        redWork.getVectorizedTestingReducerBatchSize());
    ois[tag] = sources[tag].getObjectInspector();
  }

  @Override
  void run() throws Exception {
    l4j.info("SORT_END to [" + InfoCollector.getVertexName() + "] at time " + System.currentTimeMillis());
    long runStart = System.currentTimeMillis();
    System.out.println("Profiling: GHive " +  InfoCollector.getVertexName() + " running starts at time: " + runStart +  "ms");
    Calendar calendar= Calendar.getInstance();
    SimpleDateFormat dateFormat= new SimpleDateFormat("hh:mm:ss");
    System.out.println(dateFormat.format(calendar.getTime()));

    for (Entry<String, LogicalOutput> outputEntry : outputs.entrySet()) {
      l4j.info("Starting Output: " + outputEntry.getKey());
      if (!isAborted()) {
        outputEntry.getValue().start();
        ((TezKVOutputCollector) outMap.get(outputEntry.getKey())).initialize();
      }
    }

    // run the operator pipeline
    startAbortChecks();

    if (InfoCollector.isGPU) {
      //Collect Data from each source.
      for (int tag = 0; tag < reduceWork.getTagToValueDesc().size(); tag++) {
        while (sources[tag].pushRecord()) ;
      }

      l4j.info("Profiling: Tez 'Shuffle' on Reduce stage ending at time " + System.currentTimeMillis() + " ms");
      System.out.println("Profiling: Tez 'Shuffle' on Reduce stage ending at time "
              + System.currentTimeMillis() + " ms");
      l4j.info("Profiling: Tez 'Processor' on Reduce stage starting at time "
              + System.currentTimeMillis() + " ms");
      System.out.println("Profiling: Tez 'Processor' on Reduce stage starting at time "
              + System.currentTimeMillis() + " ms");

      ReduceRecordSource source = sources[bigTablePosition];

      //Get the process root operator.
      Operator<?> reducer = source.getReducer();

      //Traverse the operator tree, get the maintainValue list for each join operator.
      traverse(reducer);

      // Ready the collected data for JNI transformation.
      JNIInterface.readyInputs();

      // GPU Processing
      GPUResult result = null;
      if (InfoCollector.hasData) {
        l4j.info("GHive: before JNIInterface.process().");
        result = JNIInterface.process();
        l4j.info("GHive: after JNIInterface.process().");
        assert result != null;
        try {
//          System.out.println("GHive: result batch: " + result.generateBatch());
        } catch (Exception e) {
          e.printStackTrace();
          System.exit(-1);
        }
      }
      if (result != null) {
        // Traverse the operator tree and get the sink operator.
        // sink operator is always at the bottom of the tree.
        Operator<?> sinkOp = reducer;
        while (!sinkOp.getChildOperators().isEmpty()) {
          sinkOp = sinkOp.getChildOperators().get(0);
        }

        // Integers in GPU are processed as long.
        // Get the type information to do transformation.
        ObjectInspector inspector = sinkOp.getInputObjInspectors()[0];
        result.setInspectorType(inspector.toString());

        // feed the computation result to the sink operator.
        if (sinkOp instanceof VectorReduceSinkCommonOperator) {
          l4j.info("GHive: sinkOp instance of VectorReduceSinkCommonOperator");
          sinkOp.process(result.generateBatch(), 0);
        } else if (sinkOp instanceof VectorFileSinkOperator) {
          l4j.info("GHive: sinkOp instance of VectorFileSinkOperator");
          while (result.hasNext()) {
            Object obj = result.next();
            ((VectorFileSinkOperator) sinkOp).processSingleRow(obj, 0);
          }
        } else if (sinkOp instanceof ReduceSinkOperator || sinkOp instanceof FileSinkOperator) {
          l4j.info("GHive: sinkOp instance of ReduceSinkOperator or FileSinkOperator");
          while (result.hasNext()) {
            Object obj = result.next();
            sinkOp.process(obj, 0);
          }
        }
      } else {
        l4j.info("GHive: no result in this " + InfoCollector.getVertexName());
      }
    } else {
      l4j.info("Profiling: Tez 'Processor' on Reduce stage starting at time "
              + System.currentTimeMillis() + " ms");
      while (sources[bigTablePosition].pushRecord()) {
        addRowAndMaybeCheckAbort();
      }
    }
    long runEnd = System.currentTimeMillis();
    System.out.println("Profiling: GHive " +  InfoCollector.getVertexName() + " running starts at time: " + runEnd + "ms");
    System.out.println("Profiling: GHive " +  InfoCollector.getVertexName() + " takes time: " + (runEnd - runStart) + "ms");
//    System.out.println("Profiling: GHive" +  InfoCollector.getVertexName() + " running ends at time:" + System.currentTimeMillis() + "ms");
    calendar= Calendar.getInstance();
    dateFormat= new SimpleDateFormat("hh:mm:ss");
    System.out.println(dateFormat.format(calendar.getTime()));

  }


  @Override
  public void abort() {
    // this will stop run() from pushing records, along with potentially
    // blocking initialization.
    super.abort();

    if (reducer != null) {
      l4j.info("Forwarding abort to reducer: {} " + reducer.getName());
      reducer.abort();
    } else {
      l4j.info("reducer not setup yet. abort not being forwarded");
    }
  }

  /**
   * Get the inputs that should be streamed through reduce plan.
   *
   * @param inputs
   * @return
   * @throws Exception
   */
  private List<LogicalInput> getShuffleInputs(Map<String, LogicalInput> inputs) throws Exception {
    // the reduce plan inputs have tags, add all inputs that have tags
    Map<Integer, String> tagToinput = reduceWork.getTagToInput();
    ArrayList<LogicalInput> shuffleInputs = new ArrayList<LogicalInput>();
    for (String inpStr : tagToinput.values()) {
      if (inputs.get(inpStr) == null) {
        throw new AssertionError("Cound not find input: " + inpStr);
      }
      inputs.get(inpStr).start();
      shuffleInputs.add(inputs.get(inpStr));
    }
    return shuffleInputs;
  }

  @Override
  void close(){
    if (cache != null && cacheKeys != null) {
      for (String key : cacheKeys) {
        cache.release(key);
      }
    }

    if (dynamicValueCache != null && dynamicValueCacheKeys != null) {
      for (String k: dynamicValueCacheKeys) {
        dynamicValueCache.release(k);
      }
    }

    try {
      if (isAborted()) {
        for (ReduceRecordSource rs: sources) {
          if (!rs.close()) {
            setAborted(false); // Preserving the old logic. Hmm...
            break;
          }
        }
      }

      boolean abort = isAborted();
      reducer.close(abort);
      if (mergeWorkList != null) {
        for (BaseWork redWork : mergeWorkList) {
          ((ReduceWork) redWork).getReducer().close(abort);
        }
      }

      // Need to close the dummyOps as well. The operator pipeline
      // is not considered "closed/done" unless all operators are
      // done. For broadcast joins that includes the dummy parents.
      List<HashTableDummyOperator> dummyOps = reduceWork.getDummyOps();
      if (dummyOps != null) {
        for (Operator<?> dummyOp : dummyOps) {
          dummyOp.close(abort);
        }
      }
      ReportStats rps = new ReportStats(reporter, jconf);
      reducer.preorderMap(rps);

    } catch (Exception e) {
      if (!isAborted()) {
        // signal new failure to map-reduce
        l4j.error("Hit error while closing operators - failing tree");
        throw new RuntimeException(
                "Hive Runtime Error while closing operators: " + e.getMessage(), e);
      }
    } finally {
      Utilities.clearWorkMap(jconf);
      MapredContext.close();
    }
  }

  private DummyStoreOperator getJoinParentOp(Operator<?> mergeReduceOp) {
    for (Operator<?> childOp : mergeReduceOp.getChildOperators()) {
      if ((childOp.getChildOperators() == null) || (childOp.getChildOperators().isEmpty())) {
        if (childOp instanceof DummyStoreOperator) {
          return (DummyStoreOperator) childOp;
        } else {
          throw new IllegalStateException("Was expecting dummy store operator but found: "
                  + childOp);
        }
      } else {
        return getJoinParentOp(childOp);
      }
    }
    throw new IllegalStateException("Expecting a DummyStoreOperator found op: " + mergeReduceOp);
  }
}
