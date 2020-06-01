package zxp.mpi.execution;


import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.*;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.util.Tuple;
import zxp.mpi.operators.mpiExecutionOperator;
import zxp.mpi.platform.mpiPlatform;

import java.util.*;


public class mpiExecutor extends ExecutorTemplate {

    private final mpiPlatform platform;

    private final Job job;

    private final Configuration configuration;

    public mpiExecutor(mpiPlatform platform, Job job) {
        super(job.getCrossPlatformExecutor());
        this.job = job;
        this.platform = platform;
        this.configuration = job.getConfiguration();
    }


    @Override
    public void execute(ExecutionStage stage, OptimizationContext optimizationContext, ExecutionState executionState) {
        Queue<ExecutionTask> scheduledTasks = new LinkedList<>(stage.getStartTasks());
        Set<ExecutionTask> executedTasks = new HashSet<>();
        while(!scheduledTasks.isEmpty()) {
            final ExecutionTask task = scheduledTasks.poll();
            if(executedTasks.contains(task)) continue;
            this.execute(task,optimizationContext,executionState);
            executedTasks.add(task);
            Arrays.stream(task.getOutputChannels()).flatMap(channel -> channel.getConsumers().stream()).filter(consumer->consumer.getStage() == stage).forEach(scheduledTasks::add);
            //test

        }
    }

    public void execute(ExecutionTask task,OptimizationContext optimizationContext,ExecutionState executionState) {
        //设置对应operator
        ChannelInstance[] inputChannelInstances = new ChannelInstance[task.getNumInputChannels()];
        for(int i = 0; i < inputChannelInstances.length;i++) {
            inputChannelInstances[i] = executionState.getChannelInstance(task.getInputChannel(i));
        }

        final OptimizationContext.OperatorContext operatorContext = optimizationContext.getOperatorContext(mpiExecutionOperator);//第一句注释上
        ChannelInstance[] outputChannelInstances = new ChannelInstance[task.getNumOuputChannels()];

        for (int i = 0; i < outputChannelInstances.length; i++) {
            outputChannelInstances[i] = task.getOutputChannel(i).createInstance(this, operatorContext, i);//this参数
        }

        long startTime = System.currentTimeMillis();
        final Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> results =
                mpiExecutionOperator.execute(inputChannelInstances, outputChannelInstances, operatorContext);//第一句注释
        long endTime = System.currentTimeMillis();


        final Collection<ExecutionLineageNode> executionLineageNodes = results.getField0();
        final Collection<ChannelInstance> producedChannelInstances = results.getField1();

        for (ChannelInstance outputChannelInstance : outputChannelInstances) {
            if (outputChannelInstance != null) {
                executionState.register(outputChannelInstance);
            }
        }

        final PartialExecution partialExecution = this.createPartialExecution(executionLineageNodes, endTime - startTime);
        executionState.add(partialExecution);
        this.registerMeasuredCardinalities(producedChannelInstances);


    }

    @Override
    public mpiPlatform getPlatform() {
        return mpiPlatform.getInstance();
    }
}
