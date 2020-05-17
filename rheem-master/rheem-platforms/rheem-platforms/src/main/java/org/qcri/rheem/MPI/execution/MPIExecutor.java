

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.ExecutionState;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.ExecutorTemplate;
import org.qcri.rheem.core.platform.PartialExecution;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.util.Tuple;


import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;


public class MPIExecutor extends ExecutorTemplate {

    private final MPIPlatform platform;

    private final Configuration configuration;

    private final Job job;

    public MPIExecutor(MPIPlatform platform, Job job) {
        super(job.getCrossPlatformExecutor());
        this.job = job;
        this.platform = platform;
        this.configuration = job.getConfiguration();
    }

    @Override
    public void execute(final ExecutionStage stage, OptimizationContext optimizationContext, ExecutionState executionState) {
        Queue<ExecutionTask> scheduledTasks = new LinkedList<>(stage.getStartTasks());
        Set<ExecutionTask> executedTasks = new HashSet<>();

        while (!scheduledTasks.isEmpty()) {
            final ExecutionTask task = scheduledTasks.poll();
            if (executedTasks.contains(task)) continue;
            this.execute(task, optimizationContext, executionState);
            executedTasks.add(task);
            Arrays.stream(task.getOutputChannels())
                    .flatMap(channel -> channel.getConsumers().stream())
                    .filter(consumer -> consumer.getStage() == stage)
                    .forEach(scheduledTasks::add);
        }
    }

    /**
     * Brings the given {@code task} into execution.
     */
    private void execute(ExecutionTask task, OptimizationContext optimizationContext, ExecutionState executionState) {
        final MPIExecution MPIExecution = (MPIExecution) task.getOperator();

        ChannelInstance[] inputChannelInstances = new ChannelInstance[task.getNumInputChannels()];
        for (int i = 0; i < inputChannelInstances.length; i++) {
            inputChannelInstances[i] = executionState.getChannelInstance(task.getInputChannel(i));
        }
        final OptimizationContext.OperatorContext operatorContext = optimizationContext.getOperatorContext(MPIExecutionOperator);
        ChannelInstance[] outputChannelInstances = new ChannelInstance[task.getNumOuputChannels()];
        for (int i = 0; i < outputChannelInstances.length; i++) {
            outputChannelInstances[i] = task.getOutputChannel(i).createInstance(this, operatorContext, i);
        }

        long startTime = System.currentTimeMillis();
        final Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> results =
                MPIExecutionOperator.execute(inputChannelInstances, outputChannelInstances, operatorContext);
        long endTime = System.currentTimeMillis()
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
    public MPIPlatform getPlatform() {
        return this.platform;
    }
}
