package zxp.mpi.operators;

import org.qcri.rheem.basic.operators.ReduceByOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.costs.LoadProfile;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.JavaChannelInstance;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.execution.JavaExecutor;
import zxp.mpi.channels.mpiChannel;
import zxp.mpi.execution.mpiExecutor;

import javax.xml.crypto.KeySelector;
import java.security.Key;
import java.util.*;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

public class mpiReduceByOperator<InputType,KeyType>
        extends ReduceByOperator<InputType,KeyType>
        implements mpiExecutionOperator{


    public mpiReduceByOperator(TransformationDescriptor<InputType, KeyType> keyDescriptor,
                               ReduceDescriptor<InputType> reduceDescriptor,
                               DataSetType<InputType> type) {
        super(keyDescriptor, reduceDescriptor, type);
    }

    public mpiReduceByOperator(ReduceByOperator<InputType, KeyType> that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            mpiExecutor mpiExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length = this.getNumInputs();
        assert outputs.length = this.getNumOutputs();
        mpiChannel.Instance input = (mpiChannel.Instance) inputs[0];
        mpiChannel.Instance output = (mpiChannel.Instance) outputs[0];

        final mpiChannel<InputType> mpiChannelInput = input.provideMpiChannel();
        FunctionCompiler compiler = mpiExecutor.getCompiler();


        KeySelector<InputType,KeyType> keySelector = compiler.compileKeySelector(this.keyDescriptor);
        ReduceFunction<InputType> reduceFunction = compiler.compile(this.reduceDescriptor);
        mpiChannel<InputType> mpiChannelOutput =
                mpiChannelInput
                            .groupBy(keySelector)
                            .reduce(reduceFunction)
                            .setParallelism(zxp.mpi.execution.mpiExecutor.getNumDefaultPartitions());
        output.accept(mpiChannelOutput,mpiExecutor);
        return ExecutionOperator.modelLazyExecution(inputs,outputs,operatorContext);

//        Fucntion<InputType,KeyType> keyExtractor = compiler.compile(this.keyDescriptor);
//        BinaryOperator<InputType> reduceFunction = compiler.compile(this.reduceDescriptor);
//        JavaExecutor.openFunction(this,reduceFunction,inputs,operatorContext);
//        final Map<KeyType,InputType> reductionResult =
//                ((JavaChannelInstance)inputs[0]).
//                        <Type>provideStream().
//                            collect(Collectors.groupingBy(keyExtractor,new ReducingCollector<>(reduceFunction))
//                            );
//        ((CollectionChannel.Instance)outputs[0]).accept(reductionResult.values());
//        return  ExecutionOperator.modelEagerExecution(inputs,outputs,operatorContext);
//
//        PairFunction<InputType,KeyType,Type> keyExtractor = complie(this.keyDescriptor);
//        Function2<Type,Type,Type> reduceFunction = compiler.compile(this.reduceDescriptor,this,operatorContext,inputs);
//        JavaPairRDD<KeyType,Type> pairRdd = (RddChannel.Instance)inputs[0].mapToPair(keyExtractor);
//        JavaPairRDD<KeyType,Type> reducePairRdd = pairRdd.reduceBykey(reduceFunction,sparkExecutor.getNumDefaultPartitions());
//        outputRdd = reducePairRdd.map(new TupleConverter<>());
//        output.accept(outputRdd,sparkExecutor);
//        return ExecutionOperator.modelLazyExecution(inputs,outputs,operatorContext);




        return null;
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator =
                mpiExecutionOperator.super.createLoadProfileEstimator(configuration);
        LoadProfileEstimator.nestUdfEstimator(optEstimator,this.keyDescriptor,configuration);
        LoadProfileEstimator.nestUdfEstimator(optEstimator,this.reduceDescriptor,configuration);
        return optEstimator;

    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.mpi.reduceby.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        if(this.getInput(index).isBroadcast()) return Collections.singletonList(mpiChannel.DESCRIPTOR_MPI);
        return Arrays.asList(mpiChannel.DESCRIPTOR_MPI,mpiChannel.DESCRIPTOR_MPI_MANY);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(mpiChannel.DESCRIPTOR_MPI);
    }

    protected ExecutionOperator createCopy() {
        return new mpiReduceByOperator<>(this.getKeyDescriptor(),this.getReduceDescriptor(),this.getType());
    }

    public boolean containsAction() {
        return false;
    }
}
