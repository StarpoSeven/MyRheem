package zxp.mpi.channels;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.AbstractChannelInstance;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.Executor;
import zxp.mpi.execution.mpiExecutor;

import java.util.Collection;
import java.util.OptionalLong;

public class mpiChannel extends Channel {
    public static final ChannelDescriptor DESCRIPTOR_MPI = new ChannelDescriptor(
            mpiChannel.class,true,false
    );

    public static final ChannelDescriptor DESCRIPTOR_MPI_MANY = new ChannelDescriptor(
            mpiChannel.class,true,false
    );

    protected mpiChannel(ChannelDescriptor descriptor, OutputSlot<?> producerSlot) {
        super(descriptor, producerSlot);
        assert descriptor == DESCRIPTOR_MPI || descriptor == DESCRIPTOR_MPI_MANY;
        this.markForInstrumentation();
    }

    protected mpiChannel(mpiChannel original) {
        super(original);
    }

    @Override
    public Channel copy() {
        return new mpiChannel(this);
    }

    @Override
    public ChannelInstance createInstance(Executor executor,
                                          OptimizationContext.OperatorContext producerOperatorContext,
                                          int producerOutputIndex) {
        return new Instance((mpiExecutor) executor,producerOperatorContext,producerOutputIndex);
    }

    public class Instance extends AbstractChannelInstance {

        private Collection<?> collection;
        private long size;


        /**
         * Creates a new instance and registers it with its {@link Executor}.
         *
         * @param executor                that maintains this instance
         * @param producerOperatorContext the {@link OptimizationContext.OperatorContext} for the producing
         *                                {@link ExecutionOperator}
         * @param producerOutputIndex     the output index of the producer {@link ExecutionTask}
         */
        protected Instance(mpiExecutor executor,
                           OptimizationContext.OperatorContext producerOperatorContext,
                           int producerOutputIndex) {
            super(executor, producerOperatorContext, producerOutputIndex);
        }

        public void accept(Collection<?> collection) {
            this.collection = collection;
        }
        
        public <T> Collection<T> provideConnection() {
            return (Collection<T>) this.collection;
        }
        
        public OptionalLong getMeasuredCardinality() {
            return this.size == 0 ? super.getMeasuredCardinality() : OptionalLong.of(this.size);
        }

        @Override
        public Channel getChannel() {
            return mpiChannel.this;
        }

        @Override
        protected void doDispose() throws Throwable {
                this.collection = null;
        }
    }
}
