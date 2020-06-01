package zxp.mpi.operators;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.util.Tuple;
import zxp.mpi.execution.mpiExecutor;
import zxp.mpi.platform.mpiPlatform;

import java.util.Collection;

public interface mpiExecutionOperator extends ExecutionOperator {

    default mpiPlatform getPlatform() {
        return mpiPlatform.getInstance();
    }

    Tuple<Collection<ExecutionLineageNode>,Collection<ChannelInstance>> evaluate (
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            mpiExecutor mpiExecutor,
            OptimizationContext.OperatorContext operatorContext);

}
