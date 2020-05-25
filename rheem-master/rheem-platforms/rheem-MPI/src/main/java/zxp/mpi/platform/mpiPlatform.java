package zxp.mpi.platform;

import org.apache.commons.lang3.ObjectUtils;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.costs.LoadProfileToTimeConverter;
import org.qcri.rheem.core.optimizer.costs.TimeToCostConverter;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.util.ReflectionUtils;
import zxp.mpi.execution.mpiExecutor;

public class mpiPlatform extends Platform {

    private static final String DEFAULT_CONFIG_FILE =;//缺失待完成

    private static mpiPlatform instance = null;


    private mpiPlatform() {

        super("MPI","mpi";);//配置文件缺失
    }

    public static mpiPlatform getInstance() {
        if(instance == null) {
            instance = new mpiPlatform();
        }
        return instance;
    }
    @Override
    protected void configureDefaults(Configuration configuration) {
        configuration.load(ReflectionUtils.loadResource(DEFAULT_CONFIG_FILE));
    }

    @Override
    public Executor.Factory getExecutorFactory() {

        return job -> new mpiExecutor(this,job);
    }


    @Override
    public LoadProfileToTimeConverter createLoadProfileToTimeConverter(Configuration configuration) {
        ;
    }

    @Override
    public TimeToCostConverter createTimeToCostConverter(Configuration configuration) {
        ;
    }
}
