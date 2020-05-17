
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.costs.LoadProfileToTimeConverter;
import org.qcri.rheem.core.optimizer.costs.LoadToTimeConverter;
import org.qcri.rheem.core.optimizer.costs.TimeToCostConverter;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.util.ReflectionUtils;



public class MPIPlatform extends Platform {

    public static final String CPU_MHZ_PROPERTY = " ";

    public static final String CORES_PROPERTY = " ";

    public static final String HDFS_MS_PER_MB_PROPERTY = " ";

    private static final String DEFAULT_CONFIG_FILE = " ";

    private static MPIPlatform instance;

    protected MPIPlatform() {
        super("MPI", "MPI");
        this.initialize();
    }

    /**
     * Initializes this instance.
     */
    private void initialize() {
        // Set up.
        CompressedIO.disableCompression();
    }

    @Override
    public void configureDefaults(Configuration configuration) {
        configuration.load(ReflectionUtils.loadResource(DEFAULT_CONFIG_FILE));
    }

    public static MPIPlatform getInstance() {
        if (instance == null) {
            instance = new MPIPlatform();
        }
        return instance;
    }

    @Override
    public Executor.Factory getExecutorFactory() {
        return job -> new MPIChiExecutor(this, job);
    }

    @Override
    public LoadProfileToTimeConverter createLoadProfileToTimeConverter(Configuration configuration) {
        int cpuMhz = (int) configuration.getLongProperty(CPU_MHZ_PROPERTY);
        int numCores = (int) configuration.getLongProperty(CORES_PROPERTY);
        double hdfsMsPerMb = configuration.getDoubleProperty(HDFS_MS_PER_MB_PROPERTY);
        return LoadProfileToTimeConverter.createDefault(
                LoadToTimeConverter.createLinearCoverter(1 / (numCores * cpuMhz * 1000d)),
                LoadToTimeConverter.createLinearCoverter(hdfsMsPerMb / 1000000),
                LoadToTimeConverter.createLinearCoverter(0),
                (cpuEstimate, diskEstimate, networkEstimate) -> cpuEstimate.plus(diskEstimate).plus(networkEstimate)
        );
    }


}
