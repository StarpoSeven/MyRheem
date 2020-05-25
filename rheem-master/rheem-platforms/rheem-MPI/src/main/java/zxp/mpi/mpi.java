package zxp.mpi;


import zxp.mpi.platform.mpiPlatform;
import zxp.mpi.plugin.mpiPlugin;

public class mpi {

    private final static mpiPlugin PLUGIN = new mpiPlugin();

    public static mpiPlugin plugin() {
        return PLUGIN;
    }

    public static mpiPlatform platform() {
            return mpiPlatform.getInstance();
        }

}
