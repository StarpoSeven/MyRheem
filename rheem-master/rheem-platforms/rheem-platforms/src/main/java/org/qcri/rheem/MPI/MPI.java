

import org.qcri.rheem.MPI.platform.MPIPlatform;
import org.qcri.rheem.MPI.plugin.MPIPlugin;

/**
 * Register for relevant components of this module.
 */
public class MPI {

    private final static MPIPlugin PLUGIN = new MPIPlugin();


    public static MPIPlugin plugin() {
        return PLUGIN;
    }

    public static MPIPlatform platform() {
        return MPIPlatform.getInstance();
    }

}
