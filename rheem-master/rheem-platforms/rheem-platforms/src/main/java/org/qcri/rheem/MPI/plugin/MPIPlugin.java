package org.qcri.rheem.graphchi.plugin;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.optimizer.channels.ChannelConversion;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.plugin.Plugin;
import org.qcri.rheem.core.util.fs.LocalFileSystem;


import java.io.File;
import java.util.Collection;
import java.util.Collections;

/**
 * This {@link Plugin} activates default capabilities of the {@link GraphChiPlatform}.
 */
public class MPIPlugin implements Plugin {

    @Override
    public Collection<Mapping> getMappings() {
        return Mappings.ALL;
    }

    @Override
    public Collection<Platform> getRequiredPlatforms() {
        return Collections.singleton(MPIPlatform.getInstance());
    }

    @Override
    public Collection<ChannelConversion> getChannelConversions() {
        return ChannelConversions.ALL;
    }

    @Override
    public void setProperties(Configuration configuration) {
        final File localTempDir = LocalFileSystem.findTempDir();
        if (localTempDir != null) {
            configuration.setProperty("rheem.MPI.tempdir", localTempDir.toString());
        }
    }

}
