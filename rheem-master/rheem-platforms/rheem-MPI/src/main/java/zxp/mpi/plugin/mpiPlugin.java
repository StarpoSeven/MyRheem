package zxp.mpi.plugin;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.optimizer.channels.ChannelConversion;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.plugin.Plugin;
import org.qcri.rheem.java.channels.ChannelConversions;
import org.qcri.rheem.java.mapping.Mappings;
import zxp.mpi.platform.mpiPlatform;

import java.util.Collection;
import java.util.Collections;

public class mpiPlugin implements Plugin {
    @Override
    public Collection<Platform> getRequiredPlatforms() {
        return Collections.singleton(mpiPlatform.getInstance());
    }

    @Override
    public Collection<Mapping> getMappings() {
        return Mappings.BASIC_MAPPINGS;
    }

    @Override
    public Collection<ChannelConversion> getChannelConversions() {
        return ChannelConversions.ALL;
    }

    @Override
    public void setProperties(Configuration configuration) {
        //等待完成
        return 缺少配置文件;
    }
}
