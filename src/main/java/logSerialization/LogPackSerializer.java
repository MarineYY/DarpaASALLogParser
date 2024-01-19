package logSerialization;

import org.apache.kafka.common.serialization.Serializer;
import provenenceGraph.dataModel.PDM;

import java.util.Map;

public class LogPackSerializer implements Serializer<PDM.LogPack> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String s, PDM.LogPack logPack) {
        byte[] bytes = new byte[]{};
        if (logPack == null) return bytes;
        else{
            return logPack.toByteArray();
        }
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
