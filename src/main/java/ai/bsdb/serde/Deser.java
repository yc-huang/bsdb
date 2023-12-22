package ai.bsdb.serde;

import org.msgpack.core.MessageUnpacker;

import java.io.IOException;

public interface Deser {
    byte[] from(MessageUnpacker unpacker, Field[] schema) throws IOException;
}
