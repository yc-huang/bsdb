package tech.bsdb;

import junit.framework.TestCase;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

public class BaseTest extends TestCase {
    protected File basePath = new File("./tmp/");

    protected byte[] genValue(int len) {
        return gen(len);
    }

    protected byte[] genKey(int len, long index) {
        ByteBuffer buf = ByteBuffer.allocate(Math.max(len, 8));
        buf.putLong(index);
        if (len > 8) {
            byte[] tail = gen(len - 8);
            buf.put(tail);
        }
        return buf.array();
    }

    protected byte[] gen(int len) {
        byte[] buf = new byte[len];
        ThreadLocalRandom.current().nextBytes(buf);
        return buf;
    }
}
