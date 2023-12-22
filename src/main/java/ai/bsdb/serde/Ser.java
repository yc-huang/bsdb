package ai.bsdb.serde;

import org.apache.hadoop.fs.FileStatus;
import org.apache.parquet.schema.Type;

import java.io.IOException;

public interface Ser {
    Field[] getSchema(FileStatus file, String exclude) throws IOException;
    long read(FileStatus file, String keyField, RecordHandler handler) throws IOException, NoSuchFieldException;
    long readWithLimit(FileStatus file, String keyField, RecordHandler handler, long limit) throws IOException, NoSuchFieldException;

    static Field.Type getPrimitiveType(Type type) {
        switch (type.asPrimitiveType().getPrimitiveTypeName()) {
            case BINARY:
            case FIXED_LEN_BYTE_ARRAY:
                return Field.Type.STRING;

            case INT32:
                return Field.Type.INT;

            case INT64:
                return Field.Type.LONG;

            case FLOAT:
                return Field.Type.FLOAT;

            case DOUBLE:
                return Field.Type.DOUBLE;

            case BOOLEAN:
                return Field.Type.BOOLEAN;

            default:
                return Field.Type.UNSUPPORTED;
        }
    }

    interface RecordHandler {
        void onRecord(byte[] key, byte[] value);
    }
}
