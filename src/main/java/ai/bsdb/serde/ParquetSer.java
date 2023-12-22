package ai.bsdb.serde;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;

import java.io.IOException;
import java.util.List;

public class ParquetSer implements Ser {


    @Override
    public Field[] getSchema(FileStatus file, String exclude) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromStatus(file, new Configuration()))) {
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            List<Type> types = schema.getFields();
            int excludeIndex = schema.getFieldIndex(exclude);
            if (excludeIndex >= 0) types.remove(excludeIndex);
            Field[] pTypes = new Field[types.size()];
            int idx = 0;
            for (Type type : types) {
                Field pType = new Field();
                pType.name = type.getName();
                if (type.isPrimitive()) {
                    pType.isPrimitive = true;
                    pType.type = Ser.getPrimitiveType(type);
                } else {
                    pType.isPrimitive = false;
                    LogicalTypeAnnotation logicalType = type.getLogicalTypeAnnotation();
                    if (logicalType instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation) {
                        pType.type = Field.Type.MAP;
                        GroupType kvType = type.asGroupType().getType(0).asGroupType();
                        pType.kType = Ser.getPrimitiveType(kvType.getType(0));//TODO: adapt to different MAP structure
                        pType.vType = Ser.getPrimitiveType(kvType.getType(1)); //TODO: support complex key/value type
                    } else if (logicalType instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
                        pType.type = Field.Type.LIST;
                        pType.vType = Ser.getPrimitiveType(type.asGroupType().getType(0).asGroupType().getType(0));//TODO: support complex key/value type
                    } else {
                        pType.type = Field.Type.UNSUPPORTED;
                    }
                }

                pTypes[idx] = pType;
                idx++;
            }
            return pTypes;
        }
    }

    @Override
    public long read(FileStatus file, String keyField, RecordHandler handler) throws IOException, NoSuchFieldException {
        return readWithLimit(file, keyField, handler, 0);
    }

    @Override
    public long readWithLimit(FileStatus file, String keyField, RecordHandler handler, long limit) throws IOException, NoSuchFieldException {
        ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromStatus(file, new Configuration()));
        try {
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            List<Type> fields = schema.getFields();
            //System.err.println("found fields:" + fields);
            int keyIndex = schema.getFieldIndex(keyField);
            if (keyIndex < 0) {
                throw new NoSuchFieldException("key field not found:" + keyField);
            }

            PageReadStore pages;
            boolean enableLimit = limit > 0;
            long readCount = 0;
            MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
            while ((pages = reader.readNextRowGroup()) != null) {
                long rows = pages.getRowCount();
                MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

                for (int i = 0; i < rows; i++) {
                    Group simpleGroup = recordReader.read();
                    parseRecord(simpleGroup, schema, keyField, keyIndex, packer, handler);
                    readCount++;
                    if (enableLimit) {
                        if (readCount >= limit) return readCount;
                    }
                }
            }
            return readCount;
        } finally {
            reader.close();
        }
    }

    private static void parseRecord(Group row, MessageType schema, String keyFieldName, int keyIndex, MessageBufferPacker packer, RecordHandler handler) throws IOException {
        packer.clear();

        byte[] key = row.getBinary(keyFieldName, 0).getBytes();

        int count = schema.getFieldCount();
        for (int i = 0; i < count; i++) {
            if (i == keyIndex) continue;//ignore key field
            Type type = schema.getType(i);
            boolean exist = row.getFieldRepetitionCount(i) > 0;
            LogicalTypeAnnotation logicalType = type.getLogicalTypeAnnotation();
            if (type.isPrimitive()) {
                primitiveToMsgPack(row, i, type, packer, exist);
            } else {
                complexToMsgPack(row, i, type, logicalType, packer, exist);
            }

        }
        handler.onRecord(key, packer.toByteArray());
    }

    static void complexToMsgPack(Group parent, int fieldIdx, Type fieldType, LogicalTypeAnnotation logicalType, MessageBufferPacker packer, boolean exist) throws IOException {
        if (exist) {
            if (logicalType instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation) {
                GroupType kvType = fieldType.asGroupType().getType(0).asGroupType();
                Type kType = kvType.getType(0);
                Type vType = kvType.getType(1);
                Group map = parent.getGroup(fieldIdx, 0);
                int idx = map.getFieldRepetitionCount(0);

                packer.packMapHeader(idx);
                for (int i = 0; i < idx; i++) {
                    Group kv = map.getGroup(0, i);
                    primitiveToMsgPack(kv, 0, kType, packer, true);
                    primitiveToMsgPack(kv, 1, vType, packer, true);
                }

            } else if (logicalType instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
                Type itemType = fieldType.asGroupType().getType(0).asGroupType().getType(0);
                Group list = parent.getGroup(fieldIdx, 0);
                int idx = list.getFieldRepetitionCount(0);

                packer.packArrayHeader(idx);
                for (int i = 0; i < idx; i++) {
                    primitiveToMsgPack(list.getGroup(0, i), 0, itemType, packer, true);
                }
            } else {
                //TODO
            }
        } else {
            packer.packNil();
        }


    }

    static void primitiveToMsgPack(Group parent, int fieldIdx, Type fieldType, MessageBufferPacker packer, boolean exist) throws IOException {
        switch (fieldType.asPrimitiveType().getPrimitiveTypeName()) {
            case BINARY:
            case FIXED_LEN_BYTE_ARRAY:
                if (exist) {
                    byte[] s = parent.getBinary(fieldIdx, 0).getBytes();
                    packer.packRawStringHeader(s.length);
                    packer.writePayload(s);
                } else {
                    packer.packNil();
                }
                break;

            case INT32:
                if (exist) {
                    int v = parent.getInteger(fieldIdx, 0);
                    packer.packInt(v);
                } else {
                    packer.packNil();
                }
                break;
            case INT64:
                if (exist) {
                    long v = parent.getLong(fieldIdx, 0);
                    packer.packLong(v);
                } else {
                    packer.packNil();
                }
                break;
            case DOUBLE:
                if (exist) {
                    double v = parent.getDouble(fieldIdx, 0);
                    packer.packDouble(v);
                } else {
                    packer.packNil();
                }
                break;
            case FLOAT:
                if (exist) {
                    float v = parent.getFloat(fieldIdx, 0);
                    packer.packFloat(v);
                } else {
                    packer.packNil();
                }
                break;
            case BOOLEAN:
                if (exist) {
                    boolean v = parent.getBoolean(fieldIdx, 0);
                    packer.packBoolean(v);
                } else {
                    packer.packNil();
                }
                break;

            default:
                packer.packNil();
        }
    }
}
