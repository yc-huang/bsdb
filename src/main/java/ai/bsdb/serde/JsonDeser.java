package ai.bsdb.serde;

import org.msgpack.core.MessageUnpacker;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class JsonDeser implements Deser{
    @Override
    public byte[] from(MessageUnpacker unpacker, Field[] schema) throws IOException {
        ByteArrayOutputStream sb = new ByteArrayOutputStream();
        sb.write('{');
        boolean printComma = false;
        for(int idx = 0; idx < schema.length; idx ++){
            Field type = schema[idx];
            boolean isNil = unpacker.tryUnpackNil();
            if(!isNil){
                if(printComma) sb.write(','); else printComma = true;
                sb.write('"');
                sb.write(type.name.getBytes());
                sb.write('"');
                sb.write(':');
                if(type.isPrimitive) {
                    printPrimitive(type.type, unpacker, sb);
                }else if(type.type == Field.Type.MAP){
                    sb.write('{');
                    int size = unpacker.unpackMapHeader();
                    for(int i= 0; i < size; i++){
                        printPrimitive(type.kType, unpacker, sb);
                        sb.write(':');
                        printPrimitive(type.vType, unpacker, sb);
                        if(i < size - 1) sb.write(',');
                    }
                    sb.write('}');
                }else if(type.type == Field.Type.LIST){
                    sb.write('[');
                    int size = unpacker.unpackArrayHeader();
                    for(int i= 0; i < size; i++){
                        printPrimitive(type.vType, unpacker, sb);
                        if(i < size - 1) sb.write(',');
                    }
                    sb.write(']');
                }
            }
        }
        sb.write('}');
        return sb.toByteArray();
    }

    private void printPrimitive(Field.Type type, MessageUnpacker unpacker, ByteArrayOutputStream sb) throws IOException {
        if(type == Field.Type.STRING){
            int len = unpacker.unpackRawStringHeader();
            sb.write('"');
            if(len > 0){
                byte[] content = unpacker.readPayload(len);
                sb.write(content);
            }
            sb.write('"');
        }else if(type == Field.Type.DOUBLE){
            double v =unpacker.unpackDouble();
            sb.write(Double.toString(v).getBytes());
        } else if(type == Field.Type.LONG){
            long v =unpacker.unpackLong();
            sb.write(Long.toString(v).getBytes());
        }else if(type == Field.Type.INT){
            int v =unpacker.unpackInt();
            sb.write(Integer.toString(v).getBytes());
        }else if(type == Field.Type.FLOAT){
            float v =unpacker.unpackFloat();
            sb.write(Float.toString(v).getBytes());
        }else if(type == Field.Type.BOOLEAN){
            boolean v =unpacker.unpackBoolean();
            sb.write(Boolean.toString(v).getBytes());
        }
    }
}
