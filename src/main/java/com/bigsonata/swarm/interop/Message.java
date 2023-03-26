package com.bigsonata.swarm.interop;

import com.bigsonata.swarm.common.stats.Histogram;
import org.msgpack.core.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class Message {

  private String type;
  private Map<String, Object> data;
  private int version;
  private String nodeID;
  private static final String TYPE_CLIENT_READY = "client_ready";

  public Message(byte[] bytes) throws IOException {
    MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(bytes);

    int arrayHeader = unpacker.unpackArrayHeader();
    this.type = unpacker.unpackString();

    // unpack data
    if (unpacker.getNextFormat() != MessageFormat.NIL) {
      this.data = Message.unpackMap(unpacker);
    } else {
      unpacker.unpackNil();
      this.data = null;
    }
    if (unpacker.getNextFormat() != MessageFormat.NIL) {
      this.nodeID = unpacker.unpackString();
    } else {
      unpacker.unpackNil();
      this.nodeID = null;
    }
    unpacker.close();
  }

  public static Map<String, Object> unpackMap(MessageUnpacker unpacker) throws IOException {
    int mapSize = unpacker.unpackMapHeader();
    Map<String, Object> result = new HashMap<>(6);
    while (mapSize > 0) {
      String key = null;
      // unpack key
      if (unpacker.getNextFormat() == MessageFormat.NIL) {
        unpacker.unpackNil();
      } else {
        key = unpacker.unpackString();
      }
      // unpack value
      MessageFormat messageFormat = unpacker.getNextFormat();
      Object value;

      switch (messageFormat.getValueType()) {
        case BOOLEAN:
          value = unpacker.unpackBoolean();
          break;
        case FLOAT:
          value = unpacker.unpackFloat();
          break;
        case INTEGER:
          value = unpacker.unpackInt();
          break;
        case NIL:
          value = null;
          unpacker.unpackNil();
          break;
        case STRING:
          value = unpacker.unpackString();
          break;
        case MAP:
          value = unpackMap(unpacker);
          break;
        case ARRAY:
          int size = unpacker.unpackArrayHeader();
          value = new ArrayList(size);
          for(int index = 0; index < size; ++index) {
            ((ArrayList) value).add(unpacker.unpackString());
          }
          break;
        default:
          throw new IOException("Message received unsupported type: " + messageFormat.getValueType());
      }
      if (null != key) {
        result.put(key, value);
      }
      mapSize--;
    }
    return result;
  }

  public Message(String type, Map data, int version, String nodeID) {
    this.type = type;
    this.data = data;
    this.version = version;
    this.nodeID = nodeID;
  }

  public String getType() {
    return this.type;
  }

  public boolean isQuit() {
    return "quit".equals(getType());
  }

  public boolean isStop() {
    return "stop".equals(getType());
  }

  public boolean isSpawn() {
    return "spawn".equals(getType());
  }

  public Map getData() {
    return this.data;
  }

  public byte[] getBytes() throws IOException {
    MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
    Visitor visitor = new Visitor(packer);
    // a message contains three fields, (type & data & nodeID)
    packer.packArrayHeader(3);
    packer.packString(this.type);
    if (Message.TYPE_CLIENT_READY.equals(this.type)) {
      if (this.version == -1) {
        packer.packInt(this.version);
      } else {
        packer.packNil();
      }
    } else {
      if (this.data != null) {
        packer.packMapHeader(this.data.size());
        for (Map.Entry<String, Object> entry : this.data.entrySet()) {
          packer.packString(entry.getKey());
          visitor.visit(entry.getValue());
        }
      } else {
        packer.packNil();
      }
    }

    // pack the third field
    packer.packString(this.nodeID);
    byte[] bytes = packer.toByteArray();
    packer.close();
    return bytes;
  }

  static class Visitor {

    final MessagePacker packer;

    Visitor(MessagePacker packer) {
      this.packer = packer;
    }

    void visit(Object value) throws IOException {
      if (null == value) {
        visitNull(value);
      } else if (value instanceof String) {
        visitString(value);
      } else if (value instanceof Integer) {
        visitInt(value);
      } else if (value instanceof Long) {
        visitLong(value);
      } else if (value instanceof Boolean) {
        visitBool(value);
      } else if (value instanceof Float) {
        visitFloat(value);
      } else if (value instanceof Double) {
        visitDouble(value);
      } else if (value instanceof Map) {
        visitMap(value);
      } else if (value instanceof List) {
        visitList(value);
      } else if (value instanceof Histogram) {
        visitRps(value);
      } else {
        throw new IOException("Cannot pack type unknown type:" + value.getClass().getSimpleName());
      }
    }

    void visitNull(Object value) throws IOException {
      this.packer.packNil();
    }

    void visitString(Object value) throws IOException {
      packer.packString((String) value);
    }

    void visitInt(Object value) throws IOException {
      packer.packInt((Integer) value);
    }

    void visitLong(Object value) throws IOException {
      packer.packInt(((Long) value).intValue());
    }

    void visitBool(Object value) throws IOException {
      packer.packBoolean((Boolean) value);
    }

    void visitFloat(Object value) throws IOException {
      packer.packFloat((Float) value);
    }

    void visitDouble(Object value) throws IOException {
      packer.packFloat(((Double) value).floatValue());
    }

    void visitMap(Object value) throws IOException {
      Map<String, Object> map = (Map<String, Object>) value;
      packer.packMapHeader(map.size());
      for (Map.Entry entry : map.entrySet()) {
        this.visitString(entry.getKey());
        this.visit(entry.getValue());
      }
    }

    void visitList(Object value) throws IOException {
      List<Object> list = (List<Object>) value;
      packer.packArrayHeader(list.size());
      for (Object object : list) {
        this.visit(object);
      }
    }

    void visitRps(Object value) throws IOException {
      Histogram histogram = (Histogram) value;
      packer.packMapHeader(histogram.map.size());

      for (Map.Entry entry : histogram.map.entrySet()) {
        Long key = (Long) entry.getKey();
        int data = ((AtomicInteger) entry.getValue()).get();
        packer.packLong(key);
        packer.packInt(data);
      }
    }
  }
}
