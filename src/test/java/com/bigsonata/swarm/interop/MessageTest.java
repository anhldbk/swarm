package com.bigsonata.swarm.interop;

import static org.junit.jupiter.api.Assertions.*;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MessageTest {
  @Test
  void testMessageDecodeCorrectly() {
    List<Map<String, String>> dataList = Arrays.asList(
        null,
        ImmutableMap.of("state", "idle")
    );

    for (Map<String, String> data : dataList) {
      assertDoesNotThrow(() -> {
        Message message = new Message("test", data, -1, "nodeId");
        Message decoded = new Message(message.getBytes());

        assertEquals("test", decoded.getType());
        assertEquals(data, decoded.getData());
        assertEquals("nodeId", decoded.getNodeID());
      });
    }

  }
}