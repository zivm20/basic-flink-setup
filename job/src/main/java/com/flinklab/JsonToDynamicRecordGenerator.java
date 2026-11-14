package com.flinklab;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecord;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecordGenerator;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Routes Kafka JSON by the {@code dataset} field into Iceberg tables under a fixed namespace.
 *
 * <p>Example payload:
 *
 * <pre>
 * {"dataset":"orders","order_id":1,"amount":9.5,"status":"NEW"}
 * </pre>
 */
public class JsonToDynamicRecordGenerator implements DynamicRecordGenerator<String> {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String namespace;

  public JsonToDynamicRecordGenerator(String namespace) {
    this.namespace = namespace;
  }

  @Override
  public void generate(String json, Collector<DynamicRecord> out) throws Exception {
    JsonNode root = MAPPER.readTree(json);
    if (root == null || !root.isObject()) {
      return;
    }

    JsonNode datasetNode = root.get("dataset");
    if (datasetNode == null || datasetNode.isNull() || datasetNode.asText().isBlank()) {
      throw new IllegalArgumentException("JSON missing required string field 'dataset': " + json);
    }
    String dataset = datasetNode.asText().trim();

    List<Types.NestedField> fields = new ArrayList<>();
    List<Object> values = new ArrayList<>();
    int fieldId = 1;

    Iterator<Map.Entry<String, JsonNode>> it = root.fields();
    while (it.hasNext()) {
      Map.Entry<String, JsonNode> entry = it.next();
      String name = entry.getKey();
      if ("dataset".equals(name)) {
        continue;
      }
      JsonNode value = entry.getValue();
      Types.NestedField field = toField(fieldId++, name, value);
      fields.add(field);
      values.add(toJavaValue(value, field.type().typeId()));
    }

    if (fields.isEmpty()) {
      // Keep at least one column so Iceberg can create the table
      fields.add(Types.NestedField.optional(1, "payload", Types.StringType.get()));
      values.add(json);
    }

    Schema schema = new Schema(fields);
    RowData row = toRowData(values);
    TableIdentifier tableId = TableIdentifier.of(namespace, dataset);

    out.collect(
        new DynamicRecord(
            tableId,
            "main",
            schema,
            row,
            PartitionSpec.unpartitioned()));
  }

  private static Types.NestedField toField(int id, String name, JsonNode value) {
    if (value == null || value.isNull()) {
      return Types.NestedField.optional(id, name, Types.StringType.get());
    }
    if (value.isBoolean()) {
      return Types.NestedField.optional(id, name, Types.BooleanType.get());
    }
    if (value.isIntegralNumber()) {
      return Types.NestedField.optional(id, name, Types.LongType.get());
    }
    if (value.isNumber()) {
      return Types.NestedField.optional(id, name, Types.DoubleType.get());
    }
    // objects / arrays stored as JSON strings for simplicity
    return Types.NestedField.optional(id, name, Types.StringType.get());
  }

  private static Object toJavaValue(JsonNode value, Type.TypeID typeId) {
    if (value == null || value.isNull()) {
      return null;
    }
    return switch (typeId) {
      case BOOLEAN -> value.asBoolean();
      case LONG -> value.asLong();
      case DOUBLE -> value.asDouble();
      default -> value.isValueNode() ? value.asText() : value.toString();
    };
  }

  private static RowData toRowData(List<Object> values) {
    Object[] converted = new Object[values.size()];
    for (int i = 0; i < values.size(); i++) {
      Object v = values.get(i);
      if (v instanceof String s) {
        converted[i] = StringData.fromString(s);
      } else {
        converted[i] = v;
      }
    }
    return GenericRowData.of(converted);
  }
}
