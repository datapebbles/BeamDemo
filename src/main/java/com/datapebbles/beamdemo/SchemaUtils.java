package com.datapebbles.beamdemo;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.experimental.UtilityClass;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;

import java.util.Iterator;
import java.util.Map;

@UtilityClass
class SchemaUtils {

    Schema fromParquetType(MessageType message) {
        org.apache.avro.Schema avroSchema = new AvroSchemaConverter().convert(message);
        return AvroUtils.toBeamSchema(avroSchema);
    }

}
