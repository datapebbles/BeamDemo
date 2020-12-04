package com.datapebbles.beamdemo;

import lombok.AccessLevel;
import lombok.Getter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

class FlattenKVRows extends PTransform<PCollection<Row>, PCollection<Row>> {

    private final Map<String, Schema.FieldType> typeMap;
    private final Schema inSchema;
    @Getter(AccessLevel.PACKAGE) private final Schema flatSchema;

    FlattenKVRows(Schema inSchema, Map<String, Schema.FieldType> typeMap) {
        this.inSchema = inSchema;
        this.typeMap = typeMap;
        this.flatSchema = generateFlatSchema();
    }

    @Override
    public PCollection<Row> expand(PCollection<Row> input) {
        /*
        Transform a PCollection of the form:
            key:
                A: 1
                B: 2
            value:
                C: 3
                D: 4
        To:
            A: 1
            B: 2
            C: 3
            D: 4
         */
        return input.apply(ParDo.of(new DoFn<Row, Row>() {
            @ProcessElement
            public void processElement(ProcessContext ctx) {
                Row in = ctx.element();
                Row key = Objects.requireNonNull(in.getRow("key"));
                Row value = Objects.requireNonNull(in.getRow("value"));
                Row out = Row.withSchema(flatSchema)
                        .addValues(key.getValues())
                        .addValues(value.getValues())
                        .build();
                ctx.output(out);
            }
        }));
    }

    private Schema.Field mapField(Schema.Field f) {
        if (typeMap.containsKey(f.getName())) {
            return Schema.Field.nullable(f.getName(), typeMap.get(f.getName()));
        }
        return f.withNullable(true);
    }

    private Schema generateFlatSchema() {
        Schema.Builder builder = Schema.builder();
        Schema keySchema = Objects.requireNonNull(inSchema.getField("key").getType().getRowSchema());
        Schema valueSchema = Objects.requireNonNull(inSchema.getField("value").getType().getRowSchema());
        builder.addFields(keySchema.getFields().stream().map(this::mapField).collect(Collectors.toList()));
        builder.addFields(valueSchema.getFields().stream().map(this::mapField).collect(Collectors.toList()));
        return builder.build();
    }
}
