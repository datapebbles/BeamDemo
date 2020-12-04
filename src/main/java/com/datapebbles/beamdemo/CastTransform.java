package com.datapebbles.beamdemo;

import lombok.RequiredArgsConstructor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.util.Map;

@RequiredArgsConstructor
class CastTransform extends PTransform<PCollection<Row>, PCollection<Row>> {

    private final Schema outputSchema;
    private final Map<String, SerializableFunction<Object, Object>> casts;

    @Override
    public PCollection<Row> expand(PCollection<Row> input) {
        return input
                .apply(ParDo.of(new CastDoFn()))
                .setRowSchema(outputSchema);
    }

    private class CastDoFn extends DoFn<Row, Row> {
        @ProcessElement
        public void process(ProcessContext ctx) {
            Row input = ctx.element();
            Row.Builder builder = Row.withSchema(outputSchema);

            // For fields which have a casting function registered, apply the cast and add it to the builder.
            // For all others, just add the field as-is.
            input.getSchema().getFields().forEach(field -> {
                String name = field.getName();
                Object value = input.getValue(name);
                if (casts.containsKey(name)) {
                    value = casts.get(name).apply(value);
                }
                builder.addValue(value);
            });

            ctx.output(builder.build());
        }
    }
}
