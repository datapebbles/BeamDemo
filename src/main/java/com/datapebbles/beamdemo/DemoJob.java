package com.datapebbles.beamdemo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Filter;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DemoJob {

    private static final SerializableFunction<String, Boolean> validScorePredicate =
            (rating) -> rating != null && rating.matches("^[12345]$");
    private static final SerializableFunction<Integer, Boolean> validHelpfulnessPredicate =
            (votes) -> votes != null && votes > 0;

    public static void main(String[] args) throws IOException {
        // Instantiate the pipeline using the command line arguments
        PipelineOptions opts = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        Pipeline pipeline = Pipeline.create(opts);

        // Read the input file into a collection of Rows (which have Beam Schemas attached to them)
        PCollection<Row> input = ParquetUtils.readParquetFile("../QualityPipeline/5m_book_reviews.parquet", pipeline);
        PCollection<Row> relevantFieldsOnly = selectRelevantFields(input);
        PCollection<Row> validRowsOnly = removeInvalidRows(relevantFieldsOnly);
        PCollection<Row> withCorrectTypes = castToCorrectTypes(validRowsOnly);
        PCollection<Row> withHelpfulness = computeHelpfulnessRatio(withCorrectTypes);
        PCollection<Row> aggregated = aggregate(withHelpfulness);


        ParquetUtils.writeParquetFile("output", aggregated);

        pipeline.run().waitUntilFinish();
    }

    private static PCollection<Row> selectRelevantFields(PCollection<Row> input) {
        return input.apply(Select.fieldNames(
                "customer_id",
                "product_id",
                "review_id",
                "star_rating",
                "helpful_votes",
                "total_votes"
        ));
    }

    private static PCollection<Row> removeInvalidRows(PCollection<Row> input) {
        return input.apply(Filter.<Row>create()
                .whereFieldName("star_rating", validScorePredicate)
                .whereFieldName("total_votes", validHelpfulnessPredicate)
        ).setRowSchema(input.getSchema());
    }

    private static PCollection<Row> castToCorrectTypes(PCollection<Row> input) {
        Schema castSchema = Schema.builder()
                .addNullableField("customer_id", Schema.FieldType.STRING)
                .addNullableField("product_id", Schema.FieldType.STRING)
                .addNullableField("review_id", Schema.FieldType.STRING)
                .addNullableField("star_rating", Schema.FieldType.INT32)
                .addNullableField("helpful_votes", Schema.FieldType.INT32)
                .addNullableField("total_votes", Schema.FieldType.INT32)
                .build();

        Map<String, SerializableFunction<Object, Object>> casts = new HashMap<>();
        casts.put("customer_id", Object::toString);
        casts.put("star_rating", str -> str == null ? 0 : Integer.parseInt(str.toString()));

        return input.apply(new CastTransform(castSchema, casts));
    }

    private static PCollection<Row> computeHelpfulnessRatio(PCollection<Row> input) {
        Schema resultSchema = Schema.builder()
                .addNullableField("customer_id", Schema.FieldType.STRING)
                .addNullableField("product_id", Schema.FieldType.STRING)
                .addNullableField("review_id", Schema.FieldType.STRING)
                .addNullableField("star_rating", Schema.FieldType.INT32)
                .addNullableField("helpful_votes", Schema.FieldType.INT32)
                .addNullableField("total_votes", Schema.FieldType.INT32)
                .addNullableField("helpfulness", Schema.FieldType.DOUBLE)
                .build();
        SqlTransform tf = SqlTransform.query("SELECT *, (CASE WHEN total_votes = 0 THEN NULL ELSE helpful_votes / " +
                "CAST(total_votes AS DOUBLE) END) AS helpfulness FROM PCOLLECTION");
        return input.apply(tf).setRowSchema(resultSchema);
    }

    private static PCollection<Row> aggregate(PCollection<Row> input) {
        Schema outputSchema = Schema.builder()
                .addNullableField("product_id", Schema.FieldType.STRING)
                .addNullableField("grand_total_votes", Schema.FieldType.INT32)
                .addNullableField("total_helpful_votes", Schema.FieldType.INT32)
                .addNullableField("mean_rating", Schema.FieldType.DOUBLE)
                .addNullableField("mean_helpfulness", Schema.FieldType.DOUBLE)
                .build();

        Group.CombineFieldsByFields<Row> grouper = Group
                .<Row>byFieldNames("product_id")
                .aggregateField("total_votes", Sum.ofIntegers(), "grand_total_votes")
                .aggregateField("helpful_votes", Sum.ofIntegers(), "total_helpful_votes")
                .aggregateField("star_rating", Mean.of(), "mean_rating")
                .aggregateField("helpfulness", Mean.of(), "mean_helpfulness");

        PCollection<Row> grouped = input.apply(grouper);

        return grouped
                .apply(new FlattenKVRows(grouped.getSchema(), new HashMap<>()))
                .setRowSchema(outputSchema);
    }
}
