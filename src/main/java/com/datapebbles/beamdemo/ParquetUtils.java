package com.datapebbles.beamdemo;

import lombok.experimental.UtilityClass;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.*;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MetadataCoder;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

@UtilityClass
class ParquetUtils {

    private final int MINIMUM_PARQUET_SIZE = ParquetFileWriter.MAGIC.length * 2 + 4;
    private final ParquetReadOptions OPTIONS = ParquetReadOptions.builder().build();
    private final ParquetMetadataConverter METADATA_CONVERTER = new ParquetMetadataConverter();
    private final AvroSchemaConverter TO_AVRO_CONVERTER = new AvroSchemaConverter();

    PCollection<Row> readParquetFile(String filePattern, Pipeline pipeline) throws IOException {
        ParquetMetadata metadata = readParquetMetadata(filePattern);
        org.apache.avro.Schema avroSchema = getAvroSchema(metadata);
        Schema beamSchema = getBeamSchema(metadata);

        PTransform<PBegin, PCollection<GenericRecord>> readTransform = ParquetIO.read(avroSchema).from(filePattern);
        SerializableFunction<GenericRecord, Row> conversionFn = AvroUtils.getGenericRecordToRowFunction(beamSchema);

        PCollection<Row> output = pipeline
                .apply(readTransform)
                .apply(ParDo.of(new DoFn<GenericRecord, Row>() {
                    @ProcessElement
                    public void processElement(ProcessContext ctx) {
                        GenericRecord record = ctx.element();
                        ctx.output(conversionFn.apply(record));
                    }
                }));
        output.setRowSchema(beamSchema);
        return output;
    }

    WriteFilesResult<Void> writeParquetFile(String destination, PCollection<Row> rows) {
        org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(rows.getSchema());
        PCollection<GenericRecord> records = rows.apply(ParDo.of(new DoFn<Row, GenericRecord>() {
            @ProcessElement
            public void processElement(ProcessContext ctx) {
                Row row = ctx.element();
                ctx.output(AvroUtils.toGenericRecord(row, avroSchema));
            }
        }));

        Coder<GenericRecord> coder = AvroCoder.of(avroSchema);
        records.setCoder(coder);

        FileIO.Sink<GenericRecord> sink = ParquetIO.sink(avroSchema);
        return records.apply(FileIO.<GenericRecord>write().via(sink).to(destination));
    }

    // The code below is taken in large part from the Apache Parquet project

    private ParquetMetadata readMetadata(FileIO.ReadableFile file) throws IOException {
        long size = file.getMetadata().sizeBytes();
        if (size < MINIMUM_PARQUET_SIZE) {
            throw new IllegalArgumentException("File is too small to be a parquet file");
        }
        SeekableByteChannel input = file.openSeekable();
        int footerLength = getFooterLength(size, input);
        verifyParquetMagic(file.getMetadata().resourceId().getFilename(), size, input);

        long footerIndex = size - ParquetFileWriter.MAGIC.length - 4 - footerLength;
        if (footerIndex < 0) {
            throw new RuntimeException("Parquet file is incorrect, the footer index is not within the file");
        }
        input.position(footerIndex);

        ByteBuffer footerBuffer = ByteBuffer.allocate(footerLength);
        input.read(footerBuffer);
        footerBuffer.flip();

        ByteBufferInputStream footerStream = ByteBufferInputStream.wrap(footerBuffer);
        return METADATA_CONVERTER.readParquetMetadata(footerStream, OPTIONS.getMetadataFilter());
    }

    private Schema getBeamSchema(ParquetMetadata meta) {
        return SchemaUtils.fromParquetType(meta.getFileMetaData().getSchema());
    }

    private org.apache.avro.Schema getAvroSchema(ParquetMetadata meta) {
        return TO_AVRO_CONVERTER.convert(meta.getFileMetaData().getSchema());
    }

    private ParquetMetadata readParquetMetadata(String filePattern) throws IOException {
        MatchResult match = FileSystems.match(filePattern);
        if (match.status() != MatchResult.Status.OK) {
            throw new RuntimeException("Unable to match file pattern " + filePattern);
        }
        MatchResult.Metadata meta = match.metadata().get(0);
        FileIO.ReadableFile file = toReadableFile(meta);

        return readMetadata(file);
    }

    private int getFooterLength(long size, SeekableByteChannel input) throws IOException {
        input.position(size - ParquetFileWriter.MAGIC.length - 4);
        ByteBuffer footerLengthBuffer = ByteBuffer.allocate(4);
        int footerLengthBytesRead = input.read(footerLengthBuffer);
        if (footerLengthBytesRead != 4) {
            throw new IllegalStateException("Failed to read the 4-byte footer length");
        }
        return BytesUtils.readIntLittleEndian(footerLengthBuffer, 0);
    }

    private void verifyParquetMagic(String fileName, long fileSize, SeekableByteChannel input) throws IOException {
        input.position(fileSize - ParquetFileWriter.MAGIC.length);
        ByteBuffer magicBuffer = ByteBuffer.allocate(ParquetFileWriter.MAGIC.length);
        int magicBytesRead = input.read(magicBuffer);
        if (magicBytesRead != ParquetFileWriter.MAGIC.length) {
            throw new IllegalStateException("Failed to read magic number");
        }
        if (!Arrays.equals(ParquetFileWriter.MAGIC, magicBuffer.array())) {
            throw new IllegalArgumentException(fileName + " is not a parquet file (magic number mismatch)");
        }
    }

    private FileIO.ReadableFile toReadableFile(MatchResult.Metadata meta) throws IOException {
        Path tempFile = Files.createTempFile("readable-file-spec", ".bin");
        OutputStream tempFileStream = Files.newOutputStream(tempFile);
        Compression fileCompression = Compression.detect(meta.resourceId().getFilename());

        MetadataCoder.of().encode(meta, tempFileStream);
        VarIntCoder.of().encode(fileCompression.ordinal(), tempFileStream);
        tempFileStream.flush();
        tempFileStream.close();

        InputStream inputStream = Files.newInputStream(tempFile);
        FileIO.ReadableFile result = ReadableFileCoder.of().decode(inputStream);

        if (!tempFile.toFile().delete()) {
            tempFile.toFile().deleteOnExit();
        }

        return result;
    }

}
