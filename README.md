# Beam Demo

This is the code accompanying the article "Portable Pipelines with Apache Beam" at https://medium.com/datapebbles/portable-pipelines-with-apache-beam-2bd226563e49
It is primarily meant to provide a more concrete example of the concepts discussed in the article, but it is not a full, usable project.

The main class is in DemoJob.java, which loads a Parquet file, applies a series of transformations, and then writes the output back to disk.

To run the code for yourself, you will first need to download the data (in Parquet format) separately from here: https://s3.amazonaws.com/amazon-reviews-pds/readme.html
You will also need to edit line 34 of DemoJob.java to point to the downloaded Parquet file.
After that, it's a simple matter of building with Gradle.

To run the code, you will at least need to specify whether to use Apache Spark or Apache Flink as the runner with the `--runner=SparkRunner` or `--runner=FlinkRunner` command line parameters. To see all available parameters, see the Apache Beam documentation at https://beam.apache.org/documentation/runners/flink/ or https://beam.apache.org/documentation/runners/spark/.
