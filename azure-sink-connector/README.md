# Microsoft Azure Blob Storage connector for Apache Kafka®

This is a sink
[Apache Kafka Connect](https://kafka.apache.org/documentation/#connect)
connector that stores Kafka messages in an
[Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs/) container.

The connector requires Java 11 or newer for development and production.

## How It Works

The connector subscribes to the specified Kafka topics and collects
messages coming in them and periodically dumps the collected data to the
specified container in Azure Blob Storage.

Sometimes—for example, on reprocessing of some data—the connector will overwrite files that are already in the container. You need to ensure the container doesn't have a retention policy that prohibits overwriting.

The following object permissions must be enabled in the container:
- `Blob Storage Data Contributor` (needed for creating and overwriting blobs).


### File name format

The connector uses the following format for output files (blobs):
`<prefix><filename>`.

`<prefix>` is the optional prefix that can be used, for example, for
subdirectories in the bucket.

`<filename>` is the file name. The connector has the configurable
template for file names. It supports placeholders with variable names:
`{{ variable_name }}`. Currently, supported variables are:
- `topic` - the Kafka topic;
- `partition:padding=true|false` - the Kafka partition, if `padding` set to `true` it will set leading zeroes for offset, the default value is `false`;
- `start_offset:padding=true|false` - the Kafka offset of the first record in the file, if `padding` set to `true` it will set leading zeroes for offset, the default value is `false`;
- `timestamp:unit=yyyy|MM|dd|HH` - the timestamp of when the Kafka record has been processed by the connector.
    - `unit` parameter values:
        - `yyyy` - year, e.g. `2020` (please note that `YYYY` is deprecated and is interpreted as `yyyy`)
        - `MM` - month, e.g. `03`
        - `dd` - day, e.g. `01`
        - `HH` - hour, e.g. `24`
- `key` - the Kafka key.

To add zero padding to Kafka offsets, you need to add additional parameter `padding` in the `start_offset` variable,
which value can be `true` or `false` (the default).
For example: `{{topic}}-{{partition}}-{{start_offset:padding=true}}.gz`
will produce file names like `mytopic-1-00000000000000000001.gz`.

To add zero padding to partition number, you need to add additional parameter `padding` in the `partition` variable,
which value can be `true` or `false` (the default).
For example: `{{topic}}-{{partition:padding=true}}-{{start_offset}}.gz`
will produce file names like `mytopic-0000000001-1.gz`.

To add formatted timestamps, use `timestamp` variable.<br/>
For example: `{{topic}}-{{partition}}-{{start_offset}}-{{timestamp:unit=yyyy}}{{timestamp:unit=MM}}{{timestamp:unit=dd}}.gz`
will produce file names like `mytopic-2-1-20200301.gz`.

To configure the time zone for the `timestamp` variable,
use `file.name.timestamp.timezone` property.
Please see the description of properties in the "Configuration" section.

Only the certain combinations of variables and parameters are allowed in the file name
template (however, variables in a template can be in any order). Each
combination determines the mode of record grouping the connector will
use. Currently, supported combinations of variables and the corresponding
record grouping modes are:
- `topic`, `partition`, `start_offset`, and `timestamp` - grouping by the topic,
  partition, and timestamp;
- `key` - grouping by the key.

If the file name template is not specified, the default value is
`{{topic}}-{{partition}}-{{start_offset}}` (+ `.gz` when compression is
enabled).

### Record grouping

Incoming records are being grouped until flushed.

#### Grouping by the topic and partition

In this mode, the connector groups records by the topic and partition.
When a file is written, an offset of the first record in it is added to
its name.

For example, let's say the template is
`{{topic}}-part{{partition}}-off{{start_offset}}`. If the connector
receives records like
```
topic:topicB partition:0 offset:0
topic:topicA partition:0 offset:0
topic:topicA partition:0 offset:1
topic:topicB partition:0 offset:1
flush
```

there will be two files `topicA-part0-off0` and `topicB-part0-off0` with
two records in each.

Each `flush` produces a new set of files. For example:

```
topic:topicA partition:0 offset:0
topic:topicA partition:0 offset:1
flush
topic:topicA partition:0 offset:2
topic:topicA partition:0 offset:3
flush
```

In this case, there will be two files `topicA-part0-off0` and
`topicA-part0-off2` with two records in each.

#### Grouping by the key

In this mode, the connector groups records by the Kafka key. It always
puts one record in a file, the latest record that arrived before a flush
for each key. Also, it overwrites files if later new records with the
same keys arrive.

This mode is good for maintaining the latest values per key as files on
Azure Blob Storage.

Let's say the template is `k{{key}}`. For example, when the following
records arrive
```
key:0 value:0
key:1 value:1
key:0 value:2
key:1 value:3
flush
```

there will be two files `k0` (containing value `2`) and `k1` (containing
value `3`).

After a flush, previously written files might be overwritten:
```
key:0 value:0
key:1 value:1
key:0 value:2
key:1 value:3
flush
key:0 value:4
flush
```

In this case, there will be two files `k0` (containing value `4`) and
`k1` (containing value `3`).

##### The string representation of a key

The connector in this mode uses the following algorithm to create the
string representation of a key:

1. If `key` is `null`, the string value is `"null"` (i.e., string
   literal `null`).
2. If `key` schema type is `STRING`, it's used directly.
3. Otherwise, Java `.toString()` is applied.

If keys of you records are strings, you may want to use
`org.apache.kafka.connect.storage.StringConverter` as `key.converter`.

##### Warning: Single key in different partitions

The `group by key` mode primarily targets scenarios where each key
appears in one partition only. If the same key appears in multiple
partitions the result may be unexpected.

For example:
```
topic:topicA partition:0 key:x value:aaa
topic:topicA partition:1 key:x value:bbb
flush
```
file `kx` may contain `aaa` or `bbb`, i.e. the behavior is
non-deterministic.

### Data format

There are four types of data format available:
- **[Default]** Flat structure, where field values are separated by comma (`csv`)

  Configuration: ```format.output.type=csv```.
  Also, this is the default if the property is not present in the configuration.

- Complex structure, where file is in format of [JSON lines](https://jsonlines.org/).
  It contains one record per line and each line is a valid JSON object(`jsonl`)

  Configuration: ```format.output.type=jsonl```.

- Complex structure, where file is a valid JSON array of record objects.

  Configuration: ```format.output.type=json```.

- Complex structure, where file is in Apache [Parquet](https://parquet.apache.org/documentation/latest/) file format.

  Configuration: ```format.output.type=parquet```.

- Complex structure, where file is in Apache [Avro Container File](https://avro.apache.org/docs/current/specification/#object-container-files) file format.

  Configuration: ```format.output.type=avro```.


The connector can output the following fields from records into the
output: the key, the value, the timestamp, the offset and headers. (The set of
these output fields is configurable via ```format.output.fields``` property.) The field values are separated by comma.

It is possible to control the number of records to be put in a
particular output file by setting `file.max.records`. By default, it is
`0`, which is interpreted as "unlimited".

#### CSV Format example

The key and the value—if they're output—are stored as binaries encoded
in [Base64](https://en.wikipedia.org/wiki/Base64).

For example, if we output `key,value,offset,timestamp`, a record line might look like:
```
a2V5,TG9yZW0gaXBzdW0gZG9sb3Igc2l0IGFtZXQ=,1232155,1554210895
```

It is possible to control the encoding of the `value` field by setting
`format.output.fields.value.encoding` to `base64` or `none`.

If the key, the value or the timestamp is null, an empty string will be
output instead:

```
,,,1554210895
```

**NB!**

- The `key.converter` property must be set to `org.apache.kafka.connect.converters.ByteArrayConverter`
  or `org.apache.kafka.connect.storage.StringConverter` for this data format.

- The `value.converter` property must be set to `org.apache.kafka.connect.converters.ByteArrayConverter` for this data format.

#### JSONL Format example

For example, if we output `key,value,offset,timestamp`, a record line might look like:

```json
{ "key": "k1", "value": "v0", "offset": 1232155, "timestamp":"2020-01-01T00:00:01Z" }
```

OR

```json
{ "key": "user1", "value": {"name": "John", "address": {"city": "London"}}, "offset": 1232155, "timestamp":"2020-01-01T00:00:01Z" }
```

It is recommended to use
- `org.apache.kafka.connect.storage.StringConverter`,
- `org.apache.kafka.connect.json.JsonConverter`, or
- `io.confluent.connect.avro.AvroConverter`.

as `key.converter` and/or `value.converter` to make output files human-readable.

**NB!**

- The value of the `format.output.fields.value.encoding` property is ignored for this data format.
- Value/Key schema will not be presented in output file, even if `value.converter.schemas.enable` property is `true`.
  But, it is still important to set this property correctly, so that connector could read records correctly.

#### JSON Format example

For example, if we output `key,value,offset,timestamp`, an output file might look like:

```json
[
{ "key": "k1", "value": "v0", "offset": 1232155, "timestamp":"2020-01-01T00:00:01Z" },
{ "key": "k2", "value": "v1", "offset": 1232156, "timestamp":"2020-01-01T00:00:05Z" }
]
```

OR

```json
[
{ "key": "user1", "value": {"name": "John", "address": {"city": "London"}}, "offset": 1232155, "timestamp":"2020-01-01T00:00:01Z" }
]
```

It is recommended to use
- `org.apache.kafka.connect.storage.StringConverter`,
- `org.apache.kafka.connect.json.JsonConverter`, or
- `io.confluent.connect.avro.AvroConverter`.

as `key.converter` and/or `value.converter` to make output files human-readable.

**NB!**

- The value of the `format.output.fields.value.encoding` property is ignored for this data format.
- Value/Key schema will not be presented in output file, even if `value.converter.schemas.enable` property is `true`.
  But, it is still important to set this property correctly, so that connector could read records correctly.

##### NB!

For both JSON and JSONL another example could be for a single field output e.g. `value`, a record line might look like:

```json
{ "value": "v0" }
```

OR

```json
{ "value": {"name": "John", "address": {"city": "London"}} }
```

In this case it sometimes make sense to get rid of additional JSON object wrapping the actual value using `format.output.envelope`.
Having `format.output.envelope=false` can produce the following output:

```json
"v0"
```

OR

```json
{"name": "John", "address": {"city": "London"}}
```

#### Parquet format example

For example, if we output `key,offset,timestamp,headers,value`, an output Parquet schema might look like this:
```json
{
    "type": "record", "fields": [
      {"name": "key", "type": "RecordKeySchema"},
      {"name": "offset", "type": "long"},
      {"name": "timestamp", "type": "long"},
      {"name": "headers", "type": "map"},
      {"name": "value", "type": "RecordValueSchema"}
  ]
}
```
where `RecordKeySchema` - a key schema and `RecordValueSchema` - a record value schema.
This means that in case you have the record and key schema like:

Key schema:
```json
{
  "type": "string"
}
```

Record schema:
```json
{
    "type": "record", "fields": [
      {"name": "foo", "type": "string"},
      {"name": "bar", "type": "long"}
  ]
}
```
the final `Avro` schema for `Parquet` is:
```json
{
    "type": "record", "fields": [
      {"name": "key", "type": "string"},
      {"name": "offset", "type": "long"},
      {"name": "timestamp", "type": "long"},
      {"name": "headers", "type": "map", "values": "long"},
      { "name": "value",
        "type": "record",
        "fields": [
          {"name": "foo", "type": "string"},
          {"name": "bar", "type": "long"}
        ]
      }
  ]
}
```


For a single-field output e.g. `value`, a record line might look like:

```json
{ "value": {"name": "John", "address": {"city": "London"}} }
```

In this case it sometimes make sense to get rid of additional JSON object wrapping the actual value using `format.output.envelope`.
Having `format.output.envelope=false` can produce the following output:

```json
{"name": "John", "address": {"city": "London"}}
```

**NB!**
- The value of the `format.output.fields.value.encoding` property is ignored for this data format.
- Due to Avro limitation message headers values must be the same datatype
- If you use `org.apache.kafka.connect.json.JsonConverter` be sure that you message contains schema. E.g. possible `JSON` message:
    ```json
    {
      "schema": {
        "type": "struct",
        "fields": [
          {"type":"string", "field": "name"}
        ]
      }, "payload": {"name":  "foo"}
    }
    ```
- Connector works just fine with and without Schema Registry
- `format.output.envelope=false` is ignored if the value is not of type `org.apache.avro.Schema.Type.RECORD` or `org.apache.avro.Schema.Type.MAP`.

#### Avro format example

The output file is an [Avro Object Container File](https://avro.apache.org/docs/current/specification/#object-container-files).

For example, if we output `key,offset,timestamp,headers,value`, an output Avro schema might look like this:
```json
{
    "type": "record", "fields": [
      {"name": "key", "type": "RecordKeySchema"},
      {"name": "offset", "type": "long"},
      {"name": "timestamp", "type": "long"},
      {"name": "headers", "type": "map"},
      {"name": "value", "type": "RecordValueSchema"}
  ]
}
```
where `RecordKeySchema` - a key schema and `RecordValueSchema` - a record value schema.
This means that in case you have the record and key schema like:

Key schema:
```json
{
  "type": "string"
}
```

Record schema:
```json
{
    "type": "record", "fields": [
      {"name": "foo", "type": "string"},
      {"name": "bar", "type": "long"}
  ]
}
```
the final `Avro` schema for output is:
```json
{
    "type": "record", "fields": [
      {"name": "key", "type": "string"},
      {"name": "offset", "type": "long"},
      {"name": "timestamp", "type": "long"},
      {"name": "headers", "type": "map", "values": "long"},
      { "name": "value",
        "type": "record",
        "fields": [
          {"name": "foo", "type": "string"},
          {"name": "bar", "type": "long"}
        ]
      }
  ]
}
```


For a single-field output e.g. `value`, a record line might look like:

```json
{ "value": {"name": "John", "address": {"city": "London"}} }
```

In this case it sometimes make sense to get rid of additional object wrapping the actual value using `format.output.envelope`.
Having `format.output.envelope=false` can produce the following output:

```json
{"name": "John", "address": {"city": "London"}}
```

**NB!**
- The value of the `format.output.fields.value.encoding` property is ignored for this data format.
- Due to Avro limitation message headers values must be the same datatype
- Connector works just fine with and without Schema Registry
- `format.output.envelope=false` is ignored if the value is not of type `org.apache.avro.Schema.Type.RECORD` or `org.apache.avro.Schema.Type.MAP`.
- The Avro Object Container File requires that each value is written with the same schema in the file. When schema evolution happens for the input data, a new output file is created on every schema change. When data with previous and new schema is interleaved in the source topic multiple files will get generated in short duration.
- The schema for output file is derived from the Connect Schema. The Connect Schema is derived from the input records Avro schema by using the Schema Registry.


## Retry strategy configuration properties


### Apache Kafka connect retry strategy properties

- `kafka.retry.backoff.ms` - The retry backoff in milliseconds. This config is used to notify Apache Kafka Connect to retry delivering a message batch or
  performing recovery in case of transient exceptions. Maximum value is `24` hours.

### Azure Blob Storage Retry Strategy
- `azure.retry.backoff.initial.delay.ms` - Initial retry delay in milliseconds.
  This config controls the delay before the first retry.
  The default value is `1000 ms`.
- `azure.retry.backoff.max.delay.ms` - Maximum retry delay in milliseconds.
  This config puts a limit on the value of the retry delay.
  The default value is `32 000` ms.
- `azure.retry.backoff.max.attempts` - Retry max attempts.
  This config defines the maximum number of attempts to perform.
  The default value is `6`.

## Configuration

[Here](https://kafka.apache.org/documentation/#connect_running) you can
read about the Connect workers configuration and
[here](https://kafka.apache.org/documentation/#sinkconnectconfigs), about
the connector Configuration.

Here is an example connector configuration with descriptions:

```properties
### Standard connector configuration

## Fill in your values in these:

# Unique name for the connector.
# Attempting to register again with the same name will fail.
name=my-azure-sink-connector

## These must have exactly these values:

# The Java class for the connector
connector.class=io.aiven.kafka.connect.azure.sink.AzureBlobSinkConnector

# The key converter for this connector
key.converter=org.apache.kafka.connect.storage.StringConverter

# The value converter for this connector
value.converter=org.apache.kafka.connect.json.JsonConverter

# Identify, if value contains a schema.
# Required value converter is `org.apache.kafka.connect.json.JsonConverter`.
value.converter.schemas.enable=false

# The type of data format used to write data to the Azure Blob Storage output files.
# The supported values are: `csv`, `json`, `jsonl` and `parquet`.
# Optional, the default is `csv`.
format.output.type=jsonl

# A comma-separated list of topics to use as input for this connector
# Also a regular expression version `topics.regex` is supported.
# See https://kafka.apache.org/documentation/#connect_configuring
topics=topic1,topic2

### Connector-specific configuration
### Fill in you values

# The name of the Azure Blob Storage container to use
# Required.
azure.storage.container.name=my-container

## The following option is used to specify Azure Storage connection string.
## See the overview of Azure Storage authentication:
##  - https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string#configure-a-connection-string-for-an-azure-storage-account
## If none are present, the connector will throw an error.

# Azure Storage connection string.
# Required.
azure.storage.connection.string=DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey;EndpointSuffix=core.windows.net


# The set of the fields that are to be output, comma separated.
# Supported values are: `key`, `value`, `offset`, `timestamp`, and `headers`.
# Optional, the default is `value`.
format.output.fields=key,value,offset,timestamp,headers

# The option to enable/disable wrapping of plain values into additional JSON object(aka envelope)
# Optional, the default value is `true`.
format.output.envelope=true

# The prefix to be added to the name of each file put on Azure Blob Storage.
# See the Azure Blob Storage naming requirements https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blobs-introduction
# Optional, the default is empty.
file.name.prefix=some-prefix/

# The compression type used for files put on Azure Blob Storage.
# The supported values are: `gzip`, `snappy`, `zstd`, `none`.
# Optional, the default is `none`.
file.compression.type=gzip

# The compression used for Avro Container File blocks.
# The supported values are: `bzip2`, `deflate`, `null`, `snappy`, `zstandard`.
# Optional, the default is `null`.
avro.codec=null

# The time zone in which timestamps are represented.
# Accepts short and long standard names like: `UTC`, `PST`, `ECT`,
# `Europe/Berlin`, `Europe/Helsinki`, or `America/New_York`.
# For more information please refer to https://docs.oracle.com/javase/tutorial/datetime/iso/timezones.html.
# The default is `UTC`.
file.name.timestamp.timezone=UTC

# The source of timestamps.
# Supports only `wallclock` which is the default value.
file.name.timestamp.source=wallclock

# The file name template.
# See "File name format" section.
# Optional, the default is `{{topic}}-{{partition:padding=false}}-{{start_offset:padding=false}}` or
# `{{topic}}-{{partition:padding=false}}-{{start_offset:padding=false}}.gz` if the compression is enabled.
file.name.template={{topic}}-{{partition:padding=true}}-{{start_offset:padding=true}}.gz
```

## Getting releases

The connector releases are available in the Releases section.

Release JARs are available in Maven Central:

```xml
<dependency>
  <groupId>io.aiven</groupId>
  <artifactId>azure-blob-sink-connector-for-apache-kafka</artifactId>
  <version>x.y.z</version>
</dependency>
```

## Development

### Integration testing

Integration tests are implemented using JUnit, Gradle and Docker.

To run them, you need:
- a Azure bucket with the read-write permissions;
- Docker installed.

In order to run the integration tests, execute from the project root
directory:

```bash
./gradlew clean integrationTest -PtestAzureStorage=test-storage-name
```

where `PtestAzureStorage` is the name of the Azure Storage to use.

The default Azure credentials will be used during the test [see the Azure documentation](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string#configure-a-connection-string-for-an-azure-storage-account).

To specify the Azure Storage Connection string, use `testAzureStorageString` property:

```bash
./gradlew clean integrationTest -PtestAzureStorage=test-storage-name -PtestAzureStorageString=AzureStorageString
```

Gradle allows setting properties using environment variables, for
example, `ORG_GRADLE_PROJECT_testAzureBucket=test-bucket-name`. See more
about the ways to set properties
[here](https://docs.gradle.org/current/userguide/build_environment.html#sec:project_properties).

### Releasing

TBD

## License

This project is licensed under the [Apache License, Version 2.0](../LICENSE).

## Trademarks

Apache Kafka, Apache Kafka Connect are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries. Azure Blob Storage (Azure) is a trademark and property of their respective owners. All product and service names used in this website are for identification purposes only and do not imply endorsement.
