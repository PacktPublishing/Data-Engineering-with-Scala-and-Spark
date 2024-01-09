# Databricks notebook source
# MAGIC %pip install dbldatagen

# COMMAND ----------

from pyspark.sql.types import LongType, IntegerType, StringType, StructField, StructType, TimestampType
import dbldatagen as dg

shuffle_partitions_requested = 8
device_population = 100000
data_rows = 20 * 1000000
partitions_requested = 20

spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

country_codes = ['CN', 'US', 'FR', 'CA', 'IN', 'JM', 'IE', 'PK', 'GB', 'IL', 'AU', 'SG',
                 'ES', 'GE', 'MX', 'ET', 'SA', 'LB', 'NL']
country_weights = [1300, 365, 67, 38, 1300, 3, 7, 212, 67, 9, 25, 6, 47, 83, 126, 109, 58, 8,
                   17]

manufacturers = ['Delta corp', 'Xyzzy Inc.', 'Lakehouse Ltd', 'Acme Corp', 'Embanks Devices']

lines = ['delta', 'xyzzy', 'lakehouse', 'gadget', 'droid']

keyval = ['device_state']
testDataSpec = (
    dg.DataGenerator(spark, name="device_data_set", rows=data_rows,
                     partitions=partitions_requested,
                     randomSeedMethod='hash_fieldname')
    .withColumn("internal_device_id", LongType(), minValue=0x1000000000000,
                uniqueValues=device_population, omit=True, baseColumnType="hash")
    .withColumn("device_id", StringType(), format="0x%013x",
                baseColumn="internal_device_id", omit=True)
    .withColumn("country", StringType(), values=country_codes,
                weights=country_weights,
                baseColumn="internal_device_id", omit=True)
    .withColumn("manufacturer", StringType(), values=manufacturers,
                baseColumn="internal_device_id", omit=True)
    .withColumn("line", StringType(), values=lines, baseColumn="manufacturer",
                baseColumnType="hash", omit=True)
    .withColumn("model_ser", IntegerType(), minValue=1, maxValue=11,
                baseColumn="device_id",
                baseColumnType="hash", omit=True)
    .withColumn("model_line", StringType(), expr="concat(line, '#', model_ser)",
                baseColumn=["line", "model_ser"], omit=True)
    .withColumn("event_type", StringType(),
                values=["activation", "deactivation", "plan change",
                        "telecoms activity", "internet activity", "device error"],
                random=True, omit=True)
    .withColumn("event_ts", "timestamp", expr="now()", omit=True)
    .withColumn("key", StringType(), values=keyval)
    .withColumn("prevalue",
                 StructType([StructField('device_id',StringType()),
                             StructField('country',StringType()),
                             StructField('event_type',StringType()),
                             StructField('event_ts', TimestampType())]),
                expr="named_struct('device_id', device_id, 'country', country, 'event_type', event_type, 'event_ts', event_ts)",
                baseColumn=['device_id', 'country', 'event_type', 'event_ts'], omit=True)
    .withColumn("value", StringType(), expr="to_json(prevalue)", baseColumn=['prevalue'])
    )

df = testDataSpec.build(withStreaming=True, options={'rowsPerSecond': 10})

display(df)

# COMMAND ----------

# run to here in a Databricks workspace, it will start the stream and keep it running
import pyspark.sql.functions as F

readConnectionString = "Endpoint=sb://erictome.servicebus.windows.net/;SharedAccessKeyName=policy1;SharedAccessKey=<yourkey>=;EntityPath=dewithscala"
topic_name = "dewithscala"
eh_namespace_name = "erictome"
eh_sasl = 'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule' \
    + f' required username="$ConnectionString" password="{readConnectionString}";'
bootstrap_servers = f"{eh_namespace_name}.servicebus.windows.net:9093"

(df.writeStream
    .format("kafka")
    .option("topic", topic_name)
    .option("kafka.bootstrap.servers", bootstrap_servers)
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", eh_sasl)
    .option("checkpointLocation", "./checkpoint")
    .start())


# COMMAND ----------

# run this cell to stop the stream
for x in spark.streams.active:
    try:
        x.stop()
    except RuntimeError:
        pass
