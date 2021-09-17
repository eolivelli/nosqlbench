package com.datastax.ebdrivers.kafkaproducer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.nosqlbench.engine.api.activityconfig.yaml.OpTemplate;
import io.nosqlbench.virtdata.core.bindings.BindingsTemplate;
import io.nosqlbench.virtdata.core.templates.ParsedTemplate;
import io.nosqlbench.virtdata.core.templates.StringBindings;
import io.nosqlbench.virtdata.core.templates.StringBindingsTemplate;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaStatement {
    private final static Logger logger = LogManager.getLogger(KafkaStatement.class);

    private Producer<Object,Object> producer = null;
    private final StringBindings bindings;
    private final String topic;
    private final String keySerializerClass;
    private final String valueSerializerClass;
    private AvroSchema keySerializerSchema = null;
    private AvroSchema valueSerializerSchema = null;
    private final String key;
    private final boolean kafkaAsyncOp;
    private final Counter bytesCounter;
    private final Histogram messagesizeHistogram;

    private volatile Throwable lastAsyncOperationFailure;

    public KafkaStatement(OpTemplate stmtDef, String servers, String clientId, String schemaRegistryUrl, boolean kafkaAsyncOp,
                          Counter bytesCounter, Histogram messagesizeHistogram) {
        ParsedTemplate paramTemplate = new ParsedTemplate(stmtDef.getStmt(), stmtDef.getBindings());
        BindingsTemplate paramBindings = new BindingsTemplate(paramTemplate.getBindPoints());
        StringBindingsTemplate template = new StringBindingsTemplate(stmtDef.getStmt(), paramBindings);
        this.kafkaAsyncOp = kafkaAsyncOp;
        this.bytesCounter = bytesCounter;
        this.messagesizeHistogram = messagesizeHistogram;

        this.bindings = template.resolve();

        // Process key serializer class and schema, if any
        this.keySerializerClass =
            stmtDef.getOptionalStringParam("key_serializer_class")
                .orElse(StringSerializer.class.getName());

        Optional<String> keySerializerSchemaFile =
            stmtDef.getOptionalStringParam("key_serializer_schema_file");

        if (keySerializerClass.equals("io.confluent.kafka.serializers.KafkaAvroSerializer")
            && keySerializerSchemaFile.isEmpty() ) {
            throw new RuntimeException("KafkaAvroSerializer requires key_serializer_schema_file");
        }

        if (keySerializerSchemaFile.isPresent()) {
            Path schemaFilePath = Path.of(keySerializerSchemaFile.get());
            try {
                this.keySerializerSchema = new AvroSchema(Files.readString(schemaFilePath));
            } catch (IOException e) {
                throw new RuntimeException("Error reading key schema file: " + keySerializerSchemaFile, e);
            }
        }

        // Process value serializer class and schema, if any
        this.valueSerializerClass =
            stmtDef.getOptionalStringParam("value_serializer_class")
                .orElse(StringSerializer.class.getName());

        Optional<String> valueSerializerSchemaFile =
            stmtDef.getOptionalStringParam("value_serializer_schema_file");

        if (valueSerializerClass.equals("io.confluent.kafka.serializers.KafkaAvroSerializer")
            && valueSerializerSchemaFile.isEmpty() ) {
            throw new RuntimeException("KafkaAvroSerializer requires value_serializer_schema_file");
        }

        if (valueSerializerSchemaFile.isPresent()) {
            Path schemaFilePath = Path.of(valueSerializerSchemaFile.get());
            try {
                this.valueSerializerSchema = new AvroSchema(Files.readString(schemaFilePath));
            } catch (IOException e) {
                throw new RuntimeException("Error reading value schema file: " + valueSerializerSchemaFile, e);
            }
        }

        this.topic = stmtDef.getParamOrDefault("topic","default-topic");
        this.key = stmtDef.getOptionalStringParam("key").orElse("key");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,  servers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass);
        props.put("schema.registry.url", schemaRegistryUrl);

        try {
            producer = new KafkaProducer<>(props);
        } catch (Exception e) {
            throw new RuntimeException("Error constructing kafka producer", e);
        }
    }

    private Object bindKey(long cycle) {
        Object statement = key;
        if (keySerializerClass != null &&
            keySerializerClass.equals("io.confluent.kafka.serializers.KafkaAvroSerializer")) {
            try {
                statement = AvroSchemaUtils.toObject((String)statement, keySerializerSchema);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return statement;
    }

    private Object bindValue(long cycle) {
        Object statement = bindings.bind(cycle);
        if (valueSerializerClass != null &&
            valueSerializerClass.equals("io.confluent.kafka.serializers.KafkaAvroSerializer")) {
            try {
                statement = AvroSchemaUtils.toObject((String)statement, valueSerializerSchema);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return statement;
    }

    public void write(long cycle, Runnable timeTracker) {
        if (lastAsyncOperationFailure != null) {
            throw new RuntimeException("Some operations failed", lastAsyncOperationFailure);
        }

        Object key = bindKey(cycle);
        Object value = bindValue(cycle);
        ProducerRecord<Object, Object> record = new ProducerRecord<>(topic, key, value);
        if (kafkaAsyncOp) {
            // we rely on max.block.ms in order to throttle the request in this case
            // the same happens in PulsarProducerOp
            producer.send(record, (metadata, exception) -> {
                timeTracker.run();
                if (exception != null) {
                    lastAsyncOperationFailure = exception;
                } else {
                    int messagesize = metadata.serializedKeySize() + metadata.serializedValueSize();
                    messagesizeHistogram.update(messagesize);
                    bytesCounter.inc(messagesize);
                }
            });
        } else {
            try {
                Future<RecordMetadata> send = producer.send(record);
                send.get();
                timeTracker.run();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
