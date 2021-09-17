package com.datastax.ebdrivers.kafkaproducer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import io.nosqlbench.engine.api.activityapi.errorhandling.modular.NBErrorHandler;
import io.nosqlbench.engine.api.activityapi.planning.OpSequence;
import io.nosqlbench.engine.api.activityapi.planning.SequencePlanner;
import io.nosqlbench.engine.api.activityapi.planning.SequencerType;
import io.nosqlbench.engine.api.activityconfig.StatementsLoader;
import io.nosqlbench.engine.api.activityconfig.yaml.OpTemplate;
import io.nosqlbench.engine.api.activityconfig.yaml.StmtsDocList;
import io.nosqlbench.engine.api.activityimpl.ActivityDef;
import io.nosqlbench.engine.api.activityimpl.SimpleActivity;
import io.nosqlbench.engine.api.metrics.ActivityMetrics;
import io.nosqlbench.engine.api.templating.StrInterpolator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class KafkaProducerActivity extends SimpleActivity {
    private final static Logger logger = LogManager.getLogger(KafkaProducerActivity.class);
    private String yamlLoc;
    private String clientId;
    private String servers;
    private String schemaRegistryUrl;
    private OpSequence<KafkaStatement> opSequence;
    private boolean kafkaAsyncOp;
    Timer resultTimer;
    Timer resultSuccessTimer;
    Timer bindTimer;
    Timer executeTimer;
    Counter bytesCounter;
    Histogram messagesizeHistogram;
    private NBErrorHandler errorhandler;


    public KafkaProducerActivity(ActivityDef activityDef) {
        super(activityDef);
    }

    @Override
    public synchronized void onActivityDefUpdate(ActivityDef activityDef) {
        super.onActivityDefUpdate(activityDef);

        // sanity check
        yamlLoc = activityDef.getParams().getOptionalString("yaml", "workload")
            .orElseThrow(() -> new IllegalArgumentException("yaml is not defined"));
        servers = Arrays.stream(activityDef.getParams().getOptionalString("host","hosts")
            .orElse("localhost" + ":9092")
            .split(","))
            .map(x ->  x.indexOf(':') == -1 ? x + ":9092" : x)
            .collect(Collectors.joining(","));
        clientId = activityDef.getParams().getOptionalString("clientid","client.id","client_id")
            .orElse("TestProducerClientId");
        schemaRegistryUrl = activityDef.getParams()
            .getOptionalString("schema_registry_url", "schema.registry.url")
            .orElse("http://localhost:8081");
        kafkaAsyncOp = activityDef.getParams()
            .getOptionalBoolean("async_api")
            .orElse(false);
        logger.info("Activity params {}", activityDef.getParams());
    }

    @Override
    public void initActivity() {
        logger.debug("initializing activity: " + this.activityDef.getAlias());
        onActivityDefUpdate(activityDef);

        resultTimer = ActivityMetrics.timer(activityDef, "result");
        resultSuccessTimer = ActivityMetrics.timer(activityDef, "result-success");

        bindTimer = ActivityMetrics.timer(activityDef, "bind");
        executeTimer = ActivityMetrics.timer(activityDef, "execute");

        bytesCounter = ActivityMetrics.counter(activityDef, "bytes");
        messagesizeHistogram = ActivityMetrics.histogram(activityDef, "messagesize");

        this.errorhandler = new NBErrorHandler(
            () -> activityDef.getParams().getOptionalString("errors").orElse("stop"),
            this::getExceptionMetrics
        );

        opSequence = initOpSequencer();
        setDefaultsFromOpSequence(opSequence);

    }

    private OpSequence<KafkaStatement> initOpSequencer() {
        SequencerType sequencerType = SequencerType.valueOf(
            getParams().getOptionalString("seq").orElse("bucket")
        );
        SequencePlanner<KafkaStatement> sequencer = new SequencePlanner<>(sequencerType);

        String tagFilter = activityDef.getParams().getOptionalString("tags").orElse("");
        StmtsDocList stmtsDocList = StatementsLoader.loadPath(logger, yamlLoc, new StrInterpolator(activityDef),
            "activities");
        List<OpTemplate> statements = stmtsDocList.getStmts(tagFilter);
        logger.info("async_api {}", kafkaAsyncOp);

        String format = getParams().getOptionalString("format").orElse(null);

        if (statements.size() > 0) {
            for (OpTemplate statement : statements) {
                sequencer.addOp(
                    new KafkaStatement(statement,
                                       servers,
                                       clientId,
                                       schemaRegistryUrl,
                                       kafkaAsyncOp,
                                       bytesCounter,
                                       messagesizeHistogram),
                    statement.getParamOrDefault("ratio", 1)
                );
            }
        } else {
            logger.error("Unable to create a Kafka statement if you have no active statements.");
        }

        return sequencer.resolve();
    }

    protected OpSequence<KafkaStatement> getOpSequencer() {
        return opSequence;
    }

    public Timer getBindTimer() {
        return bindTimer;
    }

    public Timer getExecuteTimer() {
        return executeTimer;
    }

    public NBErrorHandler getErrorhandler() {
        return errorhandler;
    }
}
