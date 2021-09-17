package com.datastax.ebdrivers.kafkaproducer;

import com.codahale.metrics.Timer;
import io.nosqlbench.engine.api.activityapi.core.SyncAction;
import io.nosqlbench.engine.api.activityapi.errorhandling.modular.ErrorDetail;
import io.nosqlbench.engine.api.activityapi.planning.OpSequence;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;


public class KafkaAction implements SyncAction {

    private final static Logger logger = LogManager.getLogger(KafkaAction.class);

    private final KafkaProducerActivity activity;
    private final int slot;
    private final int maxTries;

    private OpSequence<KafkaStatement> sequencer;

    public KafkaAction(KafkaProducerActivity activity, int slot) {
        this.activity = activity;
        this.slot = slot;
        this.maxTries = activity.getActivityDef().getParams().getOptionalInteger("maxtries").orElse(10);
    }

    @Override
    public void init() {
        this.sequencer = activity.getOpSequencer();
    }

    @Override
    public int runCycle(long cycle) {

        long start = System.nanoTime();

        KafkaStatement kafkaOp;
        try (Timer.Context ctx = activity.getBindTimer().time()) {
            kafkaOp = sequencer.get(cycle);
        } catch (Exception bindException) {
            // if diagnostic mode ...
            activity.getErrorhandler().handleError(bindException, cycle, 0);
            throw new RuntimeException(
                "while binding request in cycle " + cycle + ": " + bindException.getMessage(), bindException
            );
        }

        for (int i = 0; i < maxTries; i++) {
            Timer.Context ctx = activity.getExecuteTimer().time();
            try {
                // it is up to the kafkaOp to call Context#close when the activity is executed
                // this allows us to track time for async operations
                kafkaOp.write(cycle, ctx::close);
                break;
            } catch (RuntimeException err) {
                ErrorDetail errorDetail = activity
                    .getErrorhandler()
                    .handleError(err, cycle, System.nanoTime() - start);
                if (!errorDetail.isRetryable()) {
                    break;
                }
            }
        }
        return 1;
    }

}
