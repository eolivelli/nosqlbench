package io.nosqlbench.driver.pulsar.ops;

import io.nosqlbench.driver.pulsar.PulsarSpace;
import io.nosqlbench.engine.api.templating.CommandTemplate;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;

import java.util.function.LongFunction;

public abstract class PulsarOpMapper implements LongFunction<PulsarOp> {
    protected final CommandTemplate cmdTpl;
    protected final PulsarSpace clientSpace;

    public PulsarOpMapper(CommandTemplate cmdTpl,
                          PulsarSpace clientSpace) {
        this.cmdTpl = cmdTpl;
        this.clientSpace = clientSpace;
    }
}
