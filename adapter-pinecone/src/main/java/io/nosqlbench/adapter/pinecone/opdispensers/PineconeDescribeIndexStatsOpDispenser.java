/*
 * Copyright (c) 2022 nosqlbench
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.nosqlbench.adapter.pinecone.opdispensers;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.nosqlbench.adapter.pinecone.PineconeDriverAdapter;
import io.nosqlbench.adapter.pinecone.PineconeSpace;
import io.nosqlbench.adapter.pinecone.ops.PineconeDescribeIndexStatsOp;
import io.nosqlbench.adapter.pinecone.ops.PineconeOp;
import io.nosqlbench.engine.api.templating.ParsedOp;
import io.pinecone.proto.DescribeIndexStatsRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;
import java.util.function.LongFunction;

public class PineconeDescribeIndexStatsOpDispenser extends PineconeOpDispenser {
    private static final Logger LOGGER = LogManager.getLogger(PineconeDescribeIndexStatsOpDispenser.class);
    private final LongFunction<DescribeIndexStatsRequest> indexStatsRequestFunc;

    /**
     * Create a new PineconeDescribeIndexStatsOpDispenser subclassed from {@link PineconeOpDispenser}.
     *
     * @param adapter           The associated {@link PineconeDriverAdapter}
     * @param op                The {@link ParsedOp} encapsulating the activity for this cycle
     * @param pcFunction        A function to return the associated context of this activity (see {@link PineconeSpace})
     * @param targetFunction    A LongFunction that returns the specified Pinecone Index for this Op
     */
    public PineconeDescribeIndexStatsOpDispenser(PineconeDriverAdapter adapter,
                                                 ParsedOp op,
                                                 LongFunction<PineconeSpace> pcFunction,
                                                 LongFunction<String> targetFunction) {
        super(adapter, op, pcFunction, targetFunction);
        indexStatsRequestFunc = createDescribeIndexStatsRequestFunction(op);
    }

    private LongFunction<DescribeIndexStatsRequest> createDescribeIndexStatsRequestFunction(ParsedOp op) {
        LongFunction<DescribeIndexStatsRequest.Builder> rFunc = l -> DescribeIndexStatsRequest.newBuilder();
        Optional<LongFunction<String>> filterFunction = op.getAsOptionalFunction("filter", String.class);
        if (filterFunction.isPresent()) {
            LongFunction<DescribeIndexStatsRequest.Builder> finalFunc = rFunc;
            LongFunction<Struct> builtFilter = l -> {
                String[] filterFields = filterFunction.get().apply(l).split(" ");
                return Struct.newBuilder().putFields(filterFields[0],
                        Value.newBuilder().setStructValue(Struct.newBuilder().putFields(filterFields[1],
                                        Value.newBuilder().setNumberValue(Integer.parseInt(filterFields[2])).build()))
                                .build()).build();
            };
            rFunc = l -> finalFunc.apply(l).setFilter(builtFilter.apply(l));
        }
        LongFunction<DescribeIndexStatsRequest.Builder> finalRFunc = rFunc;
        return l -> finalRFunc.apply(l).build();
    }

    @Override
    public PineconeOp apply(long value) {
        return new PineconeDescribeIndexStatsOp(pcFunction.apply(value).getConnection(targetFunction.apply(value)),
            indexStatsRequestFunc.apply(value));
    }
}
