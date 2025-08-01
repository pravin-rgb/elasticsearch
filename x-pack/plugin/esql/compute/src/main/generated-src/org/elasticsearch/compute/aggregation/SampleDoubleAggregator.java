/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

// begin generated imports
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.sort.BytesRefBucketedSort;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.topn.DefaultUnsortableTopNEncoder;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.sort.SortOrder;

import org.elasticsearch.common.Randomness;
import java.util.random.RandomGenerator;
// end generated imports

/**
 * Sample N field values for double.
 * <p>
 *     This class is generated. Edit `X-SampleAggregator.java.st` to edit this file.
 * </p>
 * <p>
 *     This works by prepending a random long to the value, and then collecting the
 *     top values. This gives a uniform random sample of the values. See also:
 *     <a href="https://en.wikipedia.org/wiki/Reservoir_sampling#With_random_sort">Wikipedia Reservoir Sampling</a>
 * </p>
 */
@Aggregator({ @IntermediateState(name = "sample", type = "BYTES_REF_BLOCK") })
@GroupingAggregator
class SampleDoubleAggregator {
    private static final DefaultUnsortableTopNEncoder ENCODER = new DefaultUnsortableTopNEncoder();

    public static SingleState initSingle(BigArrays bigArrays, int limit) {
        return new SingleState(bigArrays, limit);
    }

    public static void combine(SingleState state, double value) {
        state.add(value);
    }

    public static void combineIntermediate(SingleState state, BytesRefBlock values) {
        int start = values.getFirstValueIndex(0);
        int end = start + values.getValueCount(0);
        BytesRef scratch = new BytesRef();
        for (int i = start; i < end; i++) {
            state.internalState.sort.collect(values.getBytesRef(i, scratch), 0);
        }
    }

    public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
        return stripWeights(driverContext, state.toBlock(driverContext.blockFactory()));
    }

    public static GroupingState initGrouping(BigArrays bigArrays, int limit) {
        return new GroupingState(bigArrays, limit);
    }

    public static void combine(GroupingState state, int groupId, double value) {
        state.add(groupId, value);
    }

    public static void combineIntermediate(GroupingState state, int groupId, BytesRefBlock values, int valuesPosition) {
        int start = values.getFirstValueIndex(valuesPosition);
        int end = start + values.getValueCount(valuesPosition);
        BytesRef scratch = new BytesRef();
        for (int i = start; i < end; i++) {
            state.sort.collect(values.getBytesRef(i, scratch), groupId);
        }
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        return stripWeights(ctx.driverContext(), state.toBlock(ctx.blockFactory(), selected));
    }

    private static Block stripWeights(DriverContext driverContext, Block block) {
        if (block.areAllValuesNull()) {
            return block;
        }
        try (
            BytesRefBlock bytesRefBlock = (BytesRefBlock) block;
            DoubleBlock.Builder doubleBlock = driverContext.blockFactory().newDoubleBlockBuilder(bytesRefBlock.getPositionCount())
        ) {
            BytesRef scratch = new BytesRef();
            for (int position = 0; position < block.getPositionCount(); position++) {
                if (bytesRefBlock.isNull(position)) {
                    doubleBlock.appendNull();
                } else {
                    int valueCount = bytesRefBlock.getValueCount(position);
                    if (valueCount > 1) {
                        doubleBlock.beginPositionEntry();
                    }
                    int start = bytesRefBlock.getFirstValueIndex(position);
                    int end = start + valueCount;
                    for (int i = start; i < end; i++) {
                        BytesRef value = bytesRefBlock.getBytesRef(i, scratch).clone();
                        ENCODER.decodeLong(value);
                        doubleBlock.appendDouble(ENCODER.decodeDouble(value));
                    }
                    if (valueCount > 1) {
                        doubleBlock.endPositionEntry();
                    }
                }
            }
            return doubleBlock.build();
        }
    }

    public static class GroupingState implements GroupingAggregatorState {
        private final BytesRefBucketedSort sort;
        private final BreakingBytesRefBuilder bytesRefBuilder;

        private GroupingState(BigArrays bigArrays, int limit) {
            CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
            this.sort = new BytesRefBucketedSort(breaker, "sample", bigArrays, SortOrder.ASC, limit);
            boolean success = false;
            try {
                this.bytesRefBuilder = new BreakingBytesRefBuilder(breaker, "sample");
                success = true;
            } finally {
                if (success == false) {
                    Releasables.closeExpectNoException(sort);
                }
            }
        }

        public void add(int groupId, double value) {
            ENCODER.encodeLong(Randomness.get().nextLong(), bytesRefBuilder);
            ENCODER.encodeDouble(value, bytesRefBuilder);
            sort.collect(bytesRefBuilder.bytesRefView(), groupId);
            bytesRefBuilder.clear();
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext.blockFactory(), selected);
        }

        Block toBlock(BlockFactory blockFactory, IntVector selected) {
            return sort.toBlock(blockFactory, selected);
        }

        @Override
        public void enableGroupIdTracking(SeenGroupIds seen) {
            // we figure out seen values from nulls on the values block
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(sort, bytesRefBuilder);
        }
    }

    public static class SingleState implements AggregatorState {
        private final GroupingState internalState;

        private SingleState(BigArrays bigArrays, int limit) {
            this.internalState = new GroupingState(bigArrays, limit);
        }

        public void add(double value) {
            internalState.add(0, value);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext.blockFactory());
        }

        Block toBlock(BlockFactory blockFactory) {
            try (var intValues = blockFactory.newConstantIntVector(0, 1)) {
                return internalState.toBlock(blockFactory, intValues);
            }
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(internalState);
        }
    }
}
