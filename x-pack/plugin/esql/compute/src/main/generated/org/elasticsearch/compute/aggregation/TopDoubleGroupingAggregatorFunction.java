// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation;

import java.lang.Integer;
import java.lang.Override;
import java.lang.String;
import java.lang.StringBuilder;
import java.util.List;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link TopDoubleAggregator}.
 * This class is generated. Edit {@code GroupingAggregatorImplementer} instead.
 */
public final class TopDoubleGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("top", ElementType.DOUBLE)  );

  private final TopDoubleAggregator.GroupingState state;

  private final List<Integer> channels;

  private final DriverContext driverContext;

  private final int limit;

  private final boolean ascending;

  public TopDoubleGroupingAggregatorFunction(List<Integer> channels,
      TopDoubleAggregator.GroupingState state, DriverContext driverContext, int limit,
      boolean ascending) {
    this.channels = channels;
    this.state = state;
    this.driverContext = driverContext;
    this.limit = limit;
    this.ascending = ascending;
  }

  public static TopDoubleGroupingAggregatorFunction create(List<Integer> channels,
      DriverContext driverContext, int limit, boolean ascending) {
    return new TopDoubleGroupingAggregatorFunction(channels, TopDoubleAggregator.initGrouping(driverContext.bigArrays(), limit, ascending), driverContext, limit, ascending);
  }

  public static List<IntermediateStateDesc> intermediateStateDesc() {
    return INTERMEDIATE_STATE_DESC;
  }

  @Override
  public int intermediateBlockCount() {
    return INTERMEDIATE_STATE_DESC.size();
  }

  @Override
  public GroupingAggregatorFunction.AddInput prepareProcessRawInputPage(SeenGroupIds seenGroupIds,
      Page page) {
    DoubleBlock valuesBlock = page.getBlock(channels.get(0));
    DoubleVector valuesVector = valuesBlock.asVector();
    if (valuesVector == null) {
      if (valuesBlock.mayHaveNulls()) {
        state.enableGroupIdTracking(seenGroupIds);
      }
      return new GroupingAggregatorFunction.AddInput() {
        @Override
        public void add(int positionOffset, IntArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, valuesBlock);
        }

        @Override
        public void add(int positionOffset, IntBigArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, valuesBlock);
        }

        @Override
        public void add(int positionOffset, IntVector groupIds) {
          addRawInput(positionOffset, groupIds, valuesBlock);
        }

        @Override
        public void close() {
        }
      };
    }
    return new GroupingAggregatorFunction.AddInput() {
      @Override
      public void add(int positionOffset, IntArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, valuesVector);
      }

      @Override
      public void add(int positionOffset, IntBigArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, valuesVector);
      }

      @Override
      public void add(int positionOffset, IntVector groupIds) {
        addRawInput(positionOffset, groupIds, valuesVector);
      }

      @Override
      public void close() {
      }
    };
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, DoubleBlock values) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition) || values.isNull(groupPosition + positionOffset)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valuesStart = values.getFirstValueIndex(groupPosition + positionOffset);
        int valuesEnd = valuesStart + values.getValueCount(groupPosition + positionOffset);
        for (int v = valuesStart; v < valuesEnd; v++) {
          TopDoubleAggregator.combine(state, groupId, values.getDouble(v));
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, DoubleVector values) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        TopDoubleAggregator.combine(state, groupId, values.getDouble(groupPosition + positionOffset));
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block topUncast = page.getBlock(channels.get(0));
    if (topUncast.areAllValuesNull()) {
      return;
    }
    DoubleBlock top = (DoubleBlock) topUncast;
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        TopDoubleAggregator.combineIntermediate(state, groupId, top, groupPosition + positionOffset);
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, DoubleBlock values) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition) || values.isNull(groupPosition + positionOffset)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int valuesStart = values.getFirstValueIndex(groupPosition + positionOffset);
        int valuesEnd = valuesStart + values.getValueCount(groupPosition + positionOffset);
        for (int v = valuesStart; v < valuesEnd; v++) {
          TopDoubleAggregator.combine(state, groupId, values.getDouble(v));
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, DoubleVector values) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        TopDoubleAggregator.combine(state, groupId, values.getDouble(groupPosition + positionOffset));
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block topUncast = page.getBlock(channels.get(0));
    if (topUncast.areAllValuesNull()) {
      return;
    }
    DoubleBlock top = (DoubleBlock) topUncast;
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        TopDoubleAggregator.combineIntermediate(state, groupId, top, groupPosition + positionOffset);
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, DoubleBlock values) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (values.isNull(groupPosition + positionOffset)) {
        continue;
      }
      int groupId = groups.getInt(groupPosition);
      int valuesStart = values.getFirstValueIndex(groupPosition + positionOffset);
      int valuesEnd = valuesStart + values.getValueCount(groupPosition + positionOffset);
      for (int v = valuesStart; v < valuesEnd; v++) {
        TopDoubleAggregator.combine(state, groupId, values.getDouble(v));
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, DoubleVector values) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = groups.getInt(groupPosition);
      TopDoubleAggregator.combine(state, groupId, values.getDouble(groupPosition + positionOffset));
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert channels.size() == intermediateBlockCount();
    Block topUncast = page.getBlock(channels.get(0));
    if (topUncast.areAllValuesNull()) {
      return;
    }
    DoubleBlock top = (DoubleBlock) topUncast;
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int groupId = groups.getInt(groupPosition);
      TopDoubleAggregator.combineIntermediate(state, groupId, top, groupPosition + positionOffset);
    }
  }

  @Override
  public void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds) {
    state.enableGroupIdTracking(seenGroupIds);
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, IntVector selected) {
    state.toIntermediate(blocks, offset, selected, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, IntVector selected,
      GroupingAggregatorEvaluationContext ctx) {
    blocks[offset] = TopDoubleAggregator.evaluateFinal(state, selected, ctx);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName()).append("[");
    sb.append("channels=").append(channels);
    sb.append("]");
    return sb.toString();
  }

  @Override
  public void close() {
    state.close();
  }
}
