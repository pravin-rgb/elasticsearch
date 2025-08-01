/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.template;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.ingest.PutPipelineTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.Index;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ilm.DeleteAction;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.action.ILMActions;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleRequest;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.oneOf;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class IndexTemplateRegistryTests extends ESTestCase {

    private final ProjectId projectId = randomProjectIdOrDefault();

    private TestRegistryWithCustomPlugin registry;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private VerifyingClient client;

    @Before
    public void createRegistryAndClient() {
        threadPool = new TestThreadPool(this.getClass().getName());
        client = new VerifyingClient(threadPool);
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        registry = new TestRegistryWithCustomPlugin(Settings.EMPTY, clusterService, threadPool, client, NamedXContentRegistry.EMPTY);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testThatIndependentPipelinesAreAddedImmediately() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<ProjectId, AtomicInteger> calledTimesMap = new ConcurrentHashMap<>();
        client.setVerifier((projectId, action, request, listener) -> {
            if (action == PutPipelineTransportAction.TYPE) {
                final var calledTimes = calledTimesMap.computeIfAbsent(projectId, k -> new AtomicInteger(0));
                assertPutPipelineAction(calledTimes, action, request, listener, "custom-plugin-final_pipeline");
                return AcknowledgedResponse.TRUE;
            } else if (action == ILMActions.PUT) {
                // ignore lifecycle policies in this case
                return AcknowledgedResponse.TRUE;
            } else {
                // the composable template is not expected to be added, as it's dependency is not available in the cluster state
                // custom-plugin-settings.json is not expected to be added as it contains a dependency on the default_pipeline
                fail("client called with unexpected request: " + request.toString());
                return null;
            }
        });

        ClusterChangedEvent event = createClusterChangedEvent(Collections.emptyMap(), nodes);
        registry.clusterChanged(event);
        assertCalledTimes(calledTimesMap, event, 1);
    }

    public void testThatDependentPipelinesAreAddedIfDependenciesExist() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<ProjectId, AtomicInteger> calledTimesMap = new ConcurrentHashMap<>();
        client.setVerifier((projectId, action, request, listener) -> {
            if (action == PutPipelineTransportAction.TYPE) {
                final var calledTimes = calledTimesMap.computeIfAbsent(projectId, k -> new AtomicInteger(0));
                assertPutPipelineAction(calledTimes, action, request, listener, "custom-plugin-default_pipeline");
                return AcknowledgedResponse.TRUE;
            } else if (action == ILMActions.PUT) {
                // ignore lifecycle policies in this case
                return AcknowledgedResponse.TRUE;
            } else {
                // the composable template is not expected to be added, as it's dependency is not available in the cluster state
                // custom-plugin-settings.json is not expected to be added as it contains a dependency on the default_pipeline
                fail("client called with unexpected request: " + request.toString());
                return null;
            }
        });

        ClusterChangedEvent event = createClusterChangedEvent(
            Collections.emptyMap(),
            Collections.emptyMap(),
            Map.of("custom-plugin-final_pipeline", 3),
            nodes
        );
        registry.clusterChanged(event);
        assertCalledTimes(calledTimesMap, event, 1);
    }

    public void testThatTemplateIsAddedIfAllDependenciesExist() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<ProjectId, AtomicInteger> calledTimesMap = new ConcurrentHashMap<>();
        client.setVerifier((projectId, action, request, listener) -> {
            if (action instanceof PutComponentTemplateAction) {
                final var calledTimes = calledTimesMap.computeIfAbsent(projectId, k -> new AtomicInteger(0));
                assertPutComponentTemplate(calledTimes, action, request, listener);
                return AcknowledgedResponse.TRUE;
            } else if (action == ILMActions.PUT) {
                // ignore lifecycle policies in this case
                return AcknowledgedResponse.TRUE;
            } else {
                // the composable template is not expected to be added, as it's dependency is not available in the cluster state
                fail("client called with unexpected request: " + request.toString());
                return null;
            }
        });

        ClusterChangedEvent event = createClusterChangedEvent(
            Collections.emptyMap(),
            Collections.emptyMap(),
            Map.of("custom-plugin-default_pipeline", 3, "custom-plugin-final_pipeline", 3),
            nodes
        );
        registry.clusterChanged(event);
        assertCalledTimes(calledTimesMap, event, 1);
    }

    public void testThatTemplateIsNotAddedIfNotAllDependenciesExist() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<ProjectId, AtomicInteger> calledTimesMap = new ConcurrentHashMap<>();
        client.setVerifier((projectId, action, request, listener) -> {
            if (action == PutPipelineTransportAction.TYPE) {
                final var calledTimes = calledTimesMap.computeIfAbsent(projectId, k -> new AtomicInteger(0));
                assertPutPipelineAction(calledTimes, action, request, listener, "custom-plugin-default_pipeline");
                return AcknowledgedResponse.TRUE;
            } else if (action == ILMActions.PUT) {
                // ignore lifecycle policies in this case
                return AcknowledgedResponse.TRUE;
            } else {
                // the template is not expected to be added, as the final pipeline is missing
                fail("client called with unexpected request: " + request.toString());
                return null;
            }
        });

        ClusterChangedEvent event = createClusterChangedEvent(
            Collections.emptyMap(),
            Collections.emptyMap(),
            Map.of("custom-plugin-final_pipeline", 3),
            nodes
        );
        registry.clusterChanged(event);
        assertCalledTimes(calledTimesMap, event, 1);
    }

    public void testThatComposableTemplateIsAddedIfDependenciesExist() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<ProjectId, AtomicInteger> calledTimesMap = new ConcurrentHashMap<>();
        client.setVerifier((projectId, action, request, listener) -> {
            if (action == TransportPutComposableIndexTemplateAction.TYPE) {
                final var calledTimes = calledTimesMap.computeIfAbsent(projectId, k -> new AtomicInteger(0));
                assertPutComposableIndexTemplateAction(calledTimes, action, request, listener);
                return AcknowledgedResponse.TRUE;
            } else if (action == ILMActions.PUT) {
                // ignore lifecycle policies in this case
                return AcknowledgedResponse.TRUE;
            } else if (action == PutPipelineTransportAction.TYPE) {
                // ignore pipelines in this case
                return AcknowledgedResponse.TRUE;
            } else {
                // other components should be added as they already exist with the right version already
                fail("client called with unexpected request: " + request.toString());
                return null;
            }
        });

        ClusterChangedEvent event = createClusterChangedEvent(Collections.singletonMap("custom-plugin-settings", 3), nodes);
        registry.clusterChanged(event);
        assertCalledTimes(calledTimesMap, event, 1);
    }

    public void testThatComposableTemplateIsAddedIfDependenciesHaveRightVersion() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<ProjectId, AtomicInteger> calledTimesMap = new ConcurrentHashMap<>();
        client.setVerifier((projectId, action, request, listener) -> {
            if (action == TransportPutComposableIndexTemplateAction.TYPE) {
                final var calledTimes = calledTimesMap.computeIfAbsent(projectId, k -> new AtomicInteger(0));
                assertPutComposableIndexTemplateAction(calledTimes, action, request, listener);
                return AcknowledgedResponse.TRUE;
            } else if (action instanceof PutComponentTemplateAction) {
                // ignore the component template upgrade
                return AcknowledgedResponse.TRUE;
            } else if (action == ILMActions.PUT) {
                // ignore lifecycle policies in this case
                return AcknowledgedResponse.TRUE;
            } else if (action == PutPipelineTransportAction.TYPE) {
                // ignore pipelines in this case
                return AcknowledgedResponse.TRUE;
            } else {
                // other components should be added as they already exist with the right version already
                fail("client called with unexpected request: " + request.toString());
                return null;
            }
        });

        // unless the registry requires rollovers after index template updates, the dependencies only need to be available, without regard
        // to their version
        ClusterChangedEvent event = createClusterChangedEvent(Collections.singletonMap("custom-plugin-settings", 2), nodes);
        registry.clusterChanged(event);
        assertCalledTimes(calledTimesMap, event, 1);

        // when a registry requires rollovers after index template updates, the upgrade should occur only if the dependencies are have
        // the required version
        registry.setApplyRollover(true);
        calledTimesMap.values().forEach(calledTimes -> calledTimes.set(0));
        registry.clusterChanged(event);
        Thread.sleep(100L);
        assertCalledTimes(calledTimesMap, event, 0);
        event = createClusterChangedEvent(Collections.singletonMap("custom-plugin-settings", 3), nodes);
        registry.clusterChanged(event);
        assertCalledTimes(calledTimesMap, event, 1);
    }

    public void testThatTemplatesAreUpgradedWhenNeeded() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<ProjectId, AtomicInteger> calledTimesMap = new ConcurrentHashMap<>();
        client.setVerifier((projectId, action, request, listener) -> {
            final var calledTimes = calledTimesMap.computeIfAbsent(projectId, k -> new AtomicInteger(0));
            if (action == PutPipelineTransportAction.TYPE) {
                assertPutPipelineAction(
                    calledTimes,
                    action,
                    request,
                    listener,
                    "custom-plugin-default_pipeline",
                    "custom-plugin-final_pipeline"
                );
                return AcknowledgedResponse.TRUE;
            } else if (action == ILMActions.PUT) {
                // ignore lifecycle policies in this case
                return AcknowledgedResponse.TRUE;
            } else if (action instanceof PutComponentTemplateAction) {
                assertPutComponentTemplate(calledTimes, action, request, listener);
                return AcknowledgedResponse.TRUE;
            } else if (action == TransportPutComposableIndexTemplateAction.TYPE) {
                assertPutComposableIndexTemplateAction(calledTimes, action, request, listener);
                return AcknowledgedResponse.TRUE;
            } else {
                fail("client called with unexpected request: " + request.toString());
                return null;
            }
        });

        ClusterChangedEvent event = createClusterChangedEvent(
            Map.of("custom-plugin-settings", 2, "custom-plugin-template", 2),
            Collections.emptyMap(),
            Map.of("custom-plugin-default_pipeline", 2, "custom-plugin-final_pipeline", 2),
            nodes
        );
        registry.clusterChanged(event);
        assertCalledTimes(calledTimesMap, event, 4);
    }

    public void testAutomaticRollover() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        ClusterState state = createClusterState(
            Map.of("custom-plugin-settings", 3, "custom-plugin-template", 3),
            Collections.emptyMap(),
            Map.of("custom-plugin-default_pipeline", 3, "custom-plugin-final_pipeline", 3),
            nodes
        );
        Map<String, ComposableIndexTemplate> composableTemplateConfigs = registry.getComposableTemplateConfigs();
        final var metadataBuilder = Metadata.builder(state.metadata());
        for (Map.Entry<String, ComposableIndexTemplate> entry : composableTemplateConfigs.entrySet()) {
            ComposableIndexTemplate template = entry.getValue();
            for (var project : state.metadata().projects().values()) {
                metadataBuilder.put(
                    metadataBuilder.getProject(project.id())
                        .put(
                            entry.getKey(),
                            ComposableIndexTemplate.builder()
                                .indexPatterns(template.indexPatterns())
                                .template(template.template())
                                .componentTemplates(template.composedOf())
                                .priority(template.priority())
                                .version(2L)
                                .metadata(template.metadata())
                                .dataStreamTemplate(template.getDataStreamTemplate())
                                .build()
                        )
                );
            }
        }
        for (var project : state.metadata().projects().values()) {
            metadataBuilder.put(
                metadataBuilder.getProject(project.id())
                    .put(DataStreamTestHelper.newInstance("logs-my_app-1", Collections.singletonList(new Index(".ds-ds1-000001", "ds1i"))))
                    .put(DataStreamTestHelper.newInstance("logs-my_app-2", Collections.singletonList(new Index(".ds-ds2-000001", "ds2i"))))
                    .put(
                        DataStreamTestHelper.newInstance("traces-my_app-1", Collections.singletonList(new Index(".ds-ds3-000001", "ds3i")))
                    )
            );
        }
        state = ClusterState.builder(state).metadata(metadataBuilder).build();
        ClusterChangedEvent event = createClusterChangedEvent(nodes, state);

        Map<ProjectId, AtomicInteger> rolloverCounterMap = new ConcurrentHashMap<>();
        Map<ProjectId, AtomicInteger> putIndexTemplateCounterMap = new ConcurrentHashMap<>();
        client.setVerifier((projectId, action, request, listener) -> {
            if (action instanceof RolloverAction) {
                final var rolloverCounter = rolloverCounterMap.computeIfAbsent(projectId, k -> new AtomicInteger(0));
                rolloverCounter.incrementAndGet();
                RolloverRequest rolloverRequest = ((RolloverRequest) request);
                assertThat(rolloverRequest.getRolloverTarget(), startsWith("logs-my_app-"));
                assertThat(rolloverRequest.isLazy(), equalTo(true));
            } else if (action == TransportPutComposableIndexTemplateAction.TYPE) {
                final var putIndexTemplateCounter = putIndexTemplateCounterMap.computeIfAbsent(projectId, k -> new AtomicInteger(0));
                putIndexTemplateCounter.incrementAndGet();
            }
            return AcknowledgedResponse.TRUE;
        });

        registry.clusterChanged(event);
        assertCalledTimes(putIndexTemplateCounterMap, event, 1);
        // no rollover on upgrade because the test registry doesn't support automatic rollover by default
        Thread.sleep(100L);
        assertCalledTimes(rolloverCounterMap, event, 0);

        // test successful rollovers
        registry.setApplyRollover(true);
        putIndexTemplateCounterMap.values().forEach(calledTimes -> calledTimes.set(0));
        registry.clusterChanged(event);
        assertCalledTimes(putIndexTemplateCounterMap, event, 1);
        assertCalledTimes(rolloverCounterMap, event, 2);
        var rolloverResponsesRef = registry.getRolloverResponses();
        var projectIds = state.metadata().projects().keySet();
        assertBusy(() -> {
            assertThat(rolloverResponsesRef.keySet(), equalTo(projectIds));
            for (var rolloverResponses : rolloverResponsesRef.values()) {
                assertNotNull(rolloverResponses.get());
                assertThat(rolloverResponses.get(), hasSize(2));
            }
        });

        // test again, to verify that the per-index-template creation lock gets released for reuse
        putIndexTemplateCounterMap.values().forEach(calledTimes -> calledTimes.set(0));
        rolloverCounterMap.values().forEach(calledTimes -> calledTimes.set(0));
        rolloverResponsesRef.values().forEach(v -> v.set(Set.of()));
        registry.clusterChanged(event);
        assertCalledTimes(putIndexTemplateCounterMap, event, 1);
        assertCalledTimes(rolloverCounterMap, event, 2);
        assertBusy(() -> rolloverResponsesRef.values().forEach(v -> assertThat(v.get(), hasSize(2))));

        // test rollover failures
        putIndexTemplateCounterMap.values().forEach(calledTimes -> calledTimes.set(0));
        rolloverCounterMap.values().forEach(calledTimes -> calledTimes.set(0));
        client.setVerifier((projectId, action, request, listener) -> {
            if (action instanceof RolloverAction) {
                final var rolloverCounter = rolloverCounterMap.computeIfAbsent(projectId, k -> new AtomicInteger(0));
                rolloverCounter.incrementAndGet();
                RolloverRequest rolloverRequest = ((RolloverRequest) request);
                assertThat(rolloverRequest.getRolloverTarget(), startsWith("logs-my_app-"));
                throw new RuntimeException("Failed to rollover " + rolloverRequest.getRolloverTarget());
            } else if (action == TransportPutComposableIndexTemplateAction.TYPE) {
                final var putIndexTemplateCounter = putIndexTemplateCounterMap.computeIfAbsent(projectId, k -> new AtomicInteger(0));
                putIndexTemplateCounter.incrementAndGet();
            }
            return AcknowledgedResponse.TRUE;
        });
        registry.clusterChanged(event);
        assertCalledTimes(putIndexTemplateCounterMap, event, 1);
        assertCalledTimes(rolloverCounterMap, event, 2);
        var rolloverFailureRefMap = registry.getRolloverFailure();
        assertBusy(() -> {
            assertThat(rolloverFailureRefMap.keySet(), equalTo(projectIds));
            rolloverFailureRefMap.values().forEach(rolloverFailureRef -> {
                assertNotNull(rolloverFailureRef.get());
                Exception rolloverFailure = rolloverFailureRef.get();
                assertThat(rolloverFailure.getMessage(), startsWith("Failed to rollover logs-my_app-"));
                Throwable[] suppressed = rolloverFailure.getSuppressed();
                assertThat(suppressed.length, equalTo(1));
                assertThat(suppressed[0].getMessage(), startsWith("Failed to rollover logs-my_app-"));
            });
        });
    }

    public void testRolloverForFreshInstalledIndexTemplate() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        ClusterState state = createClusterState(
            Map.of("custom-plugin-settings", 3, "custom-plugin-template", 3),
            Collections.emptyMap(),
            Map.of("custom-plugin-default_pipeline", 3, "custom-plugin-final_pipeline", 3),
            nodes
        );
        final var metadataBuilder = Metadata.builder(state.metadata());
        for (var project : state.metadata().projects().values()) {
            metadataBuilder.put(
                metadataBuilder.getProject(project.id())
                    .put(DataStreamTestHelper.newInstance("logs-my_app-1", Collections.singletonList(new Index(".ds-ds1-000001", "ds1i"))))
                    .put(DataStreamTestHelper.newInstance("logs-my_app-2", Collections.singletonList(new Index(".ds-ds2-000001", "ds2i"))))
                    .put(
                        DataStreamTestHelper.newInstance("traces-my_app-1", Collections.singletonList(new Index(".ds-ds3-000001", "ds3i")))
                    )
            );
        }
        state = ClusterState.builder(state).metadata(metadataBuilder).build();
        ClusterChangedEvent event = createClusterChangedEvent(nodes, state);

        Map<ProjectId, AtomicInteger> rolloverCounterMap = new ConcurrentHashMap<>();
        Map<ProjectId, AtomicInteger> putIndexTemplateCounterMap = new ConcurrentHashMap<>();
        client.setVerifier((projectId, action, request, listener) -> {
            if (action instanceof RolloverAction) {
                final var rolloverCounter = rolloverCounterMap.computeIfAbsent(projectId, k -> new AtomicInteger(0));
                rolloverCounter.incrementAndGet();
                RolloverRequest rolloverRequest = ((RolloverRequest) request);
                assertThat(rolloverRequest.getRolloverTarget(), startsWith("logs-my_app-"));
            } else if (action == TransportPutComposableIndexTemplateAction.TYPE) {
                final var putIndexTemplateCounter = putIndexTemplateCounterMap.computeIfAbsent(projectId, k -> new AtomicInteger(0));
                putIndexTemplateCounter.incrementAndGet();
            }
            return AcknowledgedResponse.TRUE;
        });

        registry.setApplyRollover(true);
        registry.clusterChanged(event);
        assertCalledTimes(putIndexTemplateCounterMap, event, 1);
        // rollover should be triggered even for the first installation, since the template
        // may now take precedence over a data stream's existing index template
        assertCalledTimes(rolloverCounterMap, event, 2);
    }

    public void testThatTemplatesAreNotUpgradedWhenNotNeeded() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<ProjectId, AtomicInteger> calledTimesMap = new ConcurrentHashMap<>();
        client.setVerifier((projectId, action, request, listener) -> {
            if (action == TransportPutComposableIndexTemplateAction.TYPE) {
                // ignore this
                return AcknowledgedResponse.TRUE;
            } else if (action == ILMActions.PUT) {
                // ignore lifecycle policies in this case
                return AcknowledgedResponse.TRUE;
            } else {
                fail("client called with unexpected request: " + request.toString());
                return null;
            }
        });

        ClusterChangedEvent event = createClusterChangedEvent(
            Map.of("custom-plugin-settings", 3),
            Collections.emptyMap(),
            Map.of("custom-plugin-default_pipeline", 3, "custom-plugin-final_pipeline", 3),
            nodes
        );
        registry.clusterChanged(event);
        assertCalledTimes(calledTimesMap, event, 0);
    }

    public void testThatNonExistingPoliciesAreAddedImmediately() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<ProjectId, AtomicInteger> calledTimesMap = new ConcurrentHashMap<>();
        client.setVerifier((projectId, action, request, listener) -> {
            if (action == TransportPutComposableIndexTemplateAction.TYPE) {
                // ignore this
                return AcknowledgedResponse.TRUE;
            } else if (action == ILMActions.PUT) {
                final var calledTimes = calledTimesMap.computeIfAbsent(projectId, k -> new AtomicInteger(0));
                assertPutLifecycleAction(calledTimes, action, request, listener);
                return AcknowledgedResponse.TRUE;
            } else {
                fail("client called with unexpected request: " + request.toString());
                return null;
            }
        });

        ClusterChangedEvent event = createClusterChangedEvent(
            Map.of("custom-plugin-settings", 3),
            Map.of(),
            Map.of("custom-plugin-default_pipeline", 3, "custom-plugin-final_pipeline", 3),
            nodes
        );
        registry.clusterChanged(event);
        assertCalledTimes(calledTimesMap, event, registry.getLifecyclePolicies().size());
    }

    public void testPolicyAlreadyExists() {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<String, LifecyclePolicy> policyMap = new HashMap<>();
        List<LifecyclePolicy> policies = registry.getLifecyclePolicies();
        assertThat(policies, hasSize(1));
        policies.forEach(p -> policyMap.put(p.getName(), p));

        client.setVerifier((projectId, action, request, listener) -> {
            if (action == TransportPutComposableIndexTemplateAction.TYPE) {
                // ignore this
                return AcknowledgedResponse.TRUE;
            } else if (action == ILMActions.PUT) {
                fail("if the policy already exists it should not be re-put");
            } else {
                fail("client called with unexpected request: " + request.toString());
            }
            return null;
        });

        ClusterChangedEvent event = createClusterChangedEvent(
            Map.of("custom-plugin-settings", 3),
            policyMap,
            Map.of("custom-plugin-default_pipeline", 3, "custom-plugin-final_pipeline", 3),
            nodes
        );

        registry.clusterChanged(event);
    }

    public void testPolicyAlreadyExistsButDiffers() throws IOException {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<String, LifecyclePolicy> policyMap = new HashMap<>();
        String policyStr = "{\"phases\":{\"delete\":{\"min_age\":\"1m\",\"actions\":{\"delete\":{}}}}}";
        List<LifecyclePolicy> policies = registry.getLifecyclePolicies();
        assertThat(policies, hasSize(1));
        policies.forEach(p -> policyMap.put(p.getName(), p));

        client.setVerifier((projectId, action, request, listener) -> {
            if (action == TransportPutComposableIndexTemplateAction.TYPE) {
                // ignore this
                return AcknowledgedResponse.TRUE;
            } else if (action == ILMActions.PUT) {
                fail("if the policy already exists it should not be re-put");
            } else {
                fail("client called with unexpected request: " + request.toString());
            }
            return null;
        });

        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(
                    XContentParserConfiguration.EMPTY.withRegistry(
                        new NamedXContentRegistry(
                            List.of(
                                new NamedXContentRegistry.Entry(
                                    LifecycleAction.class,
                                    new ParseField(DeleteAction.NAME),
                                    DeleteAction::parse
                                )
                            )
                        )
                    ),
                    policyStr
                )
        ) {
            LifecyclePolicy different = LifecyclePolicy.parse(parser, policies.get(0).getName());
            policyMap.put(policies.get(0).getName(), different);
            ClusterChangedEvent event = createClusterChangedEvent(
                Map.of("custom-plugin-settings", 3),
                policyMap,
                Map.of("custom-plugin-default_pipeline", 3, "custom-plugin-final_pipeline", 3),
                nodes
            );
            registry.clusterChanged(event);
        }
    }

    public void testPolicyUpgraded() throws Exception {
        registry.setPolicyUpgradeRequired(true);
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<String, LifecyclePolicy> policyMap = new HashMap<>();
        String priorPolicyStr = "{\"phases\":{\"delete\":{\"min_age\":\"1m\",\"actions\":{\"delete\":{}}}}}";
        List<LifecyclePolicy> policies = registry.getLifecyclePolicies();
        assertThat(policies, hasSize(1));
        policies.forEach(p -> policyMap.put(p.getName(), p));

        Map<ProjectId, AtomicInteger> calledTimesMap = new ConcurrentHashMap<>();
        client.setVerifier((projectId, action, request, listener) -> {
            if (action == TransportPutComposableIndexTemplateAction.TYPE) {
                // ignore this
                return AcknowledgedResponse.TRUE;
            } else if (action == ILMActions.PUT) {
                final var calledTimes = calledTimesMap.computeIfAbsent(projectId, k -> new AtomicInteger(0));
                assertPutLifecycleAction(calledTimes, action, request, listener);
                return AcknowledgedResponse.TRUE;

            } else {
                fail("client called with unexpected request: " + request.toString());
            }
            return null;
        });

        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(
                    XContentParserConfiguration.EMPTY.withRegistry(
                        new NamedXContentRegistry(
                            List.of(
                                new NamedXContentRegistry.Entry(
                                    LifecycleAction.class,
                                    new ParseField(DeleteAction.NAME),
                                    DeleteAction::parse
                                )
                            )
                        )
                    ),
                    priorPolicyStr
                )
        ) {
            LifecyclePolicy priorPolicy = LifecyclePolicy.parse(parser, policies.get(0).getName());
            policyMap.put(policies.get(0).getName(), priorPolicy);
            ClusterChangedEvent event = createClusterChangedEvent(
                Map.of("custom-plugin-settings", 3),
                policyMap,
                Map.of("custom-plugin-default_pipeline", 3, "custom-plugin-final_pipeline", 3),
                nodes
            );
            registry.clusterChanged(event);
            // we've changed one policy that should be upgraded
            assertCalledTimes(calledTimesMap, event, 1);
        }
    }

    private static void assertPutComponentTemplate(
        AtomicInteger calledTimes,
        ActionType<?> action,
        ActionRequest request,
        ActionListener<?> listener
    ) {
        assertThat(action, instanceOf(PutComponentTemplateAction.class));
        assertThat(request, instanceOf(PutComponentTemplateAction.Request.class));
        final PutComponentTemplateAction.Request putRequest = (PutComponentTemplateAction.Request) request;
        assertThat(putRequest.name(), equalTo("custom-plugin-settings"));
        ComponentTemplate componentTemplate = putRequest.componentTemplate();
        assertThat(componentTemplate.template().settings().get("index.default_pipeline"), equalTo("custom-plugin-default_pipeline"));
        assertThat(componentTemplate.metadata().get("description"), equalTo("settings for my application logs"));
        assertNotNull(listener);
        calledTimes.incrementAndGet();
    }

    private static void assertPutComposableIndexTemplateAction(
        AtomicInteger calledTimes,
        ActionType<?> action,
        ActionRequest request,
        ActionListener<?> listener
    ) {
        assertThat(request, instanceOf(TransportPutComposableIndexTemplateAction.Request.class));
        TransportPutComposableIndexTemplateAction.Request putComposableTemplateRequest =
            (TransportPutComposableIndexTemplateAction.Request) request;
        assertThat(putComposableTemplateRequest.name(), equalTo("custom-plugin-template"));
        ComposableIndexTemplate composableIndexTemplate = putComposableTemplateRequest.indexTemplate();
        assertThat(composableIndexTemplate.composedOf(), hasSize(2));
        assertThat(composableIndexTemplate.composedOf().get(0), equalTo("custom-plugin-settings"));
        assertThat(composableIndexTemplate.composedOf().get(1), equalTo("syslog@custom"));
        assertThat(composableIndexTemplate.getIgnoreMissingComponentTemplates(), hasSize(1));
        assertThat(composableIndexTemplate.getIgnoreMissingComponentTemplates().get(0), equalTo("syslog@custom"));
        assertNotNull(listener);
        calledTimes.incrementAndGet();
    }

    private static void assertPutPipelineAction(
        AtomicInteger calledTimes,
        ActionType<?> action,
        ActionRequest request,
        ActionListener<?> listener,
        String... pipelineIds
    ) {
        assertSame(PutPipelineTransportAction.TYPE, action);
        assertThat(request, instanceOf(PutPipelineRequest.class));
        final PutPipelineRequest putRequest = (PutPipelineRequest) request;
        assertThat(putRequest.getId(), oneOf(pipelineIds));
        PipelineConfiguration pipelineConfiguration = new PipelineConfiguration(
            putRequest.getId(),
            putRequest.getSource(),
            putRequest.getXContentType()
        );
        List<?> processors = (List<?>) pipelineConfiguration.getConfig().get("processors");
        assertThat(processors, hasSize(1));
        Map<?, ?> setProcessor = (Map<?, ?>) ((Map<?, ?>) processors.get(0)).get("set");
        assertNotNull(setProcessor.get("field"));
        assertNotNull(setProcessor.get("copy_from"));
        assertNotNull(listener);
        calledTimes.incrementAndGet();
    }

    private static void assertPutLifecycleAction(
        AtomicInteger calledTimes,
        ActionType<?> action,
        ActionRequest request,
        ActionListener<?> listener
    ) {
        assertSame(ILMActions.PUT, action);
        assertThat(request, instanceOf(PutLifecycleRequest.class));
        final PutLifecycleRequest putRequest = (PutLifecycleRequest) request;
        assertThat(putRequest.getPolicy().getName(), equalTo("custom-plugin-policy"));
        assertNotNull(listener);
        calledTimes.incrementAndGet();
    }

    private static void assertCalledTimes(Map<ProjectId, AtomicInteger> calledTimesMap, ClusterChangedEvent event, int expectedTimes)
        throws Exception {
        assertBusy(() -> {
            if (expectedTimes > 0) {
                assertThat(calledTimesMap.keySet(), equalTo(event.state().metadata().projects().keySet()));
            }
            for (var calledTimes : calledTimesMap.values()) {
                assertThat(calledTimes.get(), equalTo(expectedTimes));
            }
        });
    }

    private ClusterChangedEvent createClusterChangedEvent(Map<String, Integer> existingTemplates, DiscoveryNodes nodes) {
        return createClusterChangedEvent(existingTemplates, Collections.emptyMap(), Collections.emptyMap(), nodes);
    }

    private ClusterChangedEvent createClusterChangedEvent(
        Map<String, Integer> existingTemplates,
        Map<String, LifecyclePolicy> existingPolicies,
        Map<String, Integer> existingIngestPipelines,
        DiscoveryNodes nodes
    ) {
        ClusterState clusterState = createClusterState(existingTemplates, existingPolicies, existingIngestPipelines, nodes);
        return createClusterChangedEvent(nodes, clusterState);
    }

    private ClusterChangedEvent createClusterChangedEvent(DiscoveryNodes nodes, ClusterState state) {
        ClusterChangedEvent realEvent = new ClusterChangedEvent(
            "created-from-test",
            state,
            ClusterState.builder(new ClusterName("test")).build()
        );
        ClusterChangedEvent event = spy(realEvent);
        when(event.localNodeMaster()).thenReturn(nodes.isLocalNodeElectedMaster());

        return event;
    }

    private ClusterState createClusterState(
        Map<String, Integer> existingComponentTemplates,
        Map<String, LifecyclePolicy> existingPolicies,
        Map<String, Integer> existingIngestPipelines,
        DiscoveryNodes nodes
    ) {
        Map<String, ComponentTemplate> componentTemplates = new HashMap<>();
        for (Map.Entry<String, Integer> template : existingComponentTemplates.entrySet()) {
            ComponentTemplate mockTemplate = mock(ComponentTemplate.class);
            when(mockTemplate.version()).thenReturn(template.getValue() == null ? null : (long) template.getValue());
            componentTemplates.put(template.getKey(), mockTemplate);
        }

        Map<String, LifecyclePolicyMetadata> existingILMMeta = existingPolicies.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> new LifecyclePolicyMetadata(e.getValue(), Collections.emptyMap(), 1, 1)));
        IndexLifecycleMetadata ilmMeta = new IndexLifecycleMetadata(existingILMMeta, OperationMode.RUNNING);

        Map<String, PipelineConfiguration> ingestPipelines = new HashMap<>();
        for (Map.Entry<String, Integer> pipelineEntry : existingIngestPipelines.entrySet()) {
            // we cannot mock PipelineConfiguration as it is a final class
            ingestPipelines.put(
                pipelineEntry.getKey(),
                new PipelineConfiguration(
                    pipelineEntry.getKey(),
                    new BytesArray(Strings.format("{\"version\": %d}", pipelineEntry.getValue())),
                    XContentType.JSON
                )
            );
        }
        IngestMetadata ingestMetadata = new IngestMetadata(ingestPipelines);

        return ClusterState.builder(new ClusterName("test"))
            .metadata(
                Metadata.builder()
                    .transientSettings(Settings.EMPTY)
                    .put(
                        ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID)
                            .componentTemplates(componentTemplates)
                            .putCustom(IndexLifecycleMetadata.TYPE, ilmMeta)
                            .putCustom(IngestMetadata.TYPE, ingestMetadata)
                            .build()
                    )
                    .put(
                        ProjectMetadata.builder(projectId)
                            .componentTemplates(componentTemplates)
                            .putCustom(IndexLifecycleMetadata.TYPE, ilmMeta)
                            .putCustom(IngestMetadata.TYPE, ingestMetadata)
                            .build()
                    )
                    .build()
            )
            .blocks(new ClusterBlocks.Builder().build())
            .nodes(nodes)
            .build();
    }

    // ------------- functionality unit test --------

    public void testFindRolloverTargetDataStreams() {
        ComposableIndexTemplate it1 = ComposableIndexTemplate.builder()
            .indexPatterns(List.of("ds1*", "ds2*", "ds3*"))
            .priority(100L)
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            .build();

        ComposableIndexTemplate it2 = ComposableIndexTemplate.builder()
            .indexPatterns(List.of("ds2*"))
            .priority(200L)
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            .build();

        ComposableIndexTemplate it5 = ComposableIndexTemplate.builder()
            .indexPatterns(List.of("ds5*"))
            .priority(200L)
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            .build();

        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(DataStreamTestHelper.newInstance("ds1", Collections.singletonList(new Index(".ds-ds1-000001", "ds1i"))))
            .put(DataStreamTestHelper.newInstance("ds2", Collections.singletonList(new Index(".ds-ds2-000001", "ds2i"))))
            .put(DataStreamTestHelper.newInstance("ds3", Collections.singletonList(new Index(".ds-ds3-000001", "ds3i"))))
            .put(DataStreamTestHelper.newInstance("ds4", Collections.singletonList(new Index(".ds-ds4-000001", "ds4i"))))
            .put("it1", it1)
            .put("it2", it2)
            .put("it5", it5)
            .build();

        assertThat(IndexTemplateRegistry.findRolloverTargetDataStreams(project, "it1", it1), containsInAnyOrder("ds1", "ds3"));
        assertThat(IndexTemplateRegistry.findRolloverTargetDataStreams(project, "it2", it2), contains("ds2"));
        assertThat(IndexTemplateRegistry.findRolloverTargetDataStreams(project, "it5", it5), empty());
    }

    // -------------

    /**
     * A client that delegates to a verifying function for action/request/listener
     */
    public static class VerifyingClient extends NoOpClient {

        private Verifier verifier = (p, a, r, l) -> {
            fail("verifier not set");
            return null;
        };

        VerifyingClient(ThreadPool threadPool) {
            super(threadPool, TestProjectResolvers.usingRequestHeader(threadPool.getThreadContext()));
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            try {
                final ProjectId projectId = ProjectId.fromId(
                    threadPool().getThreadContext().getHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER)
                );
                listener.onResponse((Response) verifier.verify(projectId, action, request, listener));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        public VerifyingClient setVerifier(Verifier verifier) {
            this.verifier = verifier;
            return this;
        }
    }

    private interface Verifier {
        ActionResponse verify(ProjectId projectId, ActionType<?> action, ActionRequest request, ActionListener<?> listener);
    }
}
