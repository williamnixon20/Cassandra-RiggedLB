/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.example;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistanceEvaluator;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.loadbalancing.DefaultLoadBalancingPolicy;
import com.datastax.oss.driver.internal.core.loadbalancing.helper.DefaultNodeDistanceEvaluatorHelper;
import com.datastax.oss.driver.internal.core.loadbalancing.helper.OptionalLocalDcHelper;
import com.datastax.oss.driver.internal.core.loadbalancing.nodeset.DcAgnosticNodeSet;
import com.datastax.oss.driver.internal.core.loadbalancing.nodeset.MultiDcNodeSet;
import com.datastax.oss.driver.internal.core.loadbalancing.nodeset.NodeSet;
import com.datastax.oss.driver.internal.core.loadbalancing.nodeset.SingleDcNodeSet;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

/**
 * A basic implementation of {@link LoadBalancingPolicy} that can serve as a building block for more
 * advanced use cases.
 *
 * <p>To activate this policy, modify the {@code basic.load-balancing-policy} section in the driver
 * configuration, for example:
 *
 * <pre>
 * datastax-java-driver {
 *   basic.load-balancing-policy {
 *     class = BasicLoadBalancingPolicy
 *     local-datacenter = datacenter1 # optional
 *   }
 * }
 * </pre>
 *
 * See {@code reference.conf} (in the manual or core driver JAR) for more details.
 *
 * <p><b>Local datacenter</b>: This implementation will only define a local datacenter if it is
 * explicitly set either through configuration or programmatically; if the local datacenter is
 * unspecified, this implementation will effectively act as a datacenter-agnostic load balancing
 * policy and will consider all nodes in the cluster when creating query plans, regardless of their
 * datacenter.
 *
 * <p><b>Query plan</b>: This implementation prioritizes replica nodes over non-replica ones; if
 * more than one replica is available, the replicas will be shuffled. Non-replica nodes will be
 * included in a round-robin fashion. If the local datacenter is defined (see above), query plans
 * will only include local nodes, never remote ones; if it is unspecified however, query plans may
 * contain nodes from different datacenters.
 *
 * <p><b>This class is not recommended for normal users who should always prefer {@link
 * DefaultLoadBalancingPolicy}</b>.
 */
@ThreadSafe
public class FakeLoadBalancingPolicy implements LoadBalancingPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(FakeLoadBalancingPolicy.class);

  @NonNull protected final InternalDriverContext context;
  @NonNull protected final DriverExecutionProfile profile;
  @NonNull protected final String logPrefix;

  private final int maxNodesPerRemoteDc;

  // private because they should be set in init() and never be modified after
  private volatile DistanceReporter distanceReporter;
  private volatile NodeDistanceEvaluator nodeDistanceEvaluator;
  private volatile String localDc;
  private volatile NodeSet liveNodes;

  public FakeLoadBalancingPolicy(@NonNull DriverContext context, @NonNull String profileName) {
    this.context = (InternalDriverContext) context;
    profile = context.getConfig().getProfile(profileName);
    logPrefix = context.getSessionName() + "|" + profileName;
    maxNodesPerRemoteDc =
        profile.getInt(DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_MAX_NODES_PER_REMOTE_DC);
  }

  /**
   * Returns the local datacenter name, if known; empty otherwise.
   *
   * <p>When this method returns null, then datacenter awareness is completely disabled. All
   * non-ignored nodes will be considered "local" regardless of their actual datacenters, and will
   * have equal chances of being selected for query plans.
   *
   * <p>After the policy is {@linkplain #init(Map, DistanceReporter) initialized} this method will
   * return the local datacenter that was discovered by calling {@link #discoverLocalDc(Map)}.
   * Before initialization, this method always returns null.
   */
  @Nullable
  protected String getLocalDatacenter() {
    return localDc;
  }

  /** @return The nodes currently considered as live. */
  protected NodeSet getLiveNodes() {
    return liveNodes;
  }

  @Override
  public void init(@NonNull Map<UUID, Node> nodes, @NonNull DistanceReporter distanceReporter) {
    this.distanceReporter = distanceReporter;
    localDc = discoverLocalDc(nodes).orElse(null);
    nodeDistanceEvaluator = createNodeDistanceEvaluator(localDc, nodes);
    liveNodes =
        localDc == null
            ? new DcAgnosticNodeSet()
            : maxNodesPerRemoteDc <= 0 ? new SingleDcNodeSet(localDc) : new MultiDcNodeSet();
    for (Node node : nodes.values()) {
      NodeDistance distance = computeNodeDistance(node);
      distanceReporter.setDistance(node, distance);
      if (distance != NodeDistance.IGNORED && node.getState() != NodeState.DOWN) {
        // This includes state == UNKNOWN. If the node turns out to be unreachable, this will be
        // detected when we try to open a pool to it, it will get marked down and this will be
        // signaled back to this policy, which will then remove it from the live set.
        liveNodes.add(node);
      }
    }
  }

  /**
   * Returns the local datacenter, if it can be discovered, or returns {@link Optional#empty empty}
   * otherwise.
   *
   * <p>This method is called only once, during {@linkplain LoadBalancingPolicy#init(Map,
   * DistanceReporter) initialization}.
   *
   * <p>Implementors may choose to throw {@link IllegalStateException} instead of returning {@link
   * Optional#empty empty}, if they require a local datacenter to be defined in order to operate
   * properly.
   *
   * <p>If this method returns empty, then datacenter awareness will be completely disabled. All
   * non-ignored nodes will be considered "local" regardless of their actual datacenters, and will
   * have equal chances of being selected for query plans.
   *
   * @param nodes All the nodes that were known to exist in the cluster (regardless of their state)
   *     when the load balancing policy was initialized. This argument is provided in case
   *     implementors need to inspect the cluster topology to discover the local datacenter.
   * @return The local datacenter, or {@link Optional#empty empty} if none found.
   * @throws IllegalStateException if the local datacenter could not be discovered, and this policy
   *     cannot operate without it.
   */
  @NonNull
  protected Optional<String> discoverLocalDc(@NonNull Map<UUID, Node> nodes) {
    return new OptionalLocalDcHelper(context, profile, logPrefix).discoverLocalDc(nodes);
  }

  /**
   * Creates a new node distance evaluator to use with this policy.
   *
   * <p>This method is called only once, during {@linkplain LoadBalancingPolicy#init(Map,
   * DistanceReporter) initialization}, and only after local datacenter discovery has been
   * attempted.
   *
   * @param localDc The local datacenter that was just discovered, or null if none found.
   * @param nodes All the nodes that were known to exist in the cluster (regardless of their state)
   *     when the load balancing policy was initialized. This argument is provided in case
   *     implementors need to inspect the cluster topology to create the evaluator.
   * @return the distance evaluator to use.
   */
  @NonNull
  protected NodeDistanceEvaluator createNodeDistanceEvaluator(
      @Nullable String localDc, @NonNull Map<UUID, Node> nodes) {
    return new DefaultNodeDistanceEvaluatorHelper(context, profile, logPrefix)
        .createNodeDistanceEvaluator(localDc, nodes);
  }

  @NonNull
  @Override
  public Queue<Node> newQueryPlan(@Nullable Request request, @Nullable Session session) {
    Set<Node> currentNodes = liveNodes.dc(localDc);

    // Sort by Hash Code
    List<Node> sorted =
        currentNodes.stream()
            .sorted(Comparator.comparing(Node::hashCode))
            .collect(Collectors.toList());

    ConcurrentLinkedQueue<Node> returnQueue = new ConcurrentLinkedQueue<>();
    // Return 1 node only as potential coordinator for queues
    if (sorted.size() > 0) {
      returnQueue.add(sorted.get(0));
    }
    return returnQueue;
  }

  @Override
  public void onAdd(@NonNull Node node) {
    NodeDistance distance = computeNodeDistance(node);
    // Setting to a non-ignored distance triggers the session to open a pool, which will in turn
    // set the node UP when the first channel gets opened, then #onUp will be called, and the
    // node will be eventually added to the live set.
    distanceReporter.setDistance(node, distance);
    LOG.debug("[{}] {} was added, setting distance to {}", logPrefix, node, distance);
  }

  @Override
  public void onUp(@NonNull Node node) {
    NodeDistance distance = computeNodeDistance(node);
    if (node.getDistance() != distance) {
      distanceReporter.setDistance(node, distance);
    }
    if (distance != NodeDistance.IGNORED && liveNodes.add(node)) {
      LOG.debug("[{}] {} came back UP, added to live set", logPrefix, node);
    }
  }

  @Override
  public void onDown(@NonNull Node node) {
    if (liveNodes.remove(node)) {
      LOG.debug("[{}] {} went DOWN, removed from live set", logPrefix, node);
    }
  }

  @Override
  public void onRemove(@NonNull Node node) {
    if (liveNodes.remove(node)) {
      LOG.debug("[{}] {} was removed, removed from live set", logPrefix, node);
    }
  }

  /**
   * Computes the distance of the given node.
   *
   * <p>This method is called during {@linkplain #init(Map, DistanceReporter) initialization}, when
   * a node {@linkplain #onAdd(Node) is added}, and when a node {@linkplain #onUp(Node) is back UP}.
   */
  protected NodeDistance computeNodeDistance(@NonNull Node node) {
    // We interrogate the custom evaluator every time since it could be dynamic
    // and change its verdict between two invocations of this method.
    NodeDistance distance = nodeDistanceEvaluator.evaluateDistance(node, localDc);
    if (distance != null) {
      return distance;
    }
    // no local DC defined: all nodes are considered LOCAL.
    if (localDc == null) {
      return NodeDistance.LOCAL;
    }
    // otherwise, the node is LOCAL if its datacenter is the local datacenter.
    if (Objects.equals(node.getDatacenter(), localDc)) {
      return NodeDistance.LOCAL;
    }
    // otherwise, the node will be either REMOTE or IGNORED, depending
    // on how many remote nodes we accept per DC.
    if (maxNodesPerRemoteDc > 0) {
      Object[] remoteNodes = liveNodes.dc(node.getDatacenter()).toArray();
      for (int i = 0; i < maxNodesPerRemoteDc; i++) {
        if (i == remoteNodes.length) {
          // there is still room for one more REMOTE node in this DC
          return NodeDistance.REMOTE;
        } else if (remoteNodes[i] == node) {
          return NodeDistance.REMOTE;
        }
      }
    }
    return NodeDistance.IGNORED;
  }

  @Override
  public void close() {
    // nothing to do
  }
}
