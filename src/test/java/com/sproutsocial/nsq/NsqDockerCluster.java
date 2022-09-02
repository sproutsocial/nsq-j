package com.sproutsocial.nsq;

import com.google.common.base.MoreObjects;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.okhttp.OkDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.CreateNetworkResponse;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.ExposedPort;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class that can manage the full lifecycle of an NSQ cluster. This is used
 * to test publish / subscribe workflows, as well as simulate failures in the NSQ
 * infrastructure.
 *
 * Example usage:
 *
 * <pre>
 * public class MyAwesomeTest {
 *     private NsqDockerCluster cluster;
 *
 *     @Before
 *     public void setup() {
 *       cluster = NsqDockerCluster.builder()
 *            .withNsqdCount(3)
 *            .start()
 *            .awaitExposedPorts();
 *     }
 *
 *     @After
 *     public void teardown() {
 *       cluster.shutdown();
 *     }
 * }
 * </pre>
 */
public class NsqDockerCluster {
    public static class ContainerConfig {
        public String image;
        public String nameFormat;
        public String cmdFormat;

        public ContainerConfig(final String image,
                               final String nameFormat,
                               final String cmdFormat) {
            this.image = image;
            this.nameFormat = nameFormat;
            this.cmdFormat = cmdFormat;
        }
    }

    public static class CreatedContainer {
        public String containerId;
        public String name;
        public Map<String, PortBinding> exposedPortsByName;

        public CreatedContainer(final String containerId,
                                final String name,
                                final Map<String, PortBinding> exposedPortsByName) {
            this.containerId = containerId;
            this.name = name;
            this.exposedPortsByName = exposedPortsByName;
        }
    }

    private static class CreatedContainers {
        public String networkId;
        public List<NsqdNode> nsqdNodes;
        public NsqLookupNode lookupNode;

        public CreatedContainers(final String networkId,
                                 final List<NsqdNode> nsqdNodes,
                                 final NsqLookupNode lookupNode) {
            this.networkId = networkId;
            this.nsqdNodes = nsqdNodes;
            this.lookupNode = lookupNode;
        }

        public final List<String> getAllContainerIds() {
            final ImmutableList.Builder<String> containerIds = new ImmutableList.Builder<>();
            containerIds.add(lookupNode.containerId);
            for (final NsqdNode nsqd : nsqdNodes) {
                containerIds.add(nsqd.containerId);
            }
            return containerIds.build();
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(NsqDockerCluster.class);
    private static final ContainerConfig DEFAULT_NSQD_CONFIG = new ContainerConfig(
        "nsqio/nsq:v1.2.1",
        "nsqd-cluster-%d-%s",
        "/nsqd --lookupd-tcp-address=%s --broadcast-address=%s --broadcast-tcp-port=%s --broadcast-http-port=%s --msg-timeout=3s");

    private static final ContainerConfig DEFAULT_LOOKUP_CONFIG = new ContainerConfig(
        "nsqio/nsq:v1.2.1",
        "nsq-lookup-cluster-%d-%s",
        "/nsqlookupd");

    public static class Builder {
        private final ExecutorService executor = Executors.newFixedThreadPool(4);
        private int nsqdCount;
        private ContainerConfig nsqdConfig;
        private ContainerConfig lookupConfig;

        public Builder() {
            this.nsqdCount = 1;
            this.nsqdConfig = DEFAULT_NSQD_CONFIG;
            this.lookupConfig = DEFAULT_LOOKUP_CONFIG;
        }

        public Builder withNsqdCount(final int count) {
            this.nsqdCount = count;
            return this;
        }

        public Builder withNsqdConfig(final ContainerConfig config) {
            this.nsqdConfig = config;
            return this;
        }

        public Builder withLookupConfig(final ContainerConfig config) {
            this.lookupConfig = config;
            return this;
        }

        public NsqDockerCluster start() {
            final UUID clusterId = UUID.randomUUID();
            final DockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder()
                .build();
            final DockerHttpClient dockerHttpClient = new OkDockerHttpClient.Builder()
                .dockerHost(config.getDockerHost())
                .sslConfig(config.getSSLConfig())
                .build();
            final DockerClient dockerClient = DockerClientImpl.getInstance(config, dockerHttpClient);
            dockerClient.pingCmd().exec();
            final CreatedContainers containers = createAndStartContainers(clusterId, dockerClient);
            return new NsqDockerCluster(
                clusterId,
                executor,
                dockerClient,
                containers.networkId,
                containers.nsqdNodes,
                containers.lookupNode);
        }

        private CreatedContainers createAndStartContainers(final UUID clusterId, final DockerClient dockerClient) {
            final CreateNetworkResponse createdNetwork = dockerClient.createNetworkCmd()
                .withName(String.format("nsq-%s", clusterId))
                .withAttachable(true)
                .exec();
            final List<Future<CreatedContainer>> lookupContainers = createContainers(
                clusterId,
                dockerClient,
                lookupConfig,
                1,
                createdNetwork.getId(),
                ImmutableList.of(),
                ImmutableMap.of("tcp_port", ExposedPort.tcp(4160), "http_port", ExposedPort.tcp(4161)));
            final ImmutableList.Builder<NsqdNode> nsqdNodes = new ImmutableList.Builder<>();
            final NsqLookupNode lookupNode;
            try {
                ImmutableMap<String, ExposedPort> portMap = ImmutableMap.of("tcp_port", ExposedPort.tcp(4150), "http_port", ExposedPort.tcp(4151));
                final List<Future<CreatedContainer>> nsqdContainers = createContainers(
                        clusterId,
                        dockerClient,
                        nsqdConfig,
                        nsqdCount,
                        createdNetwork.getId(),
                        ImmutableList.of(String.format("%s:%d", lookupContainers.get(0).get().name, 4160),// Lookup TCP hostname and port
                                "127.0.0.1", // What address and ports this nsqd is going to broadcast to the lookup
                                "$${tcpPort}",
                                "$${httpPort}"),
                        portMap);

                lookupNode = new NsqLookupNode(lookupContainers.get(0).get().containerId, lookupContainers.get(0).get().exposedPortsByName);

                for (final Future<CreatedContainer> createdNsqd : nsqdContainers) {
                    nsqdNodes.add(new NsqdNode(createdNsqd.get().containerId, createdNsqd.get().exposedPortsByName));
                }
                final CreatedContainers created = new CreatedContainers(createdNetwork.getId(), nsqdNodes.build(), lookupNode);
                startContainers(dockerClient, created.getAllContainerIds());
                return created;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted during container creation and start");
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        private final List<Future<CreatedContainer>> createContainers(final UUID clusterId,
                                                                      final DockerClient dockerClient,
                                                                      final ContainerConfig config,
                                                                      final int count,
                                                                      final String networkId,
                                                                      final List<String> cmdBindings,
                                                                      final Map<String, ExposedPort> exposedPorts) {
            final ImmutableList.Builder<Future<CreatedContainer>> createdContainers = new ImmutableList.Builder<>();
            for (int i = 0; i < count; i++) {
                final String containerName = String.format(config.nameFormat, i, clusterId);
                final Map<String, PortBinding> allocatedPorts = exposedPorts.entrySet()
                        .stream()
                        .collect(Collectors.toMap(
                                entry -> entry.getKey(),
                                entry -> new PortBinding(Ports.Binding.bindPort(randomPort()), entry.getValue())));
                final List<String> cmd = buildCmd(containerName, allocatedPorts, config.cmdFormat, cmdBindings);
                logger.info("Startup command: {}", cmd);

                final HostConfig hostConfig = HostConfig.newHostConfig()
                    .withNetworkMode(networkId);
                createdContainers.add(executor.submit(() -> {
                            final CreateContainerResponse response = dockerClient.createContainerCmd(config.image)
                                .withName(containerName)
                                .withAliases(containerName)
                                .withHostConfig(hostConfig)
                                .withCmd(cmd)
                                .withPortBindings(allocatedPorts.values().stream().toArray(PortBinding[]::new))
                                .exec();
                            dockerClient.connectToNetworkCmd()
                                .withContainerId(response.getId())
                                .withNetworkId(networkId);
                            return new CreatedContainer(response.getId(), containerName, allocatedPorts);
                        }));
            }
            return createdContainers.build();
        }

        private final void startContainers(final DockerClient dockerClient, final List<String> containerIds) {
            NsqDockerCluster.executeForAllContainers(
                executor, containerIds,
                containerId -> dockerClient.startContainerCmd(containerId).exec());
        }

        private final List<String> buildCmd(final String containerName,
                                            Map<String, PortBinding> allocatedPorts, final String format,
                                            final List<String> bindings) {
            final List<String> substituted = bindings.stream()
                    .map(binding -> binding.replace("$${containerName}", containerName))
                    .map(binding -> binding.replace("$${httpPort}", allocatedPorts.get("http_port").getBinding().getHostPortSpec()))
                    .map(binding -> binding.replace("$${tcpPort}", allocatedPorts.get("tcp_port").getBinding().getHostPortSpec()))
                .collect(Collectors.toList());
            return Splitter.on(" ")
                .splitToList(String.format(format, substituted.stream().toArray(String[]::new)));
        }

        // Get a random, locally available port allocated to us.
        private final int randomPort() {
            ServerSocket serverSocket = null;
            try {
                serverSocket = new ServerSocket(0);
                return serverSocket.getLocalPort();
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                if (serverSocket != null) {
                    try {
                        serverSocket.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    public static class ConnectableNode {
        protected final String containerId;
        protected final Map<String, HostAndPort> hostAndPorts;

        public ConnectableNode(final String containerId,
                               final Map<String, HostAndPort> hostAndPorts) {
            this.containerId = containerId;
            this.hostAndPorts = hostAndPorts;
        }

        public final Map<String, HostAndPort> getHostAndPorts() {
            return hostAndPorts;
        }

        public final String getContainerId() {
            return containerId;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("hostAndPorts", hostAndPorts)
                .add("containerId", containerId)
                .toString();
        }

        public boolean allPortsConnectable() {
            for (final HostAndPort hostAndPort : hostAndPorts.values()) {
                try (final Socket socket = new Socket(hostAndPort.getHost(), hostAndPort.getPort())) {
                    logger.info("Successfully connected to host: {}", hostAndPort);
                } catch (IOException e) {
                    logger.info("Cannot connect to host: {}, retrying", hostAndPort);
                    return false;
                }
            }
            logger.info("All host ports on node {} connectable", this);
            return true;
        }

        public static final Map<String, HostAndPort> convertPortBindings(final Map<String, PortBinding> portBindings) {
            return portBindings.entrySet().stream().collect(
                Collectors.toMap(entry -> entry.getKey(), entry -> HostAndPort.fromParts("127.0.0.1", Integer.parseInt(entry.getValue().getBinding().getHostPortSpec()))));
        }
    }

    public static class NsqdNode extends ConnectableNode {
        public NsqdNode(final String containerId,
                        final Map<String, PortBinding> portBindings) {
            super(containerId, ConnectableNode.convertPortBindings(portBindings));
        }

        public final HostAndPort getTcpHostAndPort() {
            return hostAndPorts.get("tcp_port");
        }
    }

    public static class NsqLookupNode extends ConnectableNode {
        public NsqLookupNode(final String containerId,
                             final Map<String, PortBinding> portBindings) {
            super(containerId, ConnectableNode.convertPortBindings(portBindings));
        }

        public final HostAndPort getTcpHostAndPort() {
            return hostAndPorts.get("tcp_port");
        }

        public HostAndPort getHttpHostAndPort() {
            return hostAndPorts.get("http_port");
        }
    }

    private final UUID clusterId;
    private final ExecutorService executor;
    private final DockerClient dockerClient;
    private final String networkId;
    private final List<NsqdNode> nsqds;
    private final NsqLookupNode lookup;

    public NsqDockerCluster(final UUID clusterId,
                            final ExecutorService executor,
                            final DockerClient dockerClient,
                            final String networkId,
                            final List<NsqdNode> nsqds,
                            final NsqLookupNode lookup)  {
        this.clusterId = clusterId;
        this.executor = executor;
        this.dockerClient = dockerClient;
        this.networkId = networkId;
        this.nsqds = nsqds;
        this.lookup = lookup;
    }

    public static Builder builder() {
        return new Builder();
    }

    public final List<NsqdNode> getNsqdNodes() {
        return nsqds;
    }

    public final NsqLookupNode getLookupNode() {
        return lookup;
    }

    public final List<ConnectableNode> getAllNodes() {
        final ImmutableList.Builder<ConnectableNode> nodes  = new ImmutableList.Builder<>();
        nodes.add(lookup);
        nodes.addAll(nsqds);
        return nodes.build();
    }

    public final List<String> getAllContainerIds() {
        final ImmutableList.Builder<String> containerIds = new ImmutableList.Builder<>();
        containerIds.add(lookup.containerId);
        for (final NsqdNode nsqd : nsqds) {
            containerIds.add(nsqd.containerId);
        }
        return containerIds.build();
    }

    public void shutdown() {
        stopContainers();
        removeContainers();
        removeNetwork();
    }

    /**
     * Block until all the configured ports are successfully connectable. Allows
     * clients to synchronize their startup, without having to read from standard-out
     * on all the running containers.
     */
    public NsqDockerCluster awaitExposedPorts() {
        while (true) {
            if (getAllNodes().stream().allMatch(ConnectableNode::allPortsConnectable)) {
                logger.info("Cluster is ready: All ports connectable.");
                return this;
            } else {
                try {
                    Thread.sleep(1000);
                    continue;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during container execution");
                }
            }
        }
    }

    public void restartNetworkFor(final ConnectableNode node) {
        disconnectNetworkFor(node);
        reconnectNetworkFor(node);
    }

    /**
     * Disconnect the given node from the underlying network.
     */
    public void disconnectNetworkFor(final ConnectableNode node) {
        logger.info("Disconnecting network: {} for container: {}", networkId, node.getContainerId());
        dockerClient.disconnectFromNetworkCmd()
            .withNetworkId(networkId)
            .withContainerId(node.getContainerId())
            .exec();
        logger.info("Network {} disconnected for container: {}", networkId, node.getContainerId());
    }

    /**
     * Reconnect the given node to the underlying network
     */
    public void reconnectNetworkFor(final ConnectableNode node) {
        logger.info("Reconnecting network {} for container: {}", networkId, node.getContainerId());
        dockerClient.connectToNetworkCmd()
            .withNetworkId(networkId)
            .withContainerId(node.getContainerId())
            .exec();
        logger.info("Network {} reconnected for container: {}", networkId, node.getContainerId());
    }

    private void stopContainers() {
        executeForAllContainers(
            executor, getAllContainerIds(),
            containerId -> dockerClient.stopContainerCmd(containerId).exec());
    }

    private void removeContainers() {
        executeForAllContainers(
            executor, getAllContainerIds(),
            containerId -> dockerClient.removeContainerCmd(containerId).exec());
    }

    private void removeNetwork() {
        dockerClient.removeNetworkCmd(networkId).exec();
    }

    private static void executeForAllContainers(final ExecutorService executor,
                                                final List<String> containerIds,
                                                final Consumer<String> executeFn) {
        final List<Callable<Void>> tasks = containerIds.stream()
            .map(containerId -> new Callable<Void>() {
                    @Override
                    public Void call() {
                        executeFn.accept(containerId);
                        return null;
                    }
                })
            .collect(Collectors.toList());

        try {
            final List<Future<Void>> futures = executor.invokeAll(tasks);
            for (final Future<Void> f : futures) {
                // Block until the task is done.
                f.get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted during container execution");
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
