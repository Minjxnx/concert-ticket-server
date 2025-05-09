package com.concert.server;

import com.concert.coordination.LeaderElection;
import com.concert.coordination.ZookeeperCoordinator;
import com.concert.nameservice.EtcdNameService;
import com.concert.repository.DataRepository;
import com.concert.service.ConcertService;
import com.concert.service.ReservationService;
import com.concert.service.TicketInventoryService;
import com.concert.service.SystemCoordinationService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Main server class for the Concert Ticket Reservation System
 * Initializes all necessary components and starts the gRPC server
 */
public class ConcertServer {
    private static final Logger logger = LogManager.getLogger(ConcertServer.class);

    private final Server server;
    private final ServerNode serverNode;
    private final int port;
    private final ZookeeperCoordinator zookeeperCoordinator;
    private final EtcdNameService nameService;
    private final DataRepository dataRepository;
    private final LeaderElection leaderElection;

    public ConcertServer(int port, String zkConnectString, String etcdEndpoints) {
        this.port = port;

        // Initialize data repository
        this.dataRepository = new DataRepository();

        // Initialize ZooKeeper coordinator
        this.zookeeperCoordinator = new ZookeeperCoordinator(zkConnectString);

        // Initialize name service
        this.nameService = new EtcdNameService(etcdEndpoints);

        // Initialize leader election
        this.leaderElection = new LeaderElection(zookeeperCoordinator, "/election", "node-" + port);

        // Initialize the server node
        this.serverNode = new ServerNode(port, dataRepository, zookeeperCoordinator, nameService, leaderElection);

        // Create gRPC server and register services
        this.server = ServerBuilder.forPort(port)
                .addService(new ConcertService(serverNode))
                .addService(new ReservationService(serverNode))
                .addService(new TicketInventoryService(serverNode))
                .addService(new SystemCoordinationService(serverNode))
                .build();
    }

    /**
     * Start the server
     */
    public void start() throws IOException {
        // Start the gRPC server
        server.start();
        logger.info("Server started, listening on port " + port);

        // Register with name service
        nameService.registerNode("node-" + port, "localhost:" + port);
        logger.info("Registered with name service");

        // Start leader election
        try {
            leaderElection.start();
            logger.info("Started leader election process");
        } catch (Exception e) {
            logger.error("Failed to start leader election", e);
        }

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("JVM shutdown hook triggered");
            try {
                ConcertServer.this.stop();
            } catch (Exception e) {
                logger.error("Error during shutdown", e);
            }
        }));
    }

    /**
     * Stop the server
     */
    public void stop() throws InterruptedException {
        if (server != null) {
            // Unregister from name service
            nameService.unregisterNode("node-" + port);
            logger.info("Unregistered from name service");

            // Close ZooKeeper connection
            zookeeperCoordinator.close();
            logger.info("Closed ZooKeeper connection");

            // Stop the gRPC server
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
            logger.info("Server shut down");
        }
    }

    /**
     * Block until server is terminated
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * Main method to start the server
     */
    public static void main(String[] args) {
        try {
            // Load configuration from properties file
            Properties properties = new Properties();
            properties.load(ConcertServer.class.getClassLoader().getResourceAsStream("application.properties"));

            // Get server port from arguments or use default
            int port = args.length > 0 ? Integer.parseInt(args[0]) :
                    Integer.parseInt(properties.getProperty("server.port", "8080"));

            // Get ZooKeeper connection string
            String zkConnectString = properties.getProperty("zookeeper.connectString", "localhost:2181");

            // Get etcd endpoints
            String etcdEndpoints = properties.getProperty("etcd.endpoints", "http://localhost:2379");

            // Create and start the server
            ConcertServer server = new ConcertServer(port, zkConnectString, etcdEndpoints);
            server.start();
            server.blockUntilShutdown();
        } catch (Exception e) {
            logger.error("Error starting server", e);
            System.exit(1);
        }
    }
}