package com.example.testcontainer;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;

import eu.rekawek.toxiproxy.model.ToxicDirection;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.TestcontainersConfiguration;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetSocketAddress;
import java.time.Duration;

@SpringBootTest
@Testcontainers
class CassandraTestContainerTestBaseToxiProxy {

	public static Network network = Network.newNetwork();
	private static CqlSession session;
	private static final String KEYSPACE_NAME = "test";
	private static String datacenter = "datacenter1";
	private static final int REQUEST_TIMEOUT = 12000;

	@Container
	public static final CassandraContainer<?> cassandra = new CassandraContainer<>("cassandra:3.11.2").withReuse(true)
			.withNetwork(network);

	@Container
	private static final ToxiproxyContainer toxiproxy = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.5.0")
			.withNetwork(network);
	
	private static ToxiproxyContainer.ContainerProxy proxyToCassandra;

	@BeforeAll
	public static void startCassandraContainer() {
		proxyToCassandra = toxiproxy.getProxy(cassandra, 9042);
		TestcontainersConfiguration.getInstance().updateUserConfig("testcontainers.reuse.enable", "true");
		System.setProperty("spring.data.cassandra.contact-points", proxyToCassandra.getContainerIpAddress());
		System.setProperty("spring.data.cassandra.port", String.valueOf(proxyToCassandra.getProxyPort()));
		System.setProperty("spring.data.cassandra.local-datacenter", datacenter);
		assumeTrue(DockerClientFactory.instance().isDockerAvailable());

		session = CqlSession.builder()
				.addContactPoint(new InetSocketAddress(proxyToCassandra.getContainerIpAddress(), proxyToCassandra.getProxyPort()))
				.withLocalDatacenter(datacenter)
				.withConfigLoader(DriverConfigLoader.programmaticBuilder()
						.withDuration(DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT,
								Duration.ofMillis(REQUEST_TIMEOUT))
						.withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT,
								Duration.ofMillis(REQUEST_TIMEOUT))
						.withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofMillis(REQUEST_TIMEOUT)).build())
				.build();
		createEnviroment();

	}

	public static CqlSession getSession() {
		return session;
	}

	static void createEnviroment() {
		createKeySpace();
		selectKeySpace();
		createTable();
	}

	static void createKeySpace() {
		getSession().execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE_NAME + " WITH replication = \n"
				+ "{'class':'SimpleStrategy','replication_factor':'1'};");
	}

	static void selectKeySpace() {
		getSession().execute("USE " + KEYSPACE_NAME);
	}

	static void createTable() {
		getSession().execute("Drop table If exists test.user");
		getSession().execute("CREATE TABLE if not exists User( id uuid PRIMARY KEY, name text, address text, age int);");
		
	}


	@Test
	@Retryable(value = DriverTimeoutException.class, maxAttempts = 5, backoff = @Backoff(delay = 1000))
	void testInsertDataToxi() throws Exception{
		assertThat(getSession().execute(
				"insert into user (id, name, address, age) values (9433e0e6-8768-11ed-a1eb-0242ac120002,'Flavio','Blumenau', 60);"));
		
		proxyToCassandra.toxics().latency("extra_latency", ToxicDirection.UPSTREAM, 13000);
         
        assertThrows(DriverTimeoutException.class, () -> { 
        	getSession().execute(
        			"insert into user (id, name, address, age) values (e48800c6-876e-11ed-a1eb-0242ac120002,'Eduardo','Blumenau', 36);");
        });   
		
	}


}
