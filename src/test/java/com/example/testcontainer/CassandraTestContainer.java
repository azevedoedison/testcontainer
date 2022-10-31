package com.example.testcontainer;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Testcontainers
class CassandraTestContainer {

	private static final String KEYSPACE_NAME = "test";	

	
	@Container
    public static final CassandraContainer cassandra = new CassandraContainer(DockerImageName.parse("cassandra:latest"));

	@BeforeAll
	static void setupCassandraConnectionProperties() {
		System.setProperty("spring.data.cassandra.keyspace-name", KEYSPACE_NAME);
		System.setProperty("spring.data.cassandra.contact-points", cassandra.getHost());		
		createKeyspace(cassandra.getCluster());
	}

	static void createKeyspace(Cluster cluster) {
		try (Session session = cluster.connect()) {
			session.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE_NAME + " WITH replication = \n"
					+ "{'class':'SimpleStrategy','replication_factor':'1'};");
		}
	}

	@Test
	void givenCassandraContainer_whenSpringContextIsBootstrapped_thenContainerIsRunningWithNoExceptions() {
		assertThat(cassandra.isRunning()).isTrue();
	}

}
