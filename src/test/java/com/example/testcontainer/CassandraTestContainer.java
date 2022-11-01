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
		createEnviroment(cassandra.getCluster());
	}

	static void createEnviroment(Cluster cluster) {
		try (Session session = cluster.connect()) {
			createKeySpace(session);
			selectKeySpace(session);
			createTable(session);
			insertData(session);
		}
	}
	
	static void createKeySpace(Session session) {
		session.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE_NAME + " WITH replication = \n"
					+ "{'class':'SimpleStrategy','replication_factor':'1'};");		
	}
	
	static void selectKeySpace(Session session) {
		session.execute("USE " + KEYSPACE_NAME);		
	}
	
	static void createTable(Session session) {
		session.execute("CREATE TABLE User( id uuid PRIMARY KEY, name text, address text, age int);");		
	}
	
	static void insertData(Session session) {		
		session.execute("insert into user (id, name, address, age) values (78ac5be4-5394-11ed-bdc3-0242ac120002,'Edison','Curitiba', 36);");
		session.execute("insert into user (id, name, address, age) values (9ba65442-5394-11ed-bdc3-0242ac120002,'Paulo','Florianópolis', 50);");
		session.execute("insert into user (id, name, address, age) values (c1d6ac5c-5394-11ed-bdc3-0242ac120002,'Micael','Blumenau', 27);");
		session.execute("insert into user (id, name, address, age) values (cc842e38-df0f-45d1-bbf0-c516863b9f73,'Barbara','Timbó', 20);");
	}
	
	

	@Test
	void givenCassandraContainer_whenSpringContextIsBootstrapped_thenContainerIsRunningWithNoExceptions() {
		assertThat(cassandra.isRunning()).isTrue();
	}

}
