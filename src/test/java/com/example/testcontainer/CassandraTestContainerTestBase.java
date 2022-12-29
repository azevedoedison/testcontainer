package com.example.testcontainer;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.TestcontainersConfiguration;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetSocketAddress;
import java.time.Duration;

@SpringBootTest
@Testcontainers
class CassandraTestContainerTestBase {

	@Container
	public static final CassandraContainer<?> cassandra = new CassandraContainer<>("cassandra:3.11.2").withReuse(true);
	
	private static CqlSession session;
	private static final String KEYSPACE_NAME = "test";
	private static String datacenter = "datacenter1";
	private static final int REQUEST_TIMEOUT = 12000;

	@BeforeAll
	public static void startCassandraContainer() {
		TestcontainersConfiguration.getInstance().updateUserConfig("testcontainers.reuse.enable", "true");
		System.setProperty("spring.data.cassandra.contact-points", cassandra.getHost());
		System.setProperty("spring.data.cassandra.port", String.valueOf(cassandra.getMappedPort(9042)));
		System.setProperty("spring.data.cassandra.local-datacenter", datacenter);
		assumeTrue(DockerClientFactory.instance().isDockerAvailable());

		session = CqlSession.builder()
				.addContactPoint(new InetSocketAddress(cassandra.getHost(), cassandra.getMappedPort(9042)))
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
		insertData();
	}

	static void createKeySpace() {
		getSession().execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE_NAME + " WITH replication = \n"
				+ "{'class':'SimpleStrategy','replication_factor':'1'};");
	}

	static void selectKeySpace() {
		getSession().execute("USE " + KEYSPACE_NAME);
	}

	static void createTable() {
		getSession().execute("CREATE TABLE if not exists User( id uuid PRIMARY KEY, name text, address text, age int);");
	}

	static void insertData() {
		getSession().execute(
				"insert into user (id, name, address, age) values (78ac5be4-5394-11ed-bdc3-0242ac120002,'Edison','Curitiba', 36);");
		getSession().execute(
				"insert into user (id, name, address, age) values (9ba65442-5394-11ed-bdc3-0242ac120002,'Paulo','Florianópolis', 50);");
		getSession().execute(
				"insert into user (id, name, address, age) values (c1d6ac5c-5394-11ed-bdc3-0242ac120002,'Micael','Blumenau', 27);");
		getSession().execute(
				"insert into user (id, name, address, age) values (cc842e38-df0f-45d1-bbf0-c516863b9f73,'Barbara','Timbó', 20);");
	}


	@Test
	@DisplayName("Should verify if cassandra is running")
	void givenCassandraContainer_whenSpringContextIsBootstrapped_thenContainerIsRunningWithNoExceptions() {
		assertThat(cassandra.isRunning()).isTrue();
	}

	@Test
	@DisplayName("Should verify if the table was created and have data")
	void testTableCreation() {
		assertFalse(getSession().execute("select * from test.user").all().isEmpty());
	}
	
	@Test
	void testInsertData() {
		assertThat(getSession().execute(
				"insert into user (id, name, address, age) values (9433e0e6-8768-11ed-a1eb-0242ac120002,'Flavio','Blumenau', 60);"));
		
		assertFalse(getSession().execute("select * from test.user where id = 9433e0e6-8768-11ed-a1eb-0242ac120002").all().isEmpty());
	}
	
	@AfterAll
    public static void dropTable() {
        getSession().execute("Drop table If exists test.user");
    }

}
