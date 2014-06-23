package testdrive.cassandra.astyanax;

import java.util.Collections;
import java.util.List;

import com.google.common.base.Supplier;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.cql.CqlFamilyFactory;
import com.netflix.astyanax.cql.JavaDriverConfigBuilder;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

public class AstyanaxSimpleClient {

	private Keyspace keyspace;

	public void initialize() {
		final String clusterName = "Test Cluster";
		final String keyspaceName = "simplex";

		final String SEEDS = "localhost";
		final Supplier<List<Host>> HostSupplier = new Supplier<List<Host>>() {

			public List<Host> get() {
				Host host = new Host(SEEDS, 9160);
				return Collections.singletonList(host);
			}
		};

		AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
				.forCluster(clusterName)
				.forKeyspace(keyspaceName)
				.withHostSupplier(HostSupplier)
				.withAstyanaxConfiguration(
						new AstyanaxConfigurationImpl().setDiscoveryType(
								NodeDiscoveryType.DISCOVERY_SERVICE)
								.setDiscoveryDelayInSeconds(60000))
				.withConnectionPoolConfiguration(
						new JavaDriverConfigBuilder().build())
				.buildKeyspace(CqlFamilyFactory.getInstance());

		keyspace = context.getClient();
	}

	public void query() throws Exception {
		ColumnFamily<Integer, String> CF_EMPLOYEES = new ColumnFamily<Integer, String>(
				"EmployeeInfo", IntegerSerializer.get(), StringSerializer.get());
		keyspace.prepareQuery(CF_EMPLOYEES)
				.withCql(
						"CREATE TABLE EmployeeInfo (id int PRIMARY KEY, fname text, lname text, age int, salary bigint)")
				.execute();
	}

	public static void main(String[] args) throws Exception {
		AstyanaxSimpleClient client = new AstyanaxSimpleClient();
		client.initialize();
		client.query();
	}
}
