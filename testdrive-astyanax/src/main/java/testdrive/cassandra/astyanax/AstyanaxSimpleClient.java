package testdrive.cassandra.astyanax;

import java.util.Collections;
import java.util.List;

import com.google.common.base.Supplier;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.cql.CqlFamilyFactory;
import com.netflix.astyanax.cql.JavaDriverConfigBuilder;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

public class AstyanaxSimpleClient {
	AstyanaxContext<Keyspace> context;
	private Keyspace keyspace;
	private ColumnFamily<Integer, String> employees;

	public void initialize() throws Exception {
		final String clusterName = "Test Cluster";
		final String keyspaceName = "simplex";

		final String SEEDS = "localhost";
		final Supplier<List<Host>> HostSupplier = new Supplier<List<Host>>() {

			public List<Host> get() {
				Host host = new Host(SEEDS, 9160);
				return Collections.singletonList(host);
			}
		};

		context = new AstyanaxContext.Builder()
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

		context.start();
		keyspace = context.getClient();

		employees = ColumnFamily.newColumnFamily("employeeinfo",
				IntegerSerializer.get(), StringSerializer.get());
		employees.describe(keyspace);
	}

	public void close() {
		context.shutdown();
	}

	public void query() throws Exception {
		ColumnFamilyQuery<Integer, String> query = keyspace
				.prepareQuery(employees);
		Column<String> column = query.getKey(1).getColumn("lname").execute()
				.getResult();
		System.out.println("last name = " + column.getStringValue());
	}

	public void update() {
		MutationBatch m = keyspace.prepareMutationBatch();
		m.withRow(employees, 1).putColumn("lname", "Bob");
		try {
			m.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		AstyanaxSimpleClient client = new AstyanaxSimpleClient();
		client.initialize();
		try {
			System.out.println("# before update");
			client.query();
			client.update();
			System.out.println("# after update");
			client.query();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			client.close();
		}
	}
}
