package org.lambdata.techtestdrive.cassandra.javadriver;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class JavaDriverSimpleClient {
	private Cluster cluster;
	private Session session;

	public void connect(String node) {
		cluster = Cluster.builder().addContactPoint(node).build();
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n",
				metadata.getClusterName());
		for (Host host : metadata.getAllHosts()) {
			System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
					host.getDatacenter(), host.getAddress(), host.getRack());
		}
		session = cluster.connect();
	}

	public void dropSchema() {
		session.execute("DROP KEYSPACE simplex");
	}

	public void createSchema() {
		session.execute("CREATE KEYSPACE simplex WITH replication "
				+ "= {'class':'SimpleStrategy', 'replication_factor':3};");
		session.execute("CREATE TABLE simplex.songs (" + "id uuid PRIMARY KEY,"
				+ "title text," + "album text," + "artist text,"
				+ "tags set<text>," + "data blob" + ");");
		session.execute("CREATE TABLE simplex.playlists (" + "id uuid,"
				+ "title text," + "album text, " + "artist text,"
				+ "song_id uuid," + "PRIMARY KEY (id, title, album, artist)"
				+ ");");
	}

	/**
	 * Index must be created. otherwise, the following exception will be thrown:
	 * No indexed columns present in by-columns clause with Equal operator.
	 */
	public void createIndex() {
		session.execute("CREATE INDEX ON simplex.songs( album )");
	}

	public void loadData() {
		loadDataByBound();
	}

	public void loadDataByBound() {
		PreparedStatement statement = session
				.prepare("INSERT INTO simplex.songs "
						+ "(id, title, album, artist, tags) "
						+ "VALUES (?, ?, ?, ?, ?);");
		BoundStatement boundStatement = new BoundStatement(statement);
		Set<String> tags = new HashSet<String>();
		tags.add("jazz");
		tags.add("2013");
		session.execute(boundStatement.bind(
				UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50"),
				"La Petite Tonkinoise", "Bye Bye Blackbird", "Josephine Baker",
				tags));
		statement = session.prepare("INSERT INTO simplex.playlists "
				+ "(id, song_id, title, album, artist) "
				+ "VALUES (?, ?, ?, ?, ?);");
		boundStatement = new BoundStatement(statement);
		session.execute(boundStatement.bind(
				UUID.fromString("2cc9ccb7-6221-4ccb-8387-f22b6a1b354d"),
				UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50"),
				"La Petite Tonkinoise", "Bye Bye Blackbird", "Josephine Baker"));
	}

	public void loadDataByRegular() {
		session.execute("INSERT INTO simplex.songs (id, title, album, artist, tags) "
				+ "VALUES ("
				+ "756716f7-2e54-4715-9f00-91dcbea6cf50,"
				+ "'La Petite Tonkinoise',"
				+ "'Bye Bye Blackbird',"
				+ "'Josephine Baker'," + "{'jazz', '2013'})" + ";");
		session.execute("INSERT INTO simplex.playlists (id, song_id, title, album, artist) "
				+ "VALUES ("
				+ "2cc9ccb7-6221-4ccb-8387-f22b6a1b354d,"
				+ "756716f7-2e54-4715-9f00-91dcbea6cf50,"
				+ "'La Petite Tonkinoise',"
				+ "'Bye Bye Blackbird',"
				+ "'Josephine Baker'" + ");");
	}

	public void querySchema() {
		ResultSet results = session.execute("SELECT * FROM simplex.playlists "
				+ "WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");
		System.out
				.println(String
						.format("%-30s\t%-20s\t%-20s\n%s", "title", "album",
								"artist",
								"-------------------------------+-----------------------+--------------------"));
		for (Row row : results) {
			System.out.println(String.format("%-30s\t%-20s\t%-20s",
					row.getString("title"), row.getString("album"),
					row.getString("artist")));
		}
	}

	public void getRows(String keyspace, String table) {
		String albumName = "Bye Bye Blackbird";
		Statement statement = QueryBuilder.select().all().from(keyspace, table)
				.where(eq("album", albumName));
		List<Row> rows = session.execute(statement).all();
		System.out.println("There are totally " + rows.size()
				+ " songs from album " + albumName);
		System.out
				.println("-------------------------------+-----------------------");
		for (Row row : rows) {
			System.out.println(String.format("%-30s\t%-20s",
					row.getString("title"), row.getString("album")));
		}
	}

	public void close() {
		cluster.close();
	}

	public static void main(String[] args) {
		JavaDriverSimpleClient client = new JavaDriverSimpleClient();
		client.connect("127.0.0.1");
		// client.dropSchema();
		client.createSchema();
		// Note: We must create the index before loading data, otherwise
		// the loaded data before index creation will not be return when
		// queried.
		client.createIndex();
		client.loadData();
		client.querySchema();
		client.getRows("simplex", "songs");
		client.dropSchema();
		client.close();
	}
}
