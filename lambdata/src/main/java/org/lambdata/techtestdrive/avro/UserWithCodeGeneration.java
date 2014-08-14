package org.lambdata.techtestdrive.avro;

import java.io.File;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

public class UserWithCodeGeneration {

	private static User user1;
	private static User user2;
	private static User user3;

	public static void createUser() {
		// Property setter
		user1 = new User();
		user1.setName("Alyssa");
		user1.setFavoriteNumber(256);
		// Leave favorite color null

		// Alternate constructor
		user2 = new User("Ben", 7, "red");

		// Construct via builder
		user3 = User.newBuilder().setName("Charlie").setFavoriteColor("blue")
				.setFavoriteNumber(null).build();
	}

	public static void serialize() throws Exception {
		// Serialize user1 and user2 to disk
		File file = new File("target/specific-users.avro");
		DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(
				User.class);
		DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(
				userDatumWriter);
		dataFileWriter.create(user1.getSchema(), file);
		dataFileWriter.append(user1);
		dataFileWriter.append(user2);
		dataFileWriter.append(user3);
		dataFileWriter.close();
	}

	public static void deserialize() throws Exception {
		File file = new File("target/specific-users.avro");
		// Deserialize Users from disk
		DatumReader<User> userDatumReader = new SpecificDatumReader<User>(
				User.class);
		DataFileReader<User> dataFileReader = new DataFileReader<User>(file,
				userDatumReader);
		User user = null;
		while (dataFileReader.hasNext()) {
			// Reuse user object by passing it to next(). This saves us from
			// allocating and garbage collecting many objects for files with
			// many items.
			user = dataFileReader.next(user);
			System.out.println(user);
		}
	}

	public static void main(String[] args) throws Exception {
		createUser();
		serialize();
		deserialize();
	}
}
