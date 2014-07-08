package testdrive.javaessentials.json.simple;

import java.io.File;
import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class JacksonSimpleExample {

	public static void toJson() {
		User user = new User();
		ObjectMapper mapper = new ObjectMapper();

		try {
			// convert user object to json string, and save to a file
			mapper.writeValue(new File("target/user.json"), user);
			// display to console
			System.out.println(mapper.writeValueAsString(user));
		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void fromJson() {

		ObjectMapper mapper = new ObjectMapper();

		try {
			// read from file, convert it to user class
			User user = mapper.readValue(new File("target/user.json"), User.class);
			// display to console
			System.out.println(user);
		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
	 toJson();
	 fromJson();
	}
}
