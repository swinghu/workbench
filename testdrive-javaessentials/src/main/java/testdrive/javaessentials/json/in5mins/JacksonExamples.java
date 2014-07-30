package testdrive.javaessentials.json.in5mins;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import testdrive.javaessentials.json.in5mins.User.Gender;
import testdrive.javaessentials.json.in5mins.User.Name;

public class JacksonExamples {
	public static ObjectMapper mapper = new ObjectMapper(); // can reuse, share
															// globally

	public static void fullDataBinding() {
		try {
			User user = mapper
					.readValue(new File("data/user.json"), User.class);
			System.out.println(user);
			mapper.writeValue(new File("target/user-modified.json"), user);
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void simpleDataBinding() {
		try {
			// read
			Map<String, Object> userData = mapper.readValue(new File(
					"data/user.json"), Map.class);
			System.out.println(userData);

			// write
			userData = new HashMap<String, Object>();
			Map<String, String> nameStruct = new HashMap<String, String>();
			nameStruct.put("first", "Joe");
			nameStruct.put("last", "Sixpack");
			userData.put("name", nameStruct);
			userData.put("gender", "MALE");
			userData.put("verified", Boolean.FALSE);
			userData.put("userImage", "Rm9vYmFyIQ==");
			mapper.writeValue(new File("target/user-modified.json"), userData);
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void modifyTree() {
		// can either use mapper.readTree(source), or mapper.readValue(source,
		// JsonNode.class);
		JsonNode rootNode;
		try {
			rootNode = mapper.readTree(new File("data/user.json"));
			// ensure that "last name" isn't "Xmler"; if is, change to "Jsoner"
			JsonNode nameNode = rootNode.path("name");
			String lastName = nameNode.path("last").getTextValue();
			if ("Xmler".equalsIgnoreCase(lastName)) {
				((ObjectNode) nameNode).put("last", "Jsoner");
			}
			// and write it out:
			mapper.writeValue(new File("target/user-modified.json"), rootNode);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void constructTree() {
		ObjectNode userRoot = mapper.createObjectNode();
		ObjectNode nameOb = userRoot.putObject("name");
		nameOb.put("first", "Joe");
		nameOb.put("last", "Sixpack");
		userRoot.put("gender", User.Gender.MALE.toString());
		userRoot.put("verified", false);
		byte[] imageData = null; // or wherever it comes from
		userRoot.put("userImage", imageData);
		try {
			mapper.writeValue(new File("target/user-constructed.json"),
					userRoot);
		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void streamingWrite() throws Exception {
		JsonFactory f = new JsonFactory();
		JsonGenerator g = f.createJsonGenerator(new File("target/user.json"),
				JsonEncoding.UTF8);

		g.writeStartObject();
		g.writeObjectFieldStart("name");
		g.writeStringField("first", "Joe");
		g.writeStringField("last", "Sixpack");
		g.writeEndObject(); // for field 'name'
		g.writeStringField("gender", Gender.MALE.toString());
		g.writeBooleanField("verified", false);
		g.writeFieldName("userImage"); // no 'writeBinaryField' (yet?)
		byte[] binaryData = new byte[] {};
		g.writeBinary(binaryData);
		g.writeEndObject();
		g.close();
		// force flushing of output, close underlying output stream
	}

	public static void streamingParse() throws Exception {
		JsonFactory f = new JsonFactory();
		JsonParser jp = f.createJsonParser(new File("target/user.json"));
		User user = new User();
		jp.nextToken(); // will return JsonToken.START_OBJECT (verify?)
		while (jp.nextToken() != JsonToken.END_OBJECT) {
			String fieldname = jp.getCurrentName();
			jp.nextToken(); // move to value, or START_OBJECT/START_ARRAY
			if ("name".equals(fieldname)) { // contains an object
				Name name = new Name();
				while (jp.nextToken() != JsonToken.END_OBJECT) {
					String namefield = jp.getCurrentName();
					jp.nextToken(); // move to value
					if ("first".equals(namefield)) {
						name.setFirst(jp.getText());
					} else if ("last".equals(namefield)) {
						name.setLast(jp.getText());
					} else {
						throw new IllegalStateException("Unrecognized field '"
								+ fieldname + "'!");
					}
				}
				user.setName(name);
			} else if ("gender".equals(fieldname)) {
				user.setGender(User.Gender.valueOf(jp.getText()));
			} else if ("verified".equals(fieldname)) {
				user.setVerified(jp.getCurrentToken() == JsonToken.VALUE_TRUE);
			} else if ("userImage".equals(fieldname)) {
				user.setUserImage(jp.getBinaryValue());
			} else {
				throw new IllegalStateException("Unrecognized field '"
						+ fieldname + "'!");
			}
		}
		jp.close();
		// ensure resources get cleaned up timely and properly
	}

	public static void main(String[] args) throws Exception {
		streamingWrite();
		streamingParse();
	}
}
