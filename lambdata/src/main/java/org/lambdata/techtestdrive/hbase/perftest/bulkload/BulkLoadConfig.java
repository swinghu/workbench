package org.lambdata.techtestdrive.hbase.perftest.bulkload;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.yaml.snakeyaml.Yaml;

public class BulkLoadConfig {
	public static final String HADOOP_CONF_KEY = "hbase.perftest.bulkload.config";

	public static enum SchemaPattern {
		STANDARD, WIDE, SERIALIZED
	};

	private int varrayColCount;
	private int regionCount;
	SchemaPattern schemaPattern;
	private String tableName;
	private String initDir;
	private String rangeDir;
	private String hfileDir;

	public int getVarrayColCount() {
		return varrayColCount;
	}

	public void setVarrayColCount(int varrayColCount) {
		this.varrayColCount = varrayColCount;
	}

	public int getRegionCount() {
		return regionCount;
	}

	public void setRegionCount(int regionCount) {
		this.regionCount = regionCount;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getInitDir() {
		return initDir;
	}

	public void setInitDir(String initDir) {
		this.initDir = initDir;
	}

	public String getRangeDir() {
		return rangeDir;
	}

	public void setRangeDir(String rangeDir) {
		this.rangeDir = rangeDir;
	}

	public String getHfileDir() {
		return hfileDir;
	}

	public void setHfileDir(String hfileDir) {
		this.hfileDir = hfileDir;
	}

	public String getSchemaPattern() {
		return schemaPattern.toString();
	}

	public void setSchemaPattern(String schemaPattern) {
		this.schemaPattern = SchemaPattern.valueOf(schemaPattern);
	}

	public SchemaPattern schemaPatternAsEnum() {
		return this.schemaPattern;
	}

	public static BulkLoadConfig fromYamlFile(String path) {
		BulkLoadConfig result = null;
		Yaml yaml = new Yaml();

		try {
			InputStream ios = new FileInputStream(new File(path));
			result = (BulkLoadConfig) yaml.loadAs(ios, BulkLoadConfig.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	public String toJson() {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.writeValueAsString(this);
		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static BulkLoadConfig fromJsonString(String jsonConfig) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.readValue(jsonConfig, BulkLoadConfig.class);
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args) {
		BulkLoadConfig config = BulkLoadConfig
				.fromYamlFile("src/main/config/table1.yml");
		System.out.println(config.toJson());
		System.out.println(fromJsonString(config.toJson()));
	}
}
