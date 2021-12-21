package muti.kafka.spark.avro.integration.avro;

import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AvroSchemaLoader.
 *
 * @author Capgemini Italia S.p.A.
 */
public class AvroSchemaLoader {

	private static final Logger LOGGER = LoggerFactory.getLogger(AvroSchemaLoader.class);

	private static final String SCHEMA_EXTENSION = "avsc";

	private final ClassLoader cl;

	/**
	 * @param cl
	 */
	public AvroSchemaLoader() {

		super();
		this.cl = getClass().getClassLoader();
	}

	/**
	 * @param key
	 * 
	 * @return
	 */
	private String getResource(final Class<?> key) {
		return String.format("avro/%s.%s", key.getName(), SCHEMA_EXTENSION);
	}

	/**
	 * @param key
	 * @return
	 */
	public Schema load(final Class<?> key) {

		Schema schema = load(key, getResource(key));

		if (schema == null) {
			LOGGER.warn("avro schema for resource: {} not found", getResource(key));
		}
		return schema;
	}

	/**
	 * @param c
	 * @param name
	 * 
	 * @return
	 * 
	 */
	private Schema load(final Class<?> c, final String name) {
		try (InputStream resource = cl.getResourceAsStream(name)) {
			if (resource != null) {
				return new Schema.Parser().parse(resource);
			}
		}
		catch (IOException e) {
			LOGGER.warn("Cannot load avro schema for {} from resource {}", c.getName(), name, e);
		}

		return null;
	}

}