package org.lambdata.techtestdrive.crunch;

import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A word count example for Apache Crunch, based on Crunch's example projects.
 */
public class MemWordCount extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new MemWordCount(), args);
	}

	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.err
					.println("Usage: java this.class <input> <output>");
			System.err.println();
			return 1;
		}

		String inputPath = args[0];
		String outputPath = args[1];

		// Create an object to coordinate pipeline creation and execution.
		Pipeline pipeline = MemPipeline.getInstance();
		
		// Reference a given text file as a collection of Strings.
		PCollection<String> lines = pipeline.readTextFile(inputPath);

		// Define a function that splits each line in a PCollection of Strings
		// into
		// a PCollection made up of the individual words in the file.
		// The second argument sets the serialization format.
		PCollection<String> words = lines.parallelDo(new Tokenizer(),
				Writables.strings());

		// Take the collection of words and remove known stop words.
		PCollection<String> noStopWords = words.filter(new StopWordFilter());

		// The count method applies a series of Crunch primitives and returns
		// a map of the unique words in the input PCollection to their counts.
		PTable<String, Long> counts = noStopWords.count();

		// Instruct the pipeline to write the resulting counts to a text file.
		pipeline.writeTextFile(counts, outputPath);

		// Execute the pipeline as a MapReduce.
		PipelineResult result = pipeline.done();

		return result.succeeded() ? 0 : 1;
	}
}
