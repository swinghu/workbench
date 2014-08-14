package org.lambdata.techtestdrive.lucene;

import java.io.File;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class Searcher {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			throw new IllegalArgumentException("Usage: java "
					+ Searcher.class.getName() + " <index dir> <query>");
		}

		String indexDir = args[0];
		String q = args[1];

		search(indexDir, q);
	}

	public static void search(String indexDir, String q) throws Exception {

		IndexReader reader = DirectoryReader.open(FSDirectory.open(new File(
				indexDir)));
		IndexSearcher is = new IndexSearcher(reader);

		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_48);
		String field = "contents";
		QueryParser parser = new QueryParser(Version.LUCENE_48, field, analyzer);
		Query query = parser.parse(q);
		long start = System.currentTimeMillis();
		TopDocs hits = is.search(query, 10);
		long end = System.currentTimeMillis();

		System.err.println("Found " + hits.totalHits + " document(s) (in "
				+ (end - start) + " milliseconds) that matched query '" + q
				+ "':");

		for (ScoreDoc scoreDoc : hits.scoreDocs) {
			Document doc = is.doc(scoreDoc.doc);
			System.out.println(doc.get("fullpath"));
		}

		reader.close();
	}
}