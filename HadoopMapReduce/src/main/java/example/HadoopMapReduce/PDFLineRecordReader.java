package example.HadoopMapReduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

public class PDFLineRecordReader extends RecordReader<LongWritable, Text> {

	private LongWritable key = new LongWritable();
	private Text value = new Text();
	private int currentLine = 0;
	private List<String> lines = null;

	private PDDocument doc = null;
	private PDFTextStripper textStripper = null;

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		// If done close the doc
		if (doc != null) {
			doc.close();
		}
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return (100.0f / lines.size() * currentLine) / 100.0f;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		FileSplit fileSplit = (FileSplit) split;
		final Path file = fileSplit.getPath();

		Configuration conf = context.getConfiguration();
		FileSystem fs = file.getFileSystem(conf);
		FSDataInputStream filein = fs.open(fileSplit.getPath());

		if (filein != null) {

			doc = PDDocument.load(filein);

			// Konnte das PDF gelesen werden?
			if (doc != null) {
				textStripper = new PDFTextStripper();
				String text = textStripper.getText(doc);

				lines = Arrays.asList(text.split(System.lineSeparator()));
				currentLine = 0;

			}

		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (key == null) {
			key = new LongWritable();
		}

		if (value == null) {
			value = new Text();
		}

		if (currentLine < lines.size()) {
			String line = lines.get(currentLine);

			key.set(currentLine);

			value.set(line);
			currentLine++;

			return true;
		} else {

			// All lines are read? -> end
			key = null;
			value = null;
			return false;
		}
	}

}
