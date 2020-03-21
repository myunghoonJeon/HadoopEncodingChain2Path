package hadoopEncoder;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class BinaryFileInputFormat extends FileInputFormat<Text, BytesWritable>{

	@Override
	public RecordReader<Text, BytesWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return new WholeFileRecordReader((FileSplit) split, context.getConfiguration());
	}

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		// TODO Auto-generated method stub
		return true;
	}
}

class WholeFileRecordReader extends RecordReader <Text, BytesWritable> {

	boolean isProcessed = false;
	private FileSplit fileSplit;
	private Configuration conf;
	private boolean processed = false;
	private long offset = 0;
	WholeFileRecordReader (FileSplit fileSplit, Configuration conf){
		this.fileSplit = fileSplit;
		this.conf = conf;
	}
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Text getCurrentKey() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return new Text(fileSplit.getPath().toString());
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException,
		InterruptedException {
		byte [] contents = new byte[2048];
		Path file = fileSplit.getPath();
		FileSystem fs = file.getFileSystem(conf);
		
		FSDataInputStream in = null;
		try {
			in = fs.open(file);
			IOUtils.readFully(in, contents, 0, 2048);
			return new BytesWritable(contents);
		} finally {
			IOUtils.closeStream(in);
		}
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return  (float) offset / fileSplit.getLength();
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(!isProcessed){
			isProcessed = true;
			return true;
		}
		return false;
	}
}

