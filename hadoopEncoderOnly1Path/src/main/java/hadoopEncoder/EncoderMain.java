package hadoopEncoder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileAsBinaryOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class EncoderMain {
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException{

		if(args.length != 7){
			System.out.println("Usage : <input> <outputFolder> <confFile> <confFile> <binaryFile> <GOP per frames> <frameSize>");
			return;
		}
		
		String input = args[0];
		String output = args[1];
		String confFile = args[2];
		String confFile2 = args[3];
		String Encoder = args[4];
		String nGOPSize = args[5];
		String fSize = args[6];
		
		long fS = Long.parseLong(fSize);
		long nGOP = Long.parseLong(nGOPSize);
		
		Configuration conf = new Configuration();
		JobConf jConf = new JobConf(conf, EncoderMain.class);
		
		jConf.set("frameSize", fSize);
		jConf.set("nGOP", nGOPSize);
		jConf.set("outputDirectoryName", output);
		
		jConf.setLong("mapred.max.split.size", fS*nGOP);
		jConf.setLong("mapred.min.split.size", fS*nGOP);
		
		Job job = new Job(jConf, "EncoderMain");
		
		DistributedCache.addCacheFile(new URI(confFile), job.getConfiguration());
		DistributedCache.addCacheFile(new URI(confFile2), job.getConfiguration());
		DistributedCache.addCacheFile(new URI(Encoder), job.getConfiguration());
		
		job.setJarByClass(EncoderMain.class);
		job.setMapperClass(HMMapperOnlyPath1.class);
		
		job.setReducerClass(HMReducerOnlyPath1.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(BinaryFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
	
}
