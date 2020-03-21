package hadoopEncoder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class EncoderChainLocalMain extends Configured implements Tool {
	public int run(String[] args)throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException{
		String input = args[0];
		String output = args[1];
		String confFile = args[2];
		String confFile2 = args[3];
		String Encoder = args[4];
		String nGOPSize = args[5];
		String fSize = args[6];
		Log LOG = LogFactory.getLog(HMMapperChainLocalPath2.class);
		long fS = Long.parseLong(fSize);
		long nGOP = Long.parseLong(nGOPSize);
		//job 1
		Configuration conf = new Configuration();
		
		JobConf jConf = new JobConf(conf, EncoderChainLocalMain.class);
		
		jConf.set("frameSize", fSize);
		jConf.set("nGOP", nGOPSize);
		jConf.set("outputDirectoryName", output);
		
		jConf.setLong("mapred.max.split.size", fS*nGOP);
		jConf.setLong("mapred.min.split.size", fS*nGOP);
		
		Job job = new Job(jConf, "EncoderMain");
		
		DistributedCache.addCacheFile(new URI(confFile), job.getConfiguration());
		DistributedCache.addCacheFile(new URI(confFile2), job.getConfiguration());
		DistributedCache.addCacheFile(new URI(Encoder), job.getConfiguration());
		
		job.setJarByClass(EncoderChainLocalMain.class);
		job.setMapperClass(HMMapperChainLocalPath1.class);
		
		job.setReducerClass(HMReducerChainLocalPath1.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setInputFormatClass(BinaryFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"path1"));
		
		job.waitForCompletion(true);
		
		///job 2 //set new configuration - +frame 
		Configuration conf2 = new Configuration();
		
		JobConf jConf2 = new JobConf(conf2, EncoderChainLocalMain.class);
		
		LOG.info("reading index file : "+output+"part-r-size");
		
		jConf2.set("frameSize", fSize);
		jConf2.set("nGOP", nGOPSize);
		jConf2.set("outputDirectoryName", output);
		jConf2.set("indexfile", output+"path1/part-r-00000");//index file location & file name origin
		jConf2.set("indexfile1", output+"tempPath/part-r-11111");//2path
		jConf2.setLong("mapred.max.split.size", fS*nGOP);
		jConf2.setLong("mapred.min.split.size", fS*nGOP);
		jConf2.set("localFile", "/home/hadoop/MCcloud/"+input);
		Job job2 = new Job(jConf2, "EncoderMain");
		
		DistributedCache.addCacheFile(new URI(confFile), job2.getConfiguration());
		DistributedCache.addCacheFile(new URI(confFile2), job2.getConfiguration());
		DistributedCache.addCacheFile(new URI(Encoder), job2.getConfiguration());
		
		job2.setJarByClass(EncoderChainLocalMain.class);
		job2.setMapperClass(HMMapperChainLocalPath2.class);
		
		job2.setReducerClass(HMReducerChainLocalPath2.class);
		
		job2.setOutputKeyClass(LongWritable.class);
		job2.setOutputValueClass(BytesWritable.class);
		
		job2.setInputFormatClass(BinaryFileInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]+"path2"));
		
		return job2.waitForCompletion(true) ? 1:0;
		
	}
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException{
		System.out.println("Begin");
		if(args.length != 7){
			System.out.println("Usage : <input> <outputFolder> <confFile> <confFile> <binaryFile> <GOP per frames> <frameSize>");
			return;
		}
		try {
			ToolRunner.run(new Configuration(),new EncoderChainLocalMain(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
