package hadoopEncoder;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HMReducer extends Reducer<LongWritable, BytesWritable, Text, LongWritable>{
	private LongWritable sumWritable = new LongWritable(10);
	String filename;
	
	
	public void reduce (LongWritable key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException{
		//long sum=0;
		//FileSystem hdfs = FileSystem.get(context.getConfiguration());
		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataOutputStream fileWrite;
		filename = context.getConfiguration().get("outputDirectoryName") + "/result.bin";
		System.out.println(filename);
		
		if(fs.get(context.getConfiguration()).exists(new Path(filename))){
			System.out.println("APPEND");
			fileWrite = FileSystem.get(context.getConfiguration()).append(new Path(filename));
		}else{
			System.out.println("CREATE");
			fileWrite = FileSystem.get(context.getConfiguration()).create(new Path(filename),true);
		}
		
		for (BytesWritable val : values){
			Text temp = new Text(key.toString());
		//System.out.println();
			context.write(temp, sumWritable);
			fileWrite.write(val.getBytes(), 0, val.getLength());
		}
		fileWrite.close();
	}
}
