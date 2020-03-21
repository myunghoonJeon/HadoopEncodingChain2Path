
package hadoopEncoder;

import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


class JniMe{
	native public static int[] Me(String str);
	static{
		System.load("/home/hadoop/video/libMotionEstimation.so");
	}
}

public class HMMapperChainLocalPath1 extends Mapper<Text, BytesWritable, IntWritable, LongWritable>{
	private final static Log LOG = LogFactory.getLog(HMMapperChainLocalPath1.class);
	private Path[] localFiles;
//	private final static BytesWritable bTemp = new BytesWritable();
	private final static LongWritable offsetResultLong = new LongWritable();
//	private final static IntWritable meResult = new IntWritable();
//	private final static IntWritable bufferLength = new IntWritable();
	private final static Text frmaeNumberText = new Text();
	private final static IntWritable MotionEstimationResultInt = new IntWritable();
	private final static Text offsetText = new Text();
//	private Text word = new Text();
	boolean isProcessed = false;
	
    public void setup (Context context) throws IOException{
    	localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
    }
	
	public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException{
		if(!isProcessed){
//			System.load("/home/hadoop/video/libMotionEstimation.so");
			isProcessed = true;
			Runtime.getRuntime().exec("sudo ldconfig -v");
//			System.out.println("enter sudo ldconfig -v");
			
			//String taskId = context.getConfiguration().get("mapred.task.id");
//			long Length = ((FileSplit)context.getInputSplit()).getLength();
			long Length = (long)(3840*2160*1.5);
			Path fPath = ((FileSplit)context.getInputSplit()).getPath();
    		FileSystem fs = fPath.getFileSystem(context.getConfiguration());
    		FileSplit file = (FileSplit) context.getInputSplit();
    		long fOffset = file.getStart();
    		long frameNumber = fOffset/Length;
    		long addOffset = 0;
    		long term = 8;
    		offsetResultLong.set(fOffset);
//    		frameNumberInt.set(Integer.parseInt(frameNumber+""));
    		frmaeNumberText.set(frameNumber+"");
    		offsetText.set(fOffset+"");
    		
    		String fileOffset = new String(""+file.getStart());
		    		
    		String localInput = fileOffset+".yuv";

    		
    		
    		//For Debug
//    		System.out.println("Local file name : "+localInput);
//    		System.out.println(localOutput);
//    		System.out.println(localOutput2);
    		System.out.println(Length);
			
    		//fs.copyToLocalFile(fPath, new Path(localInput));
    		byte[] buffer = new byte[1024];
    		long byteCount = 0;
    		
    		FSDataInputStream inputStream = fs.open(fPath);

//    		LOG.info("PATH1 : select 5 frame term 8 : "+frameNumber);
    		FileOutputStream out = new FileOutputStream(localInput);
    		for(int i=0;i<5;i++){// 1frame read
    			byteCount = 0;
    			addOffset = i*term*Length;
    			if(i==4){
    				addOffset -= Length;
    			}
    			inputStream.seek(file.getStart()+addOffset);
	    		while(inputStream.read(buffer) != -1){
	    			out.write(buffer);
	    			byteCount += 1024;
	    			if(byteCount == Length){
//	    				LOG.info(byteCount + " bytes sucessfully read");
	    				break;
	    			}
	    		}//while read
    		}//for
    		inputStream.close();
    		out.close();
    		
    		///////////////JNI CALL
    		LOG.info("PATH1 : JNI START : "+frameNumber);
    		int[] resultJni = JniMe.Me(localInput);// execution JNI Motion Estimation and set split point
    		int tmp=0;
    		int avg;
    		for(int i=0;i<resultJni.length;i++){
    			tmp+=resultJni[i];// sum of 5 ME
    		}
    		avg = tmp/resultJni.length;
    		MotionEstimationResultInt.set(avg);
//    		LOG.info("Average : "+avg+" starting index : "+offsetResultLong+ " : "+frameNumber);
    		context.write(MotionEstimationResultInt, offsetResultLong); // key - Motion Estimation Result, value - Offset = descending ordering(auto)
		}
	}
}
