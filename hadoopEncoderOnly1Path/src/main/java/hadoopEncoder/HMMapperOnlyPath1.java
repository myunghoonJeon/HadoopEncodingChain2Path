
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

public class HMMapperOnlyPath1 extends Mapper<Text, BytesWritable, IntWritable, IntWritable>{
	private final static Log LOG = LogFactory.getLog(HMMapperOnlyPath1.class);
	private Path[] localFiles;
//	private final static BytesWritable bTemp = new BytesWritable();
	private final static LongWritable offsetResultLong = new LongWritable();
//	private final static IntWritable meResult = new IntWritable();
//	private final static IntWritable bufferLength = new IntWritable();
	private final static Text frmaeNumberText = new Text();
	private final static IntWritable MotionEstimationResultInt = new IntWritable();
	private final static IntWritable offsetInt = new IntWritable();
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
    		long term = 4;
    		int segmentSize = 32;
    		offsetResultLong.set(fOffset);
//    		frameNumberInt.set(Integer.parseInt(frameNumber+""));
    		frmaeNumberText.set(frameNumber+"");
    		offsetText.set(fOffset+"");
    		offsetInt.set(Integer.parseInt(frameNumber+""));
    		String fileOffset = new String(""+file.getStart());
		    		
    		String localInput = fileOffset+".yuv";
//    		String localOutput = fileOffset+"_term8.yuv";p
//    		String localOutput = fileOffset+".bin";
//    		String localOutput2 = fileOffset+".yuv";
    		
    		
    		//For Debug
//    		System.out.println("Local file name : "+localInput);
//    		System.out.println(localOutput);
//    		System.out.println(localOutput2);
    		System.out.println(Length);
			
    		//fs.copyToLocalFile(fPath, new Path(localInput));
    		byte[] buffer = new byte[1024];
    		long byteCount = 0;
    		
    		FSDataInputStream inputStream = fs.open(fPath);

    		LOG.info("PATH1 : select 5 frame term 8 : "+frameNumber);
    		FileOutputStream out = new FileOutputStream(localInput);
    		int roofCount = segmentSize/(int)term +1;
    		for(int i=0;i<roofCount;i++){// 1frame read
    			byteCount = 0;
    			addOffset = i*term*Length;
//    			if(i==roofCount-1){//last section 
//    				addOffset -= 4*Length;
//    			}
//    			if(i==0){
//    				addOffset += Length;
//    			}
    			inputStream.seek(file.getStart()+addOffset);
	    		while(inputStream.read(buffer) != -1){
	    			out.write(buffer);
	    			byteCount += 1024;
	    			if(byteCount == Length){
	    				LOG.info(byteCount + " bytes sucessfully read");
	    				break;
	    			}
	    		}//while read
    		}//for
    		inputStream.close();
    		out.close();
    		
    		///////////////JNI CALL
    		LOG.info("PATH1 : JNI START : "+frameNumber);
    		int[] resultJni = JniMe.Me(localInput);// execution JNI Motion Estimation
    		int tmp=0;
    		int avg;
    		for(int i=0;i<resultJni.length;i++){
    			tmp+=resultJni[i];
    		}
    		avg = tmp/resultJni.length;
    		MotionEstimationResultInt.set(avg);
    		LOG.info("Average : "+avg+" starting index : "+offsetResultLong+ " : "+frameNumber);
    		context.write(offsetInt, MotionEstimationResultInt);
//    		meResult.set(avg);
//    		meResultText.set(avg+"");
//    		
//    		long fSize = Long.parseLong(context.getConfiguration().get("frameSize"));
//    		System.out.println(fSize);
//    		
//    		File in = new File(localInput);
//    		
//    		int framesToBeEncoded =  (int) (in.length() / fSize);
//    		String fTE = "--FramesToBeEncoded=" + new Integer(framesToBeEncoded).toString();
//    		System.out.println(fTE);
//    		String s;
    		
    		//For Debug
//    		for (int i=0;i<localFiles.length;i++) {
//    			System.out.println(localFiles[i].toUri());
//    		}
//    		
//    		try {
//
//    			Process oProcess = new ProcessBuilder(localFiles[2].toUri().toString(),"-c",localFiles[0].toUri().toString(),"-c",localFiles[1].toString().toString(), "-i", localInput, "-b", localOutput, "-o", localOutput2, fTE).start();
//
//    			BufferedReader stdOut   = new BufferedReader(new InputStreamReader(oProcess.getInputStream()));
//    			BufferedReader stdError = new BufferedReader(new InputStreamReader(oProcess.getErrorStream()));
//
//    			while ((s =   stdOut.readLine()) != null){
//    				System.out.println(s);
//    			}
//    			while ((s = stdError.readLine()) != null){
//    				System.err.println(s);
//    			}
//
//    		} catch (IOException e) { // 에러 처리
//    			System.err.println("ERROR\n" + e.getMessage());
//    			System.exit(-1);
//    		}
    		
//    		String outputDirectory = context.getConfiguration().get("outputDirectoryName");
//    		System.out.println("output directory : "+outputDirectory);
    		
//    		fs.copyFromLocalFile(new Path(localOutput), new Path(outputDirectory));
//    		fs.copyFromLocalFile(new Path(localOutput2), new Path(outputDirectory));
    		
//    		File red = new File(localInput);
//    		System.out.println(red.getName());
//    		System.out.println(red.length());
//			word.set(fileOffset);
    		
    		
//    		try{
//    			System.out.println("readStart");
//    			byte tempBuffer[] = new byte[1024];
//    			FileInputStream reduceRead = new FileInputStream(localInput);
//
//    			while(reduceRead.read(tempBuffer) != -1){
//    				offsetResult.set(fOffset);
//    				BytesWritable data = new BytesWritable(tempBuffer);
//    				context.write(offsetResult, data);
//    			}
//    			System.out.println("readEnd");
//    			reduceRead.close();
//    		}catch (Exception e){
//    			System.out.println(e.getMessage());
//    		}
    		
    		
		}
	}
}

/*
package hadoopEncoder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class HMMapper extends Mapper<Text, BytesWritable, LongWritable, BytesWritable>{
	private Path[] localFiles;
	private final static BytesWritable bTemp = new BytesWritable();
	private final static LongWritable one = new LongWritable(1);
	//private Text word = new Text();
	boolean isProcessed = false;
	
    public void setup (Context context) throws IOException{
    	localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
    }
	
	public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException{
		if(!isProcessed){
			isProcessed = true;
			
			//String taskId = context.getConfiguration().get("mapred.task.id");
			long Length = ((FileSplit)context.getInputSplit()).getLength();
			Path fPath = ((FileSplit)context.getInputSplit()).getPath();
    		FileSystem fs = fPath.getFileSystem(context.getConfiguration());
    		FileSplit file = (FileSplit) context.getInputSplit();
    		long fOffset = file.getStart();
    		
    		String fileOffset = new String(""+file.getStart());
    		String localInput = fileOffset+"_temp.yuv";
    		String localOutput = fileOffset+".bin";
    		String localOutput2 = fileOffset+".yuv";
    		
    		
    		//For Debug
    		System.out.println(localInput);
    		System.out.println(localOutput);
    		System.out.println(localOutput2);
    		System.out.println(Length);
			
    		//fs.copyToLocalFile(fPath, new Path(localInput));
    		byte[] buffer = new byte[1024];
    		long byteCount = 0;
    		
    		FSDataInputStream inputStream = fs.open(fPath);
    		inputStream.seek(file.getStart());
    		
    		FileOutputStream out = new FileOutputStream(localInput);
    		while(inputStream.read(buffer) != -1){
    			out.write(buffer);
    			byteCount += 1024;
    			if(byteCount == Length){
    				System.out.println(byteCount + " bytes sucessfully read");
    				break;
    			}
    		}
    		
    		inputStream.close();
    		out.close();
    		
    		long fSize = Long.parseLong(context.getConfiguration().get("frameSize"));
    		System.out.println(fSize);
    		
    		File in = new File(localInput);
    		
    		int framesToBeEncoded =  (int) (in.length() / fSize);
    		String fTE = "--FramesToBeEncoded=" + new Integer(framesToBeEncoded).toString();
    		System.out.println(fTE);
    		String s;
    		
    		//For Debug
//    		for (int i=0;i<localFiles.length;i++) {
//    			System.out.println(localFiles[i].toUri());
//    		}
    		
//    		try {
//
//    			Process oProcess = new ProcessBuilder(localFiles[2].toUri().toString(),"-c",localFiles[0].toUri().toString(),"-c",localFiles[1].toString().toString(), "-i", localInput, "-b", localOutput, "-o", localOutput2, fTE).start();
//
//    			BufferedReader stdOut   = new BufferedReader(new InputStreamReader(oProcess.getInputStream()));
//    			BufferedReader stdError = new BufferedReader(new InputStreamReader(oProcess.getErrorStream()));
//
//    			while ((s =   stdOut.readLine()) != null){
//    				System.out.println(s);
//    			}
//    			while ((s = stdError.readLine()) != null){
//    				System.err.println(s);
//    			}
//
//    		} catch (IOException e) { // ?먮윭 泥섎━
//    			System.err.println("ERROR\n" + e.getMessage());
//    			System.exit(-1);
//    		}
    		
    		String outputDirectory = context.getConfiguration().get("outputDirectoryName");
    		System.out.println(outputDirectory);
    		
    		fs.copyFromLocalFile(new Path(localOutput), new Path(outputDirectory));
    		fs.copyFromLocalFile(new Path(localOutput2), new Path(outputDirectory));
    		
    		File red = new File(localOutput);
    		System.out.println(red.getName());
    		System.out.println(red.length());
			//word.set(fileOffset);
    		try{
    			System.out.println("readStart");
    			byte tempBuffer[] = new byte[1024];
    			FileInputStream reduceRead = new FileInputStream(localOutput);
    			int offset;
    			while(reduceRead.read(tempBuffer) != -1){
    				one.set(fOffset);
    				BytesWritable data = new BytesWritable(tempBuffer);
    				context.write(one, data);
    			}
    			System.out.println("readEnd");
    			reduceRead.close();
    		}catch (Exception e){
    			System.out.println(e.getMessage());
    		}
		}
	}
}
*/