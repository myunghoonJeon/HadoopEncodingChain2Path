
package hadoopEncoder;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class HMMapperPath2 extends Mapper<Text, BytesWritable, LongWritable, BytesWritable>{
	private Path[] localFiles;
	private final static BytesWritable bTemp = new BytesWritable();
	private final static LongWritable one = new LongWritable(1);
	//private Text word = new Text();
	boolean isProcessed = false;
	private final static Log LOG = LogFactory.getLog(HMMapperPath2.class);
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
			long fSize = Long.parseLong(context.getConfiguration().get("frameSize"));
			long newOffset;
			long changedIndex;
			int indexFlag=0;
			int indexfileSize;
			String fileOffset = new String(""+file.getStart());
//			String localInput = fileOffset+"_temp.yuv";
//			String localOutput = fileOffset+".bin";
//			String localOutput2 = fileOffset+".yuv";
			
			
			//For Debug
//			System.out.println(localInput);
//			System.out.println(localOutput);
//			System.out.println(localOutput2);
//			System.out.println(Length);
			FileOutputStream logOutput=null;
			OutputStreamWriter writer;
			

			Path indexPath = new Path(context.getConfiguration().get("indexfile"));
			InputStreamReader isr = new InputStreamReader(fs.open(indexPath));
			BufferedReader br = new BufferedReader(isr);
			String line;
			line = br.readLine();
			ArrayList<String> arr = new ArrayList<String>();
			LOG.info("reading index file : part-r-00000");
			while(line !=null){
//				String[] splitStr = line.split("\t");
//				arr.add(splitStr[1]);
				arr.add(line);//temprory path2 test
				line = br.readLine();
			}
			indexfileSize = arr.size()-1;
			
			indexFlag = indexfileSize-(int)(fOffset/fSize/32);
			
			changedIndex = Long.parseLong(arr.get(indexFlag));
			
			String localInput = changedIndex+"_temp.yuv";
			String localOutput = changedIndex+".bin";
			String localOutput2 = changedIndex+".yuv";
			
			///////////////////log writer////////////////////
			String logfile = changedIndex+".txt";
			logOutput = new FileOutputStream(logfile,true);
		    writer = new OutputStreamWriter(logOutput);
		    BufferedWriter logBuffer = new BufferedWriter(writer);
		    
			
			LOG.info("index changed : "+file.getStart()/fSize+" --->>>"+changedIndex/fSize);
			//fs.copyToLocalFile(fPath, new Path(localInput));
			byte[] buffer = new byte[1024];
			long byteCount = 0;
			FSDataInputStream inputStream = fs.open(fPath);
			inputStream.seek(changedIndex);
			
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
			
			
			System.out.println(fSize);
			
			File in = new File(localInput);
			
			int framesToBeEncoded =  (int) (in.length() / fSize);
			String fTE = "--FramesToBeEncoded=" + new Integer(framesToBeEncoded).toString();
			System.out.println(fTE);
			String s;
			
			//For Debug
			for (int i=0;i<localFiles.length;i++) {
				System.out.println(localFiles[i].toUri());
			}
			LOG.info("start Encoding : "+changedIndex);
			try {

				Process oProcess = new ProcessBuilder(localFiles[2].toUri().toString(),"-c",localFiles[0].toUri().toString(),"-c",localFiles[1].toString().toString(), "-i", localInput, "-b", localOutput, "-o", localOutput2, fTE).start();

				BufferedReader stdOut   = new BufferedReader(new InputStreamReader(oProcess.getInputStream()));
				BufferedReader stdError = new BufferedReader(new InputStreamReader(oProcess.getErrorStream()));

				while ((s =   stdOut.readLine()) != null){
					System.out.println(s);
					logBuffer.write(s+"\r\n", 0 , s.length());
				    logBuffer.flush();
				}
				while ((s = stdError.readLine()) != null){
					System.err.println(s);
					logBuffer.write(s+"\r\n", 0 , s.length());
				    logBuffer.flush();
				}

			} catch (IOException e) { // 에러 처리
				System.err.println("ERROR\n" + e.getMessage());
				System.exit(-1);
			}
			logOutput.close();
			logBuffer.close();
//			Runtime.getRuntime().exec("ps -al > ps.txt");
			String outputDirectory = context.getConfiguration().get("outputDirectoryName")+"path2";
			System.out.println(outputDirectory);
			
			fs.copyFromLocalFile(new Path(localOutput), new Path(outputDirectory));
			fs.copyFromLocalFile(new Path(localOutput2), new Path(outputDirectory));
			fs.copyFromLocalFile(new Path(logfile), new Path(outputDirectory));
			
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
					one.set(changedIndex); //index changed
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
////////////////////////////////////////////////////////////////////////////////////
/*
private Path[] localFiles;
//private final static BytesWritable bTemp = new BytesWritable();
private final static LongWritable offsetResultLong = new LongWritable();
//private final static IntWritable meResult = new IntWritable();
//private final static IntWritable bufferLength = new IntWritable();
private final static Text frmaeNumberText = new Text();
private final static IntWritable MotionEstimationResultInt = new IntWritable();
private final static Text offsetText = new Text();
//private Text word = new Text();
boolean isProcessed = false;

public void setup (Context context) throws IOException{
	localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
}

public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException{
	if(!isProcessed){
//		System.load("/home/hadoop/video/libMotionEstimation.so");
		isProcessed = true;
		Runtime.getRuntime().exec("sudo ldconfig -v");
		
		//String taskId = context.getConfiguration().get("mapred.task.id");
//		long Length = ((FileSplit)context.getInputSplit()).getLength();
		long Length = (long)(3840*2160*1.5);
		Path fPath = ((FileSplit)context.getInputSplit()).getPath();
		FileSystem fs = fPath.getFileSystem(context.getConfiguration());
		FileSplit file = (FileSplit) context.getInputSplit();
		long fOffset = file.getStart();
		long frameNumber = fOffset/Length;
		long addOffset = 0;
		long term = 8;
		offsetResultLong.set(frameNumber);
//		frameNumberInt.set(Integer.parseInt(frameNumber+""));
		frmaeNumberText.set(frameNumber+"");
		offsetText.set(fOffset+"");
		
		String fileOffset = new String(""+file.getStart());
	    		
		String localInput = fileOffset+".yuv";
//		String localOutput = fileOffset+"_term8.yuv";
//		String localOutput = fileOffset+".bin";
//		String localOutput2 = fileOffset+".yuv";
		
		
		//For Debug
		System.out.println("Local file name : "+localInput);
//		System.out.println(localOutput);
//		System.out.println(localOutput2);
		System.out.println(Length);
		
		//fs.copyToLocalFile(fPath, new Path(localInput));
		byte[] buffer = new byte[1024];
		long byteCount = 0;
		
		FSDataInputStream inputStream = fs.open(fPath);
		
		System.out.println("```begin process : select 5 frame term 8");
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
    				System.out.println(byteCount + " bytes sucessfully read");
    				break;
    			}
    		}//while read
		}//for
		inputStream.close();
		out.close();
		
		///////////////JNI CALL
		System.out.println("~JNI CALL!!!");
		System.out.println("Context Write complete!!");
		int[] resultJni = JniMe.Me(localInput);// execution JNI Motion Estimation
		int tmp=0;
		int avg;
		for(int i=0;i<resultJni.length;i++){
			tmp+=resultJni[i];
		}
		avg = tmp/resultJni.length;
		MotionEstimationResultInt.set(avg);
		System.out.println("Average : "+avg+" starting index : "+offsetResultLong);
		context.write(MotionEstimationResultInt, offsetResultLong);
		
	}
}

*/	