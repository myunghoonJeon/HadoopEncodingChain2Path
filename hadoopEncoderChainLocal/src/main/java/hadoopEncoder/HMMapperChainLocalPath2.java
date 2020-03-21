
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


public class HMMapperChainLocalPath2 extends Mapper<Text, BytesWritable, LongWritable, BytesWritable>{
	private Path[] localFiles;
	private final static BytesWritable bTemp = new BytesWritable();
	private final static LongWritable one = new LongWritable(1);
	//private Text word = new Text();
	boolean isProcessed = false;
	private final static Log LOG = LogFactory.getLog(HMMapperChainLocalPath2.class);
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
/*
 * --------------------- For TEST ---------------------------			
			String localInput = fileOffset+"_temp.yuv";
			String localOutput = fileOffset+".bin";
			String localOutput2 = fileOffset+".yuv";
			
			
			//For Debug
//			System.out.println(localInput);
//			System.out.println(localOutput);
//			System.out.println(localOutput2);
//			System.out.println(Length);
------------------------------------------------------------*/		
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
				String[] splitStr = line.split("\t");
				arr.add(splitStr[1]);
//				arr.add(line);//temprory path2 test
				line = br.readLine();
			}
			indexfileSize = arr.size()-1;
			
			indexFlag = indexfileSize-(int)(fOffset/fSize/32);
			
			changedIndex = Long.parseLong(arr.get(indexFlag));
			long simpleIndex = (long) (changedIndex/3840/2160/1.5);
			String localInput = "/home/hadoop/MCcloud/"+"temp"+simpleIndex+".yuv";
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
			
/******************************Data Stream TEST**************************/			
//			FSDataInputStream inputStream = fs.open(fPath);
//			inputStream.seek(changedIndex);
/*********************************load index****************************************/			
			String localFilePath = context.getConfiguration().get("localFile");
			File localFile = new File(localFilePath);
			FileInputStream inputStream = new FileInputStream(localFile);//local case
//			int roofSize = Integer.parseInt(context.getConfiguration().get("nGOP"));
			long wide = 3840;
			long height = 2160;
			long len = (long)(wide*height*(1.5)*32);
/************************************************************************************/					
			FileOutputStream out = new FileOutputStream(localInput,false);
			inputStream.skip(changedIndex);
			while(inputStream.read(buffer) != -1){
					out.write(buffer);
					byteCount += 1024;
					if(byteCount == len){
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
					logBuffer.write(s+"\r\n");
				    logBuffer.flush();
				}
				while ((s = stdError.readLine()) != null){
					System.err.println(s);
					logBuffer.write(s+"\r\n");
				    logBuffer.flush();
				}

			} catch (IOException e) { // Error 
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
			////////////////////////////////////////////////////////////////////
			//in.delete();//created file (local output) delegation
			////////////////////////////////////////////////////////////////////
			//word.set(fileOffset);
			try{//start read
				byte tempBuffer[] = new byte[1024];
				FileInputStream reduceRead = new FileInputStream(localOutput);
				int offset;
				while(reduceRead.read(tempBuffer) != -1){
					one.set(changedIndex); //index changed
					BytesWritable data = new BytesWritable(tempBuffer);
					context.write(one, data); //key - index , value - binary
				}
				//read End
				reduceRead.close();
			}catch (Exception e){
				System.out.println(e.getMessage());
			}
		}
	}
	
}
