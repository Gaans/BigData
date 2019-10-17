package edu.ucr.cs.cs226.gsiva005;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
/**
 * Hello world!
 *
 */
public class HDFSUpload 
{
    public static void main( String[] args )
    {
    	String execMethod = args[0];
    	String sourceFilePath = args[1];
    	String destFilePath = null;
        if(args.length > 2) {
    	destFilePath = args[2];
    	}
        
        try {	
        	if(execMethod.equalsIgnoreCase("write")) {
        		writeFiles(sourceFilePath,destFilePath);
        	}
        	else if(execMethod.equalsIgnoreCase("read")) {
        		readSeqFiles(sourceFilePath);
        	}
        	else if(execMethod.equalsIgnoreCase("random_read")) {
        		readRandomFiles(sourceFilePath);
        	}
        	else
        	{
        		System.out.println("Wrong arguments!.");
        	}
			
		} 
        catch (IOException e) {
        	System.out.println("IO Exception while accessing hdfs File system");
        	e.printStackTrace();
		}
    }
    
    public static void writeFiles(String source,String dest) throws IOException {
    	FSDataInputStream sourceInputStream = null;
    	FSDataOutputStream destOutStream = null;
    	FileSystem sourceFileSystem = null,destFileSystem = null;
    	try {
        	// Read source file
			//InputStream sourceFileStream = new FileInputStream(new File(localFilePath));
			
    		Configuration conf = new Configuration();
    		
    		Path sourcePath = new Path(source);
    		sourceFileSystem = sourcePath.getFileSystem(conf);
    		
    		//check if source file exists.
			if(!sourceFileSystem.exists(sourcePath)){
				System.out.println("Source File doesn't exists.");
				sourceFileSystem.close();
				return;
			}
    		
			Path destPath = new Path(dest);
			destFileSystem = destPath.getFileSystem(conf);
			
			//check if dest file exists.
			if(destFileSystem.exists(destPath)) {
				System.out.println("File already exists in destination FileSystem.");
				sourceFileSystem.close();
				destFileSystem.close();
				return;
			}
			
			//Get source filestream.
			sourceInputStream = sourceFileSystem.open(sourcePath);
			
			//Get dest filestream
			destOutStream = destFileSystem.create(destPath);
			
			long startTime = System.currentTimeMillis();
			IOUtils.copy(sourceInputStream, destOutStream);
			long stopTime = System.currentTimeMillis();
			long elapsedTime = stopTime - startTime;
			System.out.println("Time to copy file from source to dest : "+elapsedTime);
    	}
        catch (IOException e) {
        	System.out.println("IO Exception while accessing File system");
        	e.printStackTrace();
		}
    	finally {
    		if(sourceInputStream != null) {
			sourceInputStream.close();
    		}
    		if(sourceFileSystem != null)
    		{
    			sourceFileSystem.close();
    		}
    		if(destOutStream != null) {
			destOutStream.close();
    		}
    		if(destFileSystem != null) {
    			destFileSystem.close();
    		}
		}
    }
    
    public static void readSeqFiles(String source) throws IOException {
    	Configuration conf = new Configuration();
		Path sourcePath = new Path(source);
		FileSystem sourceFileSystem = sourcePath.getFileSystem(conf);
    	int byteReadLenght = 2<<16;
		byte readFileSqBytes[] = new byte[byteReadLenght];

		//Read Seq
		FSDataInputStream sourceInputStream = sourceFileSystem.open(sourcePath);
		long pos = 0;
		int bytesRead;
		long startTime = System.currentTimeMillis();
		while((bytesRead =sourceInputStream.read(pos, readFileSqBytes, 0, byteReadLenght))!=-1)
		{
			pos += bytesRead;
		}
		long stopTime = System.currentTimeMillis();
		long elapsedTime = stopTime - startTime;
		System.out.println("Read time : " + elapsedTime);
		
		//close stream
		sourceInputStream.close();
		sourceFileSystem.close();
    }
    public static void readRandomFiles(String source) throws IOException {
    	Configuration conf = new Configuration();
		Path sourcePath = new Path(source);
		FileSystem sourceFileSystem = sourcePath.getFileSystem(conf);
    	FileStatus fileStatus = sourceFileSystem.getFileStatus(sourcePath);
    	FSDataInputStream sourceInputStream = sourceFileSystem.open(sourcePath);
		long fileLength = fileStatus.getLen();
		byte readFileBytes[] = new byte[1025];
		Random rand = new Random();
    	//Random read
		long startTime = System.currentTimeMillis();
		for(int i=0;i<2000;i++)
		{	
			long seekPos = (long) (rand.nextDouble()*fileLength);
			sourceInputStream.read(seekPos,readFileBytes,0,1024);
		}
		long stopTime = System.currentTimeMillis();
		long elapsedTime = stopTime - startTime;
		System.out.println("2000 Random read : " + elapsedTime);
		sourceInputStream.close();
		sourceFileSystem.close();
    }
}
