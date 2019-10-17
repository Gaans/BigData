Write operation

local to local - 92895ms

local to hdfs - 74295ms


read seq
local - 33615ms
hdfs - 39614ms

radom read
local - 39219ms
hdfs - 28959ms



This program can take upto 3 arguments

1. command to execute write or read or random read.(write,read,random_read)
2. source file path. (if local specify file:/// , else hdfs specify hdfs://)
3. destination file path.(if local specify file:/// , else hdfs specify hdfs://)

Sample command to run

1) Write from local to local

hadoop jar target/gsiva005-0.0.1-SNAPSHOT.jar edu.ucr.cs.cs226.gsiva005.HDFSUpload write file:///media/ganesh/E0DE4200DE41D008/UCR\ Courses/Big\ Data/eclipse_workspace/Assgn1/gsiva005/AREAWATER.csv file:///media/ganesh/E0DE4200DE41D008/UCR\ Courses/Big\ Data/eclipse_workspace/Assgn1/gsiva005/output.csv

2) Write from local to hdfs -

hadoop jar target/gsiva005-0.0.1-SNAPSHOT.jar edu.ucr.cs.cs226.gsiva005.HDFSUpload write file:///media/ganesh/E0DE4200DE41D008/UCR\ Courses/Big\ Data/eclipse_workspace/Assgn1/gsiva005/AREAWATER.csv hdfs:///user/ganesh/areawater.csv

3) Read sequentially from hdfs 

hadoop jar target/gsiva005-0.0.1-SNAPSHOT.jar edu.ucr.cs.cs226.gsiva005.HDFSUpload read hdfs:///user/ganesh/areawater.csv

4) Read sequentially from local

hadoop jar target/gsiva005-0.0.1-SNAPSHOT.jar edu.ucr.cs.cs226.gsiva005.HDFSUpload read file:///media/ganesh/E0DE4200DE41D008/UCR\ Courses/Big\ Data/eclipse_workspace/Assgn1/gsiva005/AREAWATER.csv

5) Random Read from local

hadoop jar target/gsiva005-0.0.1-SNAPSHOT.jar edu.ucr.cs.cs226.gsiva005.HDFSUpload random_read file:///media/ganesh/E0DE4200DE41D008/UCR\ Courses/Big\ Data/eclipse_workspace/Assgn1/gsiva005/output.csv

6) Random Read from hdfs

hadoop jar target/gsiva005-0.0.1-SNAPSHOT.jar edu.ucr.cs.cs226.gsiva005.HDFSUpload random_read hdfs:///user/ganesh/areawater.csv

