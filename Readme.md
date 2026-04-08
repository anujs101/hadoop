BEFORE YOU START
1. Start Hadoop
start-dfs.sh
start-yarn.sh

2. Check if running
jps
You should see:
NameNode
DataNode
ResourceManager
NodeManager

3. Exit safe mode (important)
hdfs dfsadmin -safemode leave

📂 SETUP (once per run)
Create HDFS dirs (ignore error if exists)
hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/anuj

📄 INPUT FILE
Create input
nano input.txt
Paste:
hadoop 10
mapreduce 20
hadoop 30
bigdata 40
mapreduce 50
Save.

Upload to HDFS
hdfs dfs -put -f input.txt /user/anuj/

⚙️ COMPILE + JAR (if needed)
hadoop com.sun.tools.javac.Main ESEMapReduce.java
jar cf ese.jar *.class

🚀 RUN ALL QUERIES

🔹 1. Word Count
hdfs dfs -rm -r output_wc
hadoop jar ese.jar ESEMapReduce wordcount /user/anuj/input.txt output_wc
hdfs dfs -cat output_wc/part-r-00000

🔹 2. Max
hdfs dfs -rm -r output_max
hadoop jar ese.jar ESEMapReduce max /user/anuj/input.txt output_max
hdfs dfs -cat output_max/part-r-00000

🔹 3. Average
hdfs dfs -rm -r output_avg
hadoop jar ese.jar ESEMapReduce avg /user/anuj/input.txt output_avg
hdfs dfs -cat output_avg/part-r-00000

🔹 4. Distinct
hdfs dfs -rm -r output_distinct
hadoop jar ese.jar ESEMapReduce distinct /user/anuj/input.txt output_distinct
hdfs dfs -cat output_distinct/part-r-00000

🔹 5. Odd / Even
hdfs dfs -rm -r output_oddeven
hadoop jar ese.jar ESEMapReduce oddeven /user/anuj/input.txt output_oddeven
hdfs dfs -cat output_oddeven/part-r-00000

⚠️ EMERGENCY FIXES (YOU WILL NEED THIS)
❌ Safe mode error
hdfs dfsadmin -safemode leave

❌ Output already exists
hdfs dfs -rm -r output_folder_name

❌ Hadoop not starting
stop-dfs.sh
stop-yarn.sh
start-dfs.sh
start-yarn.sh

🧠 5-SECOND VIVA LINES
Say these confidently:
“Mapper emits key-value pairs.”


“Reducer aggregates values.”


“Shuffle groups data by key.”


“We used YARN for resource management.”



🔥 PRO MOVE (if she modifies question)
You only change:
Mapper logic (conditions)


OR Reducer logic (aggregation)


👉 NOT whole code

🏁 YOU’RE READY
If you follow this:
Setup: 2 mins


Run all queries: 5 mins


Viva: smooth



If you want a last-minute cheat sheet of code changes she might ask, say:
👉 “lab traps”

