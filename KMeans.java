package hadoop;

import java.util.*;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeans {

	// Mapper class
	public static class KMeansMapper extends Mapper<LongWritable, /* Input key Type */
			Text, /* Input value Type */
			IntWritable, /* Output key Type */
			Writable> /* Output value Type */
	{
		Map<Integer, ArrayList<Double>> centorids = new HashMap<Integer, ArrayList<Double>>();	//Centroids Local to Mapper Class

		//Setup Function
		@Override
		protected void setup(Context context) throws IOException {	//Run at the begining of each Map step.

			Path init_pt = new Path("shared_dir/centroids.txt");// Location of centroids file in HDFS. Updated after each pass of the algorithm
			
			Configuration config = new Configuration();
			config.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
			config.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = null;
			br = new BufferedReader(new InputStreamReader(fs.open(init_pt)));
			String line;
			while ((line = br.readLine()) != null) {						//Reading and storing the Centroid values in the Mappers memory
				StringTokenizer s = new StringTokenizer(line, "\t");
				ArrayList<Double> vector = new ArrayList<Double>();
				int id = 0;
				if (s.hasMoreTokens()) {
					String p = s.nextToken();
					id = Integer.parseInt(p);
				}
				String centr = "";
				while (s.hasMoreTokens()) {
					String tok = s.nextToken();
					if (tok.contains("|")) {
						break;
					}
					centr += tok + "\t";
					vector.add(Double.parseDouble(tok));
				}
				
				centorids.put(id, vector);
			}
			br.close();
			fs.close();

		}

		// Map function
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer s = new StringTokenizer(line, "\t");			//Parsing through the input data line.
			ArrayList<Double> vector = new ArrayList<Double>();
			int id = 0;
			Gene gene = new Gene();
			if (s.hasMoreTokens()) {
				id = Integer.parseInt(s.nextToken());
				gene.setId(new IntWritable(id));
			}
			if (s.hasMoreTokens()) {
				s.nextToken();
			}

			while (s.hasMoreTokens()) {
				double val = Double.parseDouble(s.nextToken());
				gene.getData().add(new DoubleWritable(val));
				vector.add(val);
			}

			double minDist = Double.MAX_VALUE;
			int minClusterID = -1;
			
			for (int i : centorids.keySet()) {									//Iterate over each centroid to find the closest Centroid
				double dist = calculateDist(centorids.get(i), vector);
				if (dist < minDist) {
					minDist = dist;
					minClusterID = i;
				}
			}
			
			gene.setClusterNumber(new IntWritable(minClusterID));
			GeneList geneList = new GeneList();
			geneList.geneList.add(gene);
			
			context.write(new IntWritable(minClusterID), geneList);				//Emitting the ouput from Mapper.
		}

		public Double calculateDist(List<Double> centroid, List<Double> geneData) {			//Function to Calculate the Euclidean Distance between two Points
			Double sum = 0.0;
			for (int i = 0; i < centroid.size(); ++i) {
				sum += Math.pow((centroid.get(i) - geneData.get(i)), 2);
			}
			return Math.sqrt(sum);
		}

	}

	
	//Combiner Class
	public static class KMeansCombiner extends Reducer<IntWritable, Writable, IntWritable, Writable> {

		@Override
		public void reduce(IntWritable key, Iterable<Writable> values, Context context)					//Combiner Function
				throws IOException, InterruptedException {
			GeneList geneList = new GeneList();
			
			while (values.iterator().hasNext()) {														//Combines values of a particular key into a list
				GeneList temp = (GeneList) values.iterator().next();
			
				for (Gene gene : temp.geneList) {
					geneList.geneList.add(gene);
				}
			}
			context.write(key, geneList);																//Output of Combiner Function
		}
	}


	// Reducer class
	public static class KMeansReducer extends Reducer<IntWritable, Writable, IntWritable, Writable> {
		
		@Override
		public void reduce(IntWritable key, Iterable<Writable> values, Context context)					//Reducer Functiond
				throws IOException, InterruptedException {

			Map<Integer, ArrayList<Double>> data = new HashMap<Integer, ArrayList<Double>>();
			int size = 0;
			
			Centroid centroid = new Centroid();
			centroid.id = key;
			while (values.iterator().hasNext()) {														//Iterating through the input values and extracting the data from it
				GeneList geneList = (GeneList) values.iterator().next();
				
				for (Gene gene : geneList.geneList) {
					
					ArrayList<Double> vector = new ArrayList<Double>();
					int id = gene.getId().get();
					centroid.idList.add(new IntWritable(id));
					for (DoubleWritable i : gene.geneVals) {
						vector.add(i.get());
					}
					data.put(id, vector);
					size = vector.size();
				}
			}
			ArrayList<Double> newCentroid = reCalculateCentroid(data, size);							//Recalculating the Centroid
			

			for (double val : newCentroid) {
				centroid.vector.add(new DoubleWritable(val));											//Adding new centroid to Centroid Object
			}

			context.write(key, centroid);																//Emitting the Centroid id and values
		}

		public ArrayList<Double> reCalculateCentroid(Map<Integer, ArrayList<Double>> data, int size) {	//Funtion to Recalculate the Centroid
			ArrayList<Double> newCentroid = new ArrayList<Double>();

			for (int i = 0; i < size; ++i) {
				double val = 0;
				for (int j : data.keySet()) {
					val += data.get(j).get(i);
				}
				val = val / data.size();
				newCentroid.add(val);
			}

			return newCentroid;

		}

	}

	

	public static void main(String args[]) throws Exception {
		int count = 0;
		Map<Integer, ArrayList<Double>> prevCentorids = new HashMap<Integer, ArrayList<Double>>();		//Map to Store the Previous Centroids
		Map<Integer, ArrayList<Double>> currCentorids = new HashMap<Integer, ArrayList<Double>>();		//Map to Store Current Centroids
		String hdfs_IP = args[0];																		//Reading Parameter Input
		String hdfs_OP = args[1];
		String hdfs_Shared = args[2];
		int clusNum = Integer.parseInt(args[3]);
		String init_Centroids = args[4];
		String termination = args[5];
		
		DataStore ds = new DataStore(hdfs_IP);															//Object to read the data from HDFS
		int terminationCount = Integer.parseInt(termination);
		ds.generateCentorids(hdfs_Shared, clusNum, init_Centroids);										//Function to Generate Centroid File
		

		while (true) {																					//Map Reduce Driver

			if (terminationCount == 0) {																//Termination Condition								
				break;
			}
			--terminationCount;
			++count;

			Configuration con = new Configuration();													//Configuration
			Job job = Job.getInstance(con, "KMeans");
			job.setJarByClass(KMeans.class);
			job.setMapperClass(KMeansMapper.class);
			job.setCombinerClass(KMeansCombiner.class);
			job.setReducerClass(KMeansReducer.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(GeneList.class);

			con.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
			con.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));

			FileSystem fs = FileSystem.get(con);

			FileInputFormat.addInputPath(job, new Path(hdfs_IP));
			FileOutputFormat.setOutputPath(job, new Path(hdfs_OP));
			if (job.waitForCompletion(true)) {															//Run MapReduce operation

				FileUtil.copy(fs, new Path(hdfs_OP + "/part-r-00000"), fs, new Path(hdfs_Shared + "/centroids.txt"),	//Copying Reducer Output to the HDFS input Directory to be accessed by mapper
						true, true, con);

			} else {
				System.exit(1);
			}

			prevCentorids = currCentorids;
			currCentorids = new HashMap<Integer, ArrayList<Double>>();
			BufferedReader br = null;																				//Reading new Centroid Values from HDFS
			br = new BufferedReader(new InputStreamReader(fs.open(new Path(hdfs_Shared + "/centroids.txt"))));
			String line;
			while ((line = br.readLine()) != null) {
				StringTokenizer s = new StringTokenizer(line, "\t");
				ArrayList<Double> vector = new ArrayList<Double>();
				int id = 0;
				if (s.hasMoreTokens()) {
					String p = s.nextToken();
					id = Integer.parseInt(p);
				}
				while (s.hasMoreTokens()) {
					String tok = s.nextToken();
					if (tok.contains("|")) {
						break;
					}
					vector.add(Double.parseDouble(tok));
				}
				currCentorids.put(id, vector);
			}
			br.close();
			if (checkSimilar(prevCentorids, currCentorids)) {			//Checking if new centroid and old centroid are the same
				break;
			}
			fs.delete(new Path(hdfs_OP), true);							//deleting the HDFS output directory
			fs.close();
			
		}
		System.out.println("Total Number of Iterations - "+count);
		DataStore clusResult = formatOutput(hdfs_IP);					//Create a Result object to be printed and processed using the new centroids
		printOutputFile(clusResult);									//print the output file with new centroids
		DataStore old = new DataStore(hdfs_IP);							//create an object with initial centroid values for comparison
		jaccardCoeff(old, clusResult);									//Calculate and print jaccard coefficient

	}
	
	public static boolean checkSimilar(Map<Integer, ArrayList<Double>> prevCentorids,		//To check if the previous centroid and the current centroid are the same
			Map<Integer, ArrayList<Double>> currCentorids) {
		boolean result = true;
		if (prevCentorids.size() == currCentorids.size()) {
			for (int i : prevCentorids.keySet()) {
				Double sum = 0.0;
				for (int j = 0; j < prevCentorids.get(i).size(); ++j) {
					sum += Math.pow((prevCentorids.get(i).get(j) - currCentorids.get(i).get(j)), 2);
				}
				sum = Math.sqrt(sum);
				if (sum != 0) {
					result = false;
					break;
				}
			}
		} else {
			result = false;
		}
		return result;
	}

	public static DataStore formatOutput(String ipPath) throws IOException {	//Create a DataStore Object with new Centroids
		DataStore ds = new DataStore(ipPath);
		Configuration config = new Configuration();

		config.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
		config.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));

		FileSystem fs = FileSystem.get(config);
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path("shared_dir/centroids.txt"))));
		String line;
		while ((line = br.readLine()) != null) {
			StringTokenizer s = new StringTokenizer(line, "\t");
			int id = 0;
			if (s.hasMoreTokens()) {
				String p = s.nextToken();
				id = Integer.parseInt(p);
			}

			String tok = "";
			while (s.hasMoreTokens()) {
				tok = s.nextToken();
				if (tok.contains("|")) {
					break;
				}
			}
			StringTokenizer pipeTok = new StringTokenizer(tok, "|");
			while (pipeTok.hasMoreTokens()) {
				int index = Integer.parseInt(pipeTok.nextToken());
				ds.geneData.get(index).clusterNumber = new IntWritable(id);
			}
		}
		br.close();
		fs.close();
		return ds;
	}

	public static void printOutputFile(DataStore ds) {	//Print output files to local disk
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter("/home/rohit/hdoop/test/finalOutput.txt", false));
			BufferedWriter writer2 = new BufferedWriter(new FileWriter("/home/rohit/hdoop/test/hadoop_cluster_data_toPlot.txt", false));
			for (int i : ds.geneData.keySet()) {
				StringBuilder op = new StringBuilder();
				StringBuilder plotOP = new StringBuilder();
				op.append(i + "\t");
				op.append(ds.geneData.get(i).clusterNumber + "\t");
				for (DoubleWritable j : ds.geneData.get(i).geneVals) {
					op.append(j.toString() + "\t");
					plotOP.append(j.toString() + "\t");

				}
				plotOP.append(ds.geneData.get(i).clusterNumber);
				writer.write(op.toString() + "\n");
				writer2.write(plotOP.toString() + "\n");
			}
			writer.close();
			writer2.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	////////////////////////////////////////Misc Objects and Classes///////////////////////////////////////////////////////////
	
	public static class Gene implements Writable {	//Writable Gene Object, Corresponds to a row in the dataset
		IntWritable id;
		ArrayList<DoubleWritable> geneVals;
		IntWritable clusterNumber;

		public Gene() {
			geneVals = new ArrayList<DoubleWritable>();
			id = new IntWritable(0);
			clusterNumber = new IntWritable(-1);
		}

		public IntWritable getId() {
			return id;
		}

		public void setId(IntWritable id) {
			this.id = id;
		}

		public IntWritable getClusterNumber() {
			return clusterNumber;
		}

		public void setClusterNumber(IntWritable clusterNumber) {
			this.clusterNumber = clusterNumber;
		}

		public ArrayList<DoubleWritable> getData() {
			return geneVals;
		}

		public void setData(ArrayList<DoubleWritable> data) {
			this.geneVals = data;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			id.readFields(in);
			clusterNumber.readFields(in);
			int size = in.readInt();
			geneVals = new ArrayList<DoubleWritable>(size);
			for (int i = 0; i < size; i++) {
				DoubleWritable d = new DoubleWritable();
				d.readFields(in);
				geneVals.add(d);
			}
		}

		@Override
		public void write(DataOutput out) throws IOException {
			id.write(out);
			clusterNumber.write(out);
			out.writeInt(geneVals.size());
			for (DoubleWritable i : geneVals) {
				i.write(out);
			}
		}

	}

	public static class GeneList implements Writable {	//List of Gene object. Is the output object of Mapper and Combiner
		ArrayList<Gene> geneList;

		public GeneList() {
			geneList = new ArrayList<Gene>();
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			int size = in.readInt();
			geneList = new ArrayList<Gene>(size);
			for (int i = 0; i < size; i++) {
				Gene gene = new Gene();
				gene.readFields(in);
				geneList.add(gene);
			}
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeInt(geneList.size());
			for (Gene gene : geneList) {
				gene.write(out);
			}
		}

	}

	public static class Centroid implements Writable {	//Writable Centroid object. Corresponds to a Centroid. Is the output object for reducer

		IntWritable id;
		ArrayList<DoubleWritable> vector;
		ArrayList<IntWritable> idList;

		public Centroid() {
			idList = new ArrayList<IntWritable>();
			vector = new ArrayList<DoubleWritable>();
			id = new IntWritable();
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			id.readFields(in);
			int vectorSize = in.readInt();
			vector = new ArrayList<DoubleWritable>(vectorSize);
			for (int i = 0; i < vectorSize; i++) {
				DoubleWritable val = new DoubleWritable();
				val.readFields(in);
				vector.add(val);
			}

			int idSize = in.readInt();
			idList = new ArrayList<IntWritable>(idSize);
			for (int i = 0; i < idSize; i++) {
				IntWritable val = new IntWritable();
				val.readFields(in);
				idList.add(val);
			}
		}

		@Override
		public void write(DataOutput out) throws IOException {
			id.write(out);

			out.writeInt(vector.size());
			for (DoubleWritable i : vector) {
				i.write(out);
			}

			out.writeInt(idList.size());
			for (IntWritable i : idList) {
				i.write(out);
			}

		}

		@Override
		public String toString() {
			StringBuffer buf = new StringBuffer();
			for (DoubleWritable val : vector) {
				buf.append(val.get() + "\t");
			}
			for (IntWritable index : idList) {
				buf.append(index.get() + "|");
			}
			buf.append("\t-"+idList.size());
			return buf.toString();
		}

	}

	public static class DataStore {	//Object to generate initial centroids and calculate jaccard coefficient
		public Map<Integer, Gene> geneData;
		public String filePath;
		public String centroidFilePath = "/home/rohit/hdoop/test/centroids.txt";
		public Set<Integer> clusterNames = new HashSet<Integer>();

		public DataStore(String path) {
			filePath = path;
			geneData = new HashMap<Integer, Gene>();
			populateData();
		}

		public void populateData() {
			try {
				Configuration con = new Configuration();
				FileSystem fs = FileSystem.get(con);
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(filePath))));
				String str;
				while ((str = br.readLine()) != null) {
					Gene gene = new Gene();
					StringTokenizer s = new StringTokenizer(str, "\t");
					int id = 0;
					if (s.hasMoreTokens()) {
						id = Integer.parseInt(s.nextToken());
						gene.setId(new IntWritable(id));
					}
					if (s.hasMoreTokens()) {
						int clusterNumber = Integer.parseInt(s.nextToken());
						gene.setClusterNumber(new IntWritable(clusterNumber));
						if (clusterNumber != -1) {
							clusterNames.add(clusterNumber);
						}
					}

					while (s.hasMoreTokens()) {
						gene.getData().add(new DoubleWritable(Double.parseDouble(s.nextToken())));
					}
					geneData.put(id, gene);
				}
				br.close();
				fs.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch blockgene
				e.printStackTrace();
			}
		}
				
		public void generateCentorids(String ip_Path, int clusterSize, String ids) {	//function to generate initial Centroids

			if (ids.equals("-")) {
				ids = "";
				Set<Integer> usedCentroids = new HashSet<Integer>();
				while (usedCentroids.size() < (clusterSize)) {
					int index = (int) ((Math.random() * 10000) % geneData.size()) + 1;
					if (!usedCentroids.contains(index)) {
						ids += index+",";
						usedCentroids.add(index);
					}
				}
				System.out.println("Indexes Taken as Centroids - "+ids);

			}
			try {
				BufferedWriter writer = new BufferedWriter(new FileWriter(centroidFilePath, false));
				int clusNum = 1;
				StringTokenizer tok = new StringTokenizer(ids, ",");
				while (tok.hasMoreTokens()) {
					int x = Integer.parseInt(tok.nextToken());
					StringBuffer sb = new StringBuffer();
					for (DoubleWritable j : geneData.get(x).geneVals) {
						sb.append(j.toString() + "\t");
					}
					writer.write(clusNum + "\t" + sb.toString() + "\n");
					++clusNum;
				}

				
				writer.close();
				Configuration con = new Configuration();
				FileSystem fs = FileSystem.get(con);
				fs.copyFromLocalFile(false, true, new Path(centroidFilePath), new Path(ip_Path));
				fs.close();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static void jaccardCoeff(DataStore ds, DataStore clusResult) {	//function to calculate Jaccard Coefficient
		int[][] ground = new int[ds.geneData.size()][ds.geneData.size()];
		for (int i = 0; i < ground.length; i++) {
			int clus1 = ds.geneData.get(i + 1).clusterNumber.get();
			for (int j = 0; j < ground.length; j++) {
				int clus2 = ds.geneData.get(j + 1).clusterNumber.get();
				if (clus1 == clus2 && clus1 != -1) {
					ground[i][j] = 1;
				}
			}
		}

		int[][] clustMat = new int[clusResult.geneData.size()][clusResult.geneData.size()];
		for (int i = 0; i < clustMat.length; i++) {
			int clus1 = clusResult.geneData.get(i + 1).clusterNumber.get();
			for (int j = 0; j < clustMat.length; j++) {
				int clus2 = clusResult.geneData.get(j + 1).clusterNumber.get();
				if (clus1 == clus2 && clus1 != -1) {
					clustMat[i][j] = 1;
				}
			}
		}

		int m1 = 0;
		int m0 = 0;

		for (int i = 0; i < clustMat.length; i++) {
			for (int j = 0; j < clustMat.length; j++) {
				if (clustMat[i][j] == 1 && ground[i][j] == 1) {
					m1++;
				} else if (clustMat[i][j] != ground[i][j]) {
					m0++;
				}
			}
		}

		double num = m1;
		double den = m0 + m1;
		double jaccardCoEff = num / den;
		System.out.print("Jaccard Co-effcient - ");
		System.out.println(jaccardCoEff * 100 + "%");

	}
}

