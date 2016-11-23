package sampling;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;

import sampling.partitionToReducer.PartitionInfo;
import util.SQConfig;

/**
 * DDrivePartition class contains map and reduce functions for Data Driven
 * Partitioning.
 *
 * @author Yizhou Yan
 * @version Dec 31, 2015
 */
public class CostDrivenPartition {
	/**
	 * default Map class.
	 *
	 * @author Yizhou Yan
	 * @version Dec 31, 2015
	 */

	public static class DDMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		/** incrementing index of divisions generated in mapper */
		public static int increIndex = 0;

		/** number of divisions where data is divided into (set by user) */
		private int denominator = 100;

		/** number of object pairs to be computed */
		static enum Counters {
			MapCount
		}

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			denominator = conf.getInt(SQConfig.strSamplingPercentage, 100);
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int id = increIndex % denominator;
			increIndex++;

			Text dat = new Text(value.toString());
			IntWritable key_id = new IntWritable(id);
			if (id == 0) {
				context.write(key_id, dat);
				context.getCounter(Counters.MapCount).increment(1);
			}
		}
	}

	/**
	 * @author yizhouyan
	 *
	 */
	public static class DDReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
		private MultipleOutputs<NullWritable, Text> mos;
		/**
		 * The dimension of data (set by user, now only support dimension of 2,
		 * if change to 3 or more, has to change some codes)
		 */
		private static int num_dims = 2;
		/**
		 * number of small cells per dimension: when come with a node, map to a
		 * range (divide the domain into small_cell_num_per_dim) (set by user)
		 */
		public static int cell_num = 501;
		/** number of divisions where data is divided into (set by user) */
		private int denominator = 100;

		/** The domains. (set by user) */
		private static float[] domains;

		/** size of each small buckets */
		private static int smallRange;

		/**
		 * Number of desired partitions
		 */
		private static int num_Partitions;
		/**
		 * block list, which saves each block's info including start & end
		 * positions on each dimension. print for speed up "mapping"
		 */
		private static float[][] partition_store;
		/** the total usage of the block list (partition store) */
		private static int indexForPartitionStore = 0;
		/** in order to build hash to speed up mapping process */
		public static Hashtable<Double, StartAndEnd> start_end_points;

		private static int[] partition_size;

		/**
		 * maximal acceptable number of data in one combined block, defined by
		 * user
		 */
		private static int maxAccptNum = 50000;
		/**
		 * define a maximum similarity, if each partition is different, then
		 * break the loop
		 */
		private static int max_Simmilarity = 10000;

		private static int K = 3;

		private static float radius;
		/** number of reducers */
		private static int num_Reducers = 272;

		public static class StartAndEnd {
			public int start_point;
			public int end_point;

			public StartAndEnd(int start_point, int end_point) {
				this.start_point = start_point;
				this.end_point = end_point;
			}
		}

		public void setup(Context context) {
			mos = new MultipleOutputs(context);
			/** get configuration from file */
			Configuration conf = context.getConfiguration();
			num_dims = conf.getInt(SQConfig.strDimExpression, 2);
			cell_num = conf.getInt(SQConfig.strNumOfSmallCells, 501);
			domains = new float[2];
			domains[0] = conf.getFloat(SQConfig.strDomainMin, 0.0f);
			domains[1] = conf.getFloat(SQConfig.strDomainMax, 10001.0f);
			smallRange = (int) Math.ceil((domains[1] - domains[0]) / cell_num);
			num_Partitions = conf.getInt(SQConfig.strNumOfPartitions, 2);
			partition_store = new float[num_Partitions][num_dims * 2];
			partition_size = new int[num_Partitions];
			start_end_points = new Hashtable<Double, StartAndEnd>();
			maxAccptNum = conf.getInt(SQConfig.strMaxAcceptNumberPointsPP, 50000);
			max_Simmilarity = conf.getInt(SQConfig.strMaxSimilarity, 10000);
			denominator = conf.getInt(SQConfig.strSamplingPercentage, 100);
			K = conf.getInt(SQConfig.strK, 3);
			radius = conf.getFloat(SQConfig.strRadius, 1.0f);
			num_Reducers = conf.getInt(SQConfig.strNumOfReducers, 100);
		}

		public double mini_similarity = 0;

		public Hashtable<String, Integer> initNeighbors(Hashtable<String, Integer> miniBuckets, String keyiter,
				int countInKeyIter) {

			Hashtable<String, Integer> neighbors = new Hashtable<String, Integer>();
			if (countInKeyIter >= maxAccptNum)
				return neighbors;
			String[] splits = keyiter.split(",");
			int[][] startAndEnd = new int[num_dims][2];
			for (int i = 0; i < num_dims; i++) {
				startAndEnd[i][0] = Integer.valueOf(splits[2 * i]);
				startAndEnd[i][1] = Integer.valueOf(splits[2 * i + 1]);
			}
			int[][] neighborCoordi = new int[num_dims][2];

			for (int i = 0; i < num_dims; i++) {
				// init neighbors using original point
				for (int j = 0; j < num_dims; j++) {
					neighborCoordi[j][0] = startAndEnd[j][0];
					neighborCoordi[j][1] = startAndEnd[j][1];
				}
				neighborCoordi[i][0] -= 1;
				neighborCoordi[i][1] -= 1;
				// determine if this neighbor is out of range [0,11]
				if (neighborCoordi[i][0] >= 0) {
					String tempNeighbor = "";
					for (int j = 0; j < num_dims; j++) {
						tempNeighbor = tempNeighbor + neighborCoordi[j][0] + "," + neighborCoordi[j][1] + ",";
					}
					tempNeighbor = tempNeighbor.substring(0, tempNeighbor.length() - 1);
					// System.out.println(tempNeighbor);
					int valueOfNei = miniBuckets.get(tempNeighbor);
					if ((valueOfNei + countInKeyIter) < maxAccptNum)
						neighbors.put(tempNeighbor, valueOfNei);
				}
				neighborCoordi[i][0] += 2;
				neighborCoordi[i][1] += 2;
				// determine if this neighbor is out of range [0,11]
				if (neighborCoordi[i][1] <= cell_num) {
					String tempNeighbor = "";
					for (int j = 0; j < num_dims; j++) {
						tempNeighbor = tempNeighbor + neighborCoordi[j][0] + "," + neighborCoordi[j][1] + ",";
					}
					tempNeighbor = tempNeighbor.substring(0, tempNeighbor.length() - 1);
					// System.out.println(tempNeighbor);
					int valueOfNei = miniBuckets.get(tempNeighbor);
					if ((valueOfNei + countInKeyIter) < maxAccptNum)
						neighbors.put(tempNeighbor, valueOfNei);
				}
			} // end for (int i = 0; i < num_dims; i++)
			return neighbors;
		}

		/**
		 * find the most similar bucket to merge
		 * 
		 * @author Yizhou Yan
		 * @version Sep 29, 2015
		 */
		public String findMergeBuckets(Hashtable<String, MiniBuckets> ht_miniBB, double miniSimilarity) {
			String value_smallest = "";
			String keyiter = "";
			double temp_mini = Double.POSITIVE_INFINITY;
			for (Iterator itr = ht_miniBB.keySet().iterator(); itr.hasNext();) {
				keyiter = (String) itr.next();
				MiniBuckets value = ht_miniBB.get(keyiter);
				// System.out.println(value.mini_similarity+"------XXXXXXX");
				if (value.mini_similarity <= miniSimilarity)
					return keyiter;
				if (temp_mini > value.mini_similarity) {
					temp_mini = value.mini_similarity;
					value_smallest = keyiter;
				}
			}
			return value_smallest;
		}

		/**
		 * check if the two buckets can form a rectangle (if they can merge)
		 * 
		 * @author Yizhou Yan
		 * @version Sep 29, 2015
		 */
		public boolean CanFormRectangle(String first, String second) {
			boolean rect = false;
			// first point's coordinate
			String[] firstSplits = first.split(",");
			int[][] FstartAndEnd = new int[num_dims][2];
			for (int i = 0; i < num_dims; i++) {
				FstartAndEnd[i][0] = Integer.valueOf(firstSplits[2 * i]);
				FstartAndEnd[i][1] = Integer.valueOf(firstSplits[2 * i + 1]);
			}
			// second point's coordinate
			String[] secondSplits = second.split(",");
			int[][] SstartAndEnd = new int[num_dims][2];
			for (int i = 0; i < num_dims; i++) {
				SstartAndEnd[i][0] = Integer.valueOf(secondSplits[2 * i]);
				SstartAndEnd[i][1] = Integer.valueOf(secondSplits[2 * i + 1]);
			}
			// A rectangle should only change one dimension and fix others, the
			// changed dimension should be consecutive
			int fixed = 0;
			int changed = 0;
			for (int i = 0; i < num_dims; i++) {
				if ((SstartAndEnd[i][0] == FstartAndEnd[i][0]) && (SstartAndEnd[i][1] == FstartAndEnd[i][1])) {
					fixed++;
				} else if ((SstartAndEnd[i][0] == FstartAndEnd[i][1]) || (SstartAndEnd[i][1] == FstartAndEnd[i][0])) {
					changed++;
				}
			}
			if ((fixed == num_dims - 1) && (changed == 1))
				rect = true;
			return rect;
		}

		/**
		 * Generate the coordinate for new bucket
		 * 
		 * @author Yizhou Yan
		 * @version Sep 29, 2015
		 */
		public String GenerateNewBucketCoor(String first, String second) {
			String newCoor = "";
			// first point's coordinate
			String[] firstSplits = first.split(",");
			int[][] FstartAndEnd = new int[num_dims][2];
			for (int i = 0; i < num_dims; i++) {
				FstartAndEnd[i][0] = Integer.valueOf(firstSplits[2 * i]);
				FstartAndEnd[i][1] = Integer.valueOf(firstSplits[2 * i + 1]);
			}
			// second point's coordinate
			String[] secondSplits = second.split(",");
			int[][] SstartAndEnd = new int[num_dims][2];
			for (int i = 0; i < num_dims; i++) {
				SstartAndEnd[i][0] = Integer.valueOf(secondSplits[2 * i]);
				SstartAndEnd[i][1] = Integer.valueOf(secondSplits[2 * i + 1]);
			}
			// new point's coordinate
			int[][] newPointStartAndEnd = new int[num_dims][2];
			for (int i = 0; i < num_dims; i++) {
				if (SstartAndEnd[i][0] == FstartAndEnd[i][0]) {
					newPointStartAndEnd[i][0] = SstartAndEnd[i][0];
					newPointStartAndEnd[i][1] = SstartAndEnd[i][1];
				} else {
					newPointStartAndEnd[i][0] = Math.min(SstartAndEnd[i][0], FstartAndEnd[i][0]);
					newPointStartAndEnd[i][1] = Math.max(SstartAndEnd[i][1], FstartAndEnd[i][1]);
				}
			}
			for (int j = 0; j < num_dims; j++) {
				newCoor = newCoor + newPointStartAndEnd[j][0] + "," + newPointStartAndEnd[j][1] + ",";
			}
			newCoor = newCoor.substring(0, newCoor.length() - 1);
			return newCoor;
		}

		/**
		 * update information for neighbors. If we merge two buckets into one,
		 * we have to delete old buckets from their neighbors, and find if the
		 * new bucket can be their neighbor
		 * 
		 * @author Yizhou Yan
		 * @version Sep 29, 2015
		 */
		public MiniBuckets UpdateNeighbors(MiniBuckets orgNode, MiniBuckets neighborNode, String newPointCoor,
				int newCountPoints) {
			neighborNode.neighbors.remove(orgNode.coordinates);
			if ((newCountPoints >= maxAccptNum)
					|| ((neighborNode.countOfPointsInside + newCountPoints) >= maxAccptNum)) {
				neighborNode.init_similar_statistics();
				return neighborNode;
			}
			// System.out.println(newPointCoor + "------" +
			// neighborNode.coordinates+
			// CanFormRectangle(newPointCoor,neighborNode.coordinates));

			if (CanFormRectangle(newPointCoor, neighborNode.coordinates)) {
				neighborNode.neighbors.put(newPointCoor, newCountPoints);
			}
			neighborNode.init_similar_statistics();
			return neighborNode;
		}

		/**
		 * For the new bucket, we have to find its neighbors. up-down-left-right
		 * 
		 * @author Yizhou Yan
		 * @version Oct 1, 2015
		 */
		public Hashtable<String, Integer> UpdateNewBurgeonNeighborsForNew(Hashtable<String, MiniBuckets> ht_miniBB,
				Hashtable<String, Integer> neighborsFornew, String newPointCoor, int newCountPoints) {

			if (newCountPoints >= maxAccptNum)
				return neighborsFornew;
			// System.out.println("Neighbors begin :"+ neighborsFornew.size());
			String[] firstSplits = newPointCoor.split(",");
			int[][] FstartAndEnd = new int[num_dims][2];
			for (int i = 0; i < num_dims; i++) {
				FstartAndEnd[i][0] = Integer.valueOf(firstSplits[2 * i]);
				FstartAndEnd[i][1] = Integer.valueOf(firstSplits[2 * i + 1]);
			}

			int[][] neighborCoordi = new int[num_dims][2];

			for (int i = 0; i < num_dims; i++) {
				// init neighbors using original point
				for (int j = 0; j < num_dims; j++) {
					neighborCoordi[j][0] = FstartAndEnd[j][0];
					neighborCoordi[j][1] = FstartAndEnd[j][1];
				}
				neighborCoordi[i][1] = FstartAndEnd[i][0]; // find the front
															// neighbor
				neighborCoordi[i][0] = FstartAndEnd[i][0] - 1;

				// determine if this neighbor is out of range [0,11]
				while (neighborCoordi[i][0] >= 0) {
					String tempNeighbor = "";
					for (int j = 0; j < num_dims; j++) {
						tempNeighbor = tempNeighbor + neighborCoordi[j][0] + "," + neighborCoordi[j][1] + ",";
					}
					tempNeighbor = tempNeighbor.substring(0, tempNeighbor.length() - 1);
					// System.out.println(tempNeighbor);
					// if the bucket in the front exists
					if (ht_miniBB.get(tempNeighbor) != null) {
						int valueOfNei = ht_miniBB.get(tempNeighbor).countOfPointsInside;
						if ((valueOfNei + newCountPoints) < maxAccptNum) {
							if (neighborsFornew.get(tempNeighbor) == null)
								neighborsFornew.put(tempNeighbor, valueOfNei);
						}
						break;
					}
					neighborCoordi[i][0]--;
				}

				neighborCoordi[i][0] = FstartAndEnd[i][1]; // find the next
															// neighbor
				neighborCoordi[i][1] = FstartAndEnd[i][1] + 1;

				// determine if this neighbor is out of range [0,11]
				while (neighborCoordi[i][1] <= cell_num) {
					String tempNeighbor = "";
					for (int j = 0; j < num_dims; j++) {
						tempNeighbor = tempNeighbor + neighborCoordi[j][0] + "," + neighborCoordi[j][1] + ",";
					}
					tempNeighbor = tempNeighbor.substring(0, tempNeighbor.length() - 1);
					// System.out.println(tempNeighbor);
					// if the bucket next exists
					if (ht_miniBB.get(tempNeighbor) != null) {
						int valueOfNei = ht_miniBB.get(tempNeighbor).countOfPointsInside;
						if ((valueOfNei + newCountPoints) < maxAccptNum)
							if (neighborsFornew.get(tempNeighbor) == null) {
								neighborsFornew.put(tempNeighbor, valueOfNei);
							}
						break;
					}
					neighborCoordi[i][1]++;
				}
			} // end for (int i = 0; i < num_dims; i++)
				// System.out.println("Neighbors end :"+
				// neighborsFornew.size());
			return neighborsFornew;
		}

		/**
		 * define buckets to merge and update neighbors
		 * 
		 * @author Yizhou Yan
		 * @version Sep 29, 2015
		 */
		public Hashtable<String, MiniBuckets> UpdateNeighborsAndMerge(Hashtable<String, MiniBuckets> ht_miniBB,
				String needMergeBucket) {

			// 1st merge bucket
			MiniBuckets firstMerge = ht_miniBB.get(needMergeBucket);
			ht_miniBB.remove(needMergeBucket);
			// firstMerge.printMiniBuckets();
			// 2nd merge bucket
			MiniBuckets secondMerge = ht_miniBB.get(firstMerge.simi_neighbors);
			ht_miniBB.remove(secondMerge.coordinates);
			// secondMerge.printMiniBuckets();

			// new bucket statistics
			String newPointCoor = GenerateNewBucketCoor(firstMerge.coordinates, secondMerge.coordinates);
			int newCountPoints = firstMerge.countOfPointsInside + secondMerge.countOfPointsInside;

			// save neighbors for new bucket
			Hashtable<String, Integer> neighborsFornew = new Hashtable<String, Integer>();

			// update neighbors for firstMerge
			for (Iterator itr = firstMerge.neighbors.keySet().iterator(); itr.hasNext();) {
				String keyiter = (String) itr.next();
				if (keyiter.equals(secondMerge.coordinates))
					continue;
				MiniBuckets tempFirst = ht_miniBB.get(keyiter); // neighbor that
																// we want to
																// update
				int org_nn_size = tempFirst.neighbors.size();
				MiniBuckets newtempFirst = UpdateNeighbors(firstMerge, tempFirst, newPointCoor, newCountPoints);
				// update the neighbor in ht_miniBB
				ht_miniBB.remove(keyiter);
				ht_miniBB.put(newtempFirst.coordinates, newtempFirst);
				int new_nn_size = newtempFirst.neighbors.size();
				if (new_nn_size == org_nn_size)
					neighborsFornew.put(newtempFirst.coordinates, newtempFirst.countOfPointsInside);
				// System.out.println("-----------------------------------first
				// neighbor generated------------------------");
				// newtempFirst.printMiniBuckets();
			}
			// update neighbors for secondMerge
			for (Iterator itr = secondMerge.neighbors.keySet().iterator(); itr.hasNext();) {
				String keyiter = (String) itr.next();
				if (keyiter.equals(firstMerge.coordinates))
					continue;
				MiniBuckets tempFirst = ht_miniBB.get(keyiter); // neighbor that
																// we want to
																// update
				int org_nn_size = tempFirst.neighbors.size();
				MiniBuckets newtempFirst = UpdateNeighbors(secondMerge, tempFirst, newPointCoor, newCountPoints);
				// update the neighbor in ht_miniBB
				ht_miniBB.remove(keyiter);
				ht_miniBB.put(newtempFirst.coordinates, newtempFirst);
				int new_nn_size = newtempFirst.neighbors.size();
				if (new_nn_size == org_nn_size)
					neighborsFornew.put(newtempFirst.coordinates, newtempFirst.countOfPointsInside);
				// System.out.println("-----------------------------------second
				// neighbor generated------------------------");
				// newtempFirst.printMiniBuckets();
			}
			neighborsFornew = UpdateNewBurgeonNeighborsForNew(ht_miniBB, neighborsFornew, newPointCoor, newCountPoints);

			// update new point's neighbors
			for (Iterator itr = neighborsFornew.keySet().iterator(); itr.hasNext();) {
				String keyiter = (String) itr.next();
				if (ht_miniBB.get(keyiter).neighbors.get(newPointCoor) == null) {
					ht_miniBB.get(keyiter).neighbors.put(newPointCoor, newCountPoints);
					ht_miniBB.get(keyiter).init_similar_statistics();
				}
			}
			// create new point
			MiniBuckets mb = new MiniBuckets(newPointCoor, newCountPoints, neighborsFornew, num_dims);
			mb.init_similar_statistics();
			// System.out.println("-----------------------------------new point
			// generated------------------------");
			// mb.printMiniBuckets();
			ht_miniBB.put(newPointCoor, mb);
			return ht_miniBB;
		}

		/**
		 * Compute the Volumn for a multi-dimensional ball The formula is V(n) =
		 * 2* pi * radius * radius * V(n-2)/n;
		 */
		public float VolumnForMultiDimBalls(int num_dims, float radius) {
			float volumn;
			if (num_dims % 2 == 1) {
				// n = 1,3,5,7....
				volumn = 2 * radius;
				for (int i = 3; i <= num_dims; i = i + 2) {
					volumn = (float) (2 * Math.PI * radius * radius * volumn / i);
				}
			} else {
				// n = 2,4,6,8....
				volumn = (float) (Math.PI * radius * radius);
				for (int i = 4; i <= num_dims; i = i + 2) {
					volumn = (float) (2 * Math.PI * radius * radius * volumn / i);
				}
			}
			return volumn;
		}

		public Hashtable<String, MiniBuckets> EstimateCost_2(Hashtable<String, MiniBuckets> ht_miniBB) {

			for (Iterator itr = ht_miniBB.keySet().iterator(); itr.hasNext();) {
				String keyiter = (String) itr.next();
				MiniBuckets tempBucket = ht_miniBB.get(keyiter);
				double tempForCellBased_1 = (0.5) * Math.pow((radius / 2), num_dims) * Math.pow(3, num_dims)
						* (tempBucket.countOfPointsInside * denominator
								/ (tempBucket.totalSize * Math.pow(smallRange, num_dims)));
				if (tempForCellBased_1 >= K) {
					ht_miniBB.get(keyiter).algorithmAssigned = "Cell-Based";
					ht_miniBB.get(keyiter).totalCost = tempBucket.countOfPointsInside * denominator;
				} else {
					ht_miniBB.get(keyiter).algorithmAssigned = "Nest-Based";
					ht_miniBB.get(keyiter).totalCost = (tempBucket.countOfPointsInside * denominator
							* tempBucket.totalSize * Math.pow(smallRange, num_dims) * K)
							/ VolumnForMultiDimBalls(num_dims, radius);
				}
			} // end for(Iterator itr = ht_miniBB.keySet().iterator();
				// itr.hasNext();)
			return ht_miniBB;
		}

		public int compareTwoNumbers(double[] aaa, double[] bbb) {
			if (aaa[0] > bbb[0])
				return 1;
			else if (aaa[0] < bbb[0])
				return -1;
			else {
				if (aaa[2] > bbb[2])
					return 1;
				else if (aaa[2] < bbb[2])
					return -1;
				else
					return 0;
			}
		}

		/**
		 * default Reduce class.
		 * 
		 * @author Yizhou Yan
		 * @version Dec 31, 2015
		 */

		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException {
			// init hash table, init all small buckets
			Hashtable<String, Integer> miniBuckets = new Hashtable<String, Integer>();
			for (int i = 0; i < Math.pow(cell_num, num_dims); i++) {
				String str = CellStore.GenerateCellCoordinateInString(i, num_dims, cell_num);
				miniBuckets.put(str, 0);
			}
			int num_mini = miniBuckets.size();

			// collect data
			for (Text oneValue : values) {
				String line = oneValue.toString();
				String newLine = "";
				// rescale data and save to hashtable
				for (int i = 0; i < num_dims; i++) {
					float tempDataPerDim = Float.valueOf(line.split(",")[i + 1]);
					int indexDataPerDim = (int) (Math.floor(tempDataPerDim / smallRange));
					newLine = newLine + indexDataPerDim + "," + (indexDataPerDim + 1) + ",";
				}
				newLine = newLine.substring(0, newLine.length() - 1);

				if (miniBuckets.get(newLine) != null) {
					int tempFreq = miniBuckets.get(newLine);
					miniBuckets.put(newLine, tempFreq + 1);
				} else {
					miniBuckets.put(newLine, 1);
					num_mini = miniBuckets.size();
				}
			} // end for collect
			System.out.println("End Collecting data");

			// Build up neighbors and init basic statistics for mini-buckets
			Hashtable<String, MiniBuckets> ht_miniBB = new Hashtable<String, MiniBuckets>();

			for (Iterator itr = miniBuckets.keySet().iterator(); itr.hasNext();) {
				String keyiter = (String) itr.next();
				Integer value = (Integer) miniBuckets.get(keyiter);
				Hashtable<String, Integer> neighbors = initNeighbors(miniBuckets, keyiter, value);
				MiniBuckets mb = new MiniBuckets(keyiter, value, neighbors, num_dims);
				mb.init_similar_statistics();
				ht_miniBB.put(keyiter, mb);
			}

			// output init buckets and neighbors
			// for (Iterator itr = ht_miniBB.keySet().iterator();
			// itr.hasNext();) {
			// String keyiter = (String) itr.next();
			// MiniBuckets value = ht_miniBB.get(keyiter);
			// if (value.countOfPointsInside > 0){
			// value.printMiniBuckets();
			// }
			// }

			double miniSimilarity = 0;

			System.out.println("End Init mini buckets : " + ht_miniBB.size());
			// Hierarchical clustering and generation of partitioning plan
			int count = 0;
			while (ht_miniBB.size() > num_Partitions) {
				// find bucket that needs merge
				String needMergeBucket = findMergeBuckets(ht_miniBB, miniSimilarity);
				if (needMergeBucket.equals(""))
					break;
				miniSimilarity = ht_miniBB.get(needMergeBucket).mini_similarity; // update
																					// similarity
				if (miniSimilarity >= max_Simmilarity)
					break;
				ht_miniBB = UpdateNeighborsAndMerge(ht_miniBB, needMergeBucket);
				// System.out.println("size of htminiBB : "+ht_miniBB.size()
				// +" miniSimilarity :" + miniSimilarity);
				count++;
				if (count % 1000 == 0)
					System.out.println("Merging mini buckets : " + ht_miniBB.size());
			}

			System.out.println("End merge mini buckets : " + ht_miniBB.size());
			// Cost estimation and generation of execution strategy
			ht_miniBB = EstimateCost_2(ht_miniBB);
			int iii = 0;
			count = 0;
			int countEmptyPartition = 0;
			String outputPPPath = context.getConfiguration().get(SQConfig.strPartitionPlanOutput);
			for (Iterator itr = ht_miniBB.keySet().iterator(); itr.hasNext();) {
				String keyiter = (String) itr.next();
				String saveCostAndAlgorithm = iii + ",";
				String[] sub_splits = ht_miniBB.get(keyiter).coordinates.split(",");
				for (int i = 0; i < sub_splits.length; i++) {
					saveCostAndAlgorithm += (Float.valueOf(sub_splits[i]) * smallRange) + ",";
				}
				saveCostAndAlgorithm += ht_miniBB.get(keyiter).algorithmAssigned;
				if (ht_miniBB.get(keyiter).algorithmAssigned.equals("Nest-Based"))
					count++;
				if (ht_miniBB.get(keyiter).countOfPointsInside == 0)
					countEmptyPartition++;
				try {
					mos.write(NullWritable.get(), new Text(saveCostAndAlgorithm), outputPPPath + "/pp");
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				// if (ht_miniBB.get(keyiter).countOfPointsInside > maxAccptNum)
				// System.out.println(
				// saveCostAndAlgorithm + ", Number of nodes : " +
				// ht_miniBB.get(keyiter).countOfPointsInside);
				iii++;
				// System.out.println("id: "+ iii
				// +"----cost"+value.totalCost);

			}
			System.out.println("Total Partition size: " + ht_miniBB.size() + ", Nest Loop: " + count
					+ ", empty partition: " + countEmptyPartition);
			// Merge partition for reducer
			partitionToReducer ptr = new partitionToReducer(ht_miniBB, num_Reducers);
			List<List<PartitionInfo>> pList = ptr.sort();
			String outputReducerAssignPath = context.getConfiguration().get(SQConfig.outputReducerAssignPath);
			iii = 0;
			for (List<PartitionInfo> lp : pList) {
				iii++;
				System.out.print("reducer: " + iii);
				double totalCa = 0;
				for (PartitionInfo p : lp) {
					try {
						mos.write(NullWritable.get(), new Text(p.p_id + "," + iii),
								outputReducerAssignPath + "/reducer");
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					totalCa = totalCa + (p.p_cost);
					// System.out.println("id " + p.p_id + "-----cost" +
					// p.p_cost);
				}
				System.out.println("total cost " + totalCa);
			}

		} // end reduce function

		public void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
	} // end reduce class

	public void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		new GenericOptionsParser(conf, args).getRemainingArgs();
		/** set job parameter */
		Job job = Job.getInstance(conf, "Distributed Cost Driven Sampling");

		job.setJarByClass(CostDrivenPartition.class);
		job.setMapperClass(DDMapper.class);

		/** set multiple output path */
		MultipleOutputs.addNamedOutput(job, "partitionplan", TextOutputFormat.class, NullWritable.class, Text.class);

		job.setReducerClass(DDReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);

		String strFSName = conf.get("fs.default.name");
		FileInputFormat.addInputPath(job, new Path(conf.get(SQConfig.dataset)));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(conf.get(SQConfig.strSamplingOutput)), true);
		FileOutputFormat.setOutputPath(job, new Path(conf.get(SQConfig.strSamplingOutput)));

		/** print job parameter */
		System.err.println("# of dim: " + conf.getInt(SQConfig.strDimExpression, 10));
		long begin = System.currentTimeMillis();
		job.waitForCompletion(true);
		long end = System.currentTimeMillis();
		long second = (end - begin) / 1000;
		System.err.println(job.getJobName() + " takes " + second + " seconds");
	}

	public static void main(String[] args) throws Exception {
		CostDrivenPartition DDPartition = new CostDrivenPartition();
		DDPartition.run(args);
	}
}
