package sampling;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

public class partitionToReducer {

	List<PartitionInfo> partitionList = new ArrayList<PartitionInfo>();
	List<List<PartitionInfo>> pList = new ArrayList<List<PartitionInfo>>();
	int reduce_num;
	double total_count = 0;
	double max_accept_records_per = 20000; //////////////

	public class PartitionInfo {
		int p_id;
		String p_startEnd;
		double p_cost;
		double p_num_records;

		public PartitionInfo(int p_id, String p_startEnd, double p_cost, double p_num_records) {
			this.p_cost = p_cost;
			this.p_id = p_id;
			this.p_startEnd = p_startEnd;
			this.p_num_records = p_num_records;
		}
	}

	public partitionToReducer(Hashtable<String, MiniBuckets> ht_miniBB, int reduce_num) {
		int m = 0;
		for (Iterator itr = ht_miniBB.keySet().iterator(); itr.hasNext();) {
			String keyiter = (String) itr.next();
			PartitionInfo tempPartitionInfor = new PartitionInfo(m, ht_miniBB.get(keyiter).coordinates,
					ht_miniBB.get(keyiter).totalCost, ht_miniBB.get(keyiter).countOfPointsInside);
			partitionList.add(tempPartitionInfor);
			total_count += ht_miniBB.get(keyiter).totalCost;
			m++;
		}
		this.reduce_num = reduce_num;
	}

	public List<List<PartitionInfo>> sort() {
		int partitionSize = partitionList.size();
		double avg_cost = Math.ceil(total_count / reduce_num);
		System.out.println("Average Cost: " + avg_cost);
		System.out.println("Total count: " + total_count);
		System.out.println("Reduce number: " + reduce_num);
		// sort partition list according to the cost
		if (reduce_num <= partitionSize) {
			Collections.sort(partitionList, new Comparator<PartitionInfo>() {
				public int compare(PartitionInfo p1, PartitionInfo p2) {
					return Double.valueOf(p1.p_cost).compareTo(Double.valueOf(p2.p_cost));
				}
			});
			// sorted partition list by cost
			/*
			 * for(PartitionInfo p : partitionList){
			 * 
			 * System.out.println(" id :"+ p.p_id + "  cost:"+p.p_cost); }
			 */
			int i = 0;
			int j = partitionList.size() - 1;
			for (int k = 0; k < reduce_num; k++) {
				// System.out.println(" For the "+ (k+1)+" th reducer:");
				List<PartitionInfo> tempList = new ArrayList<PartitionInfo>();
				double part_count = 0;
				double part_record_num = 0;
				if (i > j)
					break;
				if (i <= j) {
					tempList.add(partitionList.get(j));
					part_count += partitionList.get(j).p_cost;
					part_record_num += partitionList.get(j).p_num_records;
					j--;
					while ((i <= j) && (part_count < 0.8* avg_cost) && (part_record_num < max_accept_records_per)) {
						if (partitionList.get(j).p_cost > (part_count - avg_cost)) {
							tempList.add(partitionList.get(i));
							part_count += partitionList.get(i).p_cost;
							part_record_num += partitionList.get(i).p_num_records;
							i++;
						} else {
							tempList.add(partitionList.get(j));
							part_count += partitionList.get(j).p_cost;
							part_record_num += partitionList.get(j).p_num_records;
							j--;
						}
						// System.out.println("i = "+ i + " j = "+j +"
						// part_count = "+ part_count);
					}
				} // end if(i<=j)
				pList.add(k, tempList);
			} // end for (int k = 0; k< reduce_num;k++)

		} // end if (reduce_num <= partitionSize)
		return pList;
	} // end sort()

}
