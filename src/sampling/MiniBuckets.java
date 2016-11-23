package sampling;

import java.util.Hashtable;
import java.util.Iterator;

public class MiniBuckets {
	public String coordinates;
	public int countOfPointsInside;
	public Hashtable<String,Integer> neighbors;
	public double mini_similarity = Double.POSITIVE_INFINITY;
	public String simi_neighbors;
	public double totalSize;
	public int num_dims;
	public double totalCost;
	public String algorithmAssigned;
	
	public MiniBuckets(String coordinates, int countOfPoints, Hashtable<String,Integer> neighbors, int num_dims){
		this.coordinates = coordinates;
		this.countOfPointsInside = countOfPoints;
		this.neighbors = new Hashtable<String,Integer>();
		this.num_dims = num_dims;
		
		// learn coordinates from string
		String []splits = this.coordinates.split(",");
		double multiplier = 1;
		for(int i = 0; i < num_dims; i ++){
			multiplier *= (Integer.valueOf(splits[2*i+1])-Integer.valueOf(splits[2*i]));
		}
		this.totalSize = multiplier;
		
		for(Iterator itr = neighbors.keySet().iterator(); itr.hasNext();){ 
			String keyiter = (String) itr.next(); 
			Integer value = (Integer) neighbors.get(keyiter); 
			this.neighbors.put(keyiter, value);
		}
		this.totalCost = 0.0;
		this.algorithmAssigned = "";
	}
	
	public void init_similar_statistics(){
		double small_s = Double.POSITIVE_INFINITY;
		String s_n = "";
		for(Iterator itr = neighbors.keySet().iterator(); itr.hasNext();){ 
			String keyiter = (String) itr.next(); 
			Integer value = (Integer) neighbors.get(keyiter); 
			String []split_neighbor = keyiter.split(",");
			double multiplier_neighbor = 1;
			for(int i = 0; i < num_dims; i ++){
				multiplier_neighbor *= (Integer.valueOf(split_neighbor[2*i+1])-Integer.valueOf(split_neighbor[2*i]));
			}
			double density_diff = Math.abs(value/multiplier_neighbor*1.0-countOfPointsInside/totalSize*1.0);
			if(density_diff < small_s){
				small_s = density_diff;
				s_n = keyiter;
			}
		}
		this.mini_similarity = small_s;
		this.simi_neighbors = s_n;
	}
	
	public void printMiniBuckets(){
		System.out.println("Coordinates: "+ coordinates);
		System.out.println("total size of the bucket: "+ totalSize);
		System.out.println("couont of points in this bucket "+ countOfPointsInside);
		System.out.println("number of dimensions: "+ num_dims);
		System.out.println("smallest similarity: "+mini_similarity);
		System.out.println("the most similar neighbor coordinate: "+ simi_neighbors);
		
		System.out.println("neighbors: ");
		for(Iterator itr = neighbors.keySet().iterator(); itr.hasNext();){ 
			String keyiter = (String) itr.next(); 
			Integer value = (Integer) neighbors.get(keyiter); 
			System.out.println(keyiter+ "---------"+value);
		}
	}
}
