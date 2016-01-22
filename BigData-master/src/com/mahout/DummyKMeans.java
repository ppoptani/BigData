//package mahout;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Vector;
//
//import org.apache.mahout.clustering.dirichlet.UncommonDistributions;
//import org.apache.mahout.clustering.fuzzykmeans.FuzzyKMeansClusterer;
//import org.apache.mahout.clustering.fuzzykmeans.SoftCluster;
//import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
//import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
//import org.apache.mahout.math.DenseVector;
//import org.apache.mahout.math.VectorWritable;
//
//public class DummyKMeans {
//	//static List<Vector> sampleData = new ArrayList<Vector>();
//
//	public static void main(String args[]) {
//		
//		
//		List<Vector> sampleData = new ArrayList<Vector>();
//		 generateSamples(sampleData, 400, 1, 1, 3);
//		 generateSamples(sampleData, 300, 1, 0, 0.5);
//		 generateSamples(sampleData, 300, 0, 2, 0.1);
//		 int k = 3;
//		 List<Vector> randomPoints
//		 = RandomPointsUtil.chooseRandomPoints(sampleData, k);
//		 List<SoftCluster> clusters = new ArrayList<SoftCluster>();
//		 int clusterId = 0;
//		 
//		 for (Vector v : randomPoints) {
//			 clusters.add(new SoftCluster(v, clusterId++,
//			 new EuclideanDistanceMeasure()));
//			 }
//			 List<List<SoftCluster>> finalClusters
//			 = FuzzyKMeansClusterer.clusterPoints(sampleData,
//			 clusters, new EuclideanDistanceMeasure(),
//			 0.01, 3, 10);
//			 for(SoftCluster cluster : finalClusters.get(
//			 finalClusters.size() - 1)) {
//			 System.out.println("Fuzzy Cluster id: "
//			 + cluster.getId()
//			 + " center: " + cluster.getCenter().asFormatString());
//			 }
//			 }
//		
//		
//		
////		generateSamples(sampleData, 400, 1, 1, 3);
////		generateSamples(sampleData, 300, 1, 0, 0.5);
////		generateSamples(sampleData, 300, 0, 2, 0.1);       
////
////		//create the first cluster vectors
////		int k =3;
////		List<Vector> randomPoints = RandomSeedGenerator.chooseRandomPoints(sampleData,k);
////		List<Cluster> clusters = new ArrayList<Cluster>();
////
////		// associate the cluster with the random point
////		int clusterId = 0;
////		for (Vector v : randomPoints) {
////		  clusters.add(new Cluster(v, clusterId++));
////		}
////		// execute the kmeans cluster
////		List<List<Cluster>> finalClusters = KMeansClusterer.clusterPoints(points, clusters, new EuclideanDistanceMeasure(), 3, 0.01); 		
////
////		// display final cluster center
////		for(Cluster cluster : finalClusters.get(finalClusters.size() - 1)) {
////		  System.out.println("Cluster id: " + cluster.getId() + " center: "+ cluster.getCenter().asFormatString());
////		}     
//	}
//	
//	public static void generateSamples(List vectors, int num,double mx, double my, double sd) {
//		for (int i = 0; i < num; i++) {
//		   	sampleData.add(
//		        new DenseVector(new double[] {UncommonDistributions.rNorm(mx,sd),UncommonDistributions.rNorm(my, sd)}));
//		        }
//		    }
//	
//}
