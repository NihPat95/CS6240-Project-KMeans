package kmeans;

import java.util.Arrays;

/***
 * Singlenton class to get an instance of Eucledian
 */
public class Eucledian {
   private static Eucledian eucledian = null;
   private Eucledian(){}

   public static Eucledian getInstance(){
       if (eucledian == null) eucledian = new Eucledian();
       return  eucledian;
   }


    /***
     * Returns the similarity between two points
     * The dimensions of both the points must match
     * @param p1 Point 1
     * @param p2 Point 2
     * @return
     * @throws Exception
     */

   public double similarity(Point p1, Point p2) throws Exception {
       double x1[] = p1.getVector();
       double x2[] = p2.getVector();

       if(x1.length != x2.length){
           System.out.println(Arrays.toString(x1));
           System.out.println(Arrays.toString(x2));
           throw new Exception("Length of point must match");
       }

       double result = 0.0;
       for(int i=0; i<x1.length; i++){
           result = result + Math.pow(x1[i]-x2[i], 2);
       }

       return result;
   }

}
