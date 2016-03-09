// Author: Timothy Chu & Michael Wong
// Lab 7
// CPE369 - Section 01

import org.json.JSONObject;

import java.io.File;
import java.util.*;

public class histogramSeq {
   public static Map<String, Integer> locations = new HashMap<>();

   public static void main(String args[]) {
      long start = System.currentTimeMillis();
      if(args.length != 2) {
         System.out.println("format: <input> <output>");
         System.exit(-1);
      }

      File input = new File(args[0]);
      File output = new File(args[1]);

      try {
         Scanner scan = new Scanner(input);
            while(scan.hasNext()) {
            JSONObject json = new JSONObject(scan.nextLine());
            if(json.has("action")) {
               JSONObject action = json.getJSONObject(("action"));
               if(action.get("actionType").equals("Move")) {
                  int x = action.getJSONObject("location").getInt("x");
                  int y = action.getJSONObject("location").getInt("y");
                  String loc = x + "," + y;
                  if(locations.containsKey(loc)) {
                     locations.put(loc, locations.get(loc) + 1);
                  } else {
                     locations.put(loc, 1);
                  }
               }
            }
         }
      } catch (Exception e) {
         System.out.println("Input scanning failed");
         System.exit(-1);
      }

      Set locationSet = locations.keySet();
      Iterator<String> iter = locationSet.iterator();
      while(iter.hasNext()) {
         String temp = iter.next();
         //System.out.println(temp + "   " + locations.get(temp));
      }
      System.out.println("Time: " + (System.currentTimeMillis() - start) + " milliseconds");
   }
}