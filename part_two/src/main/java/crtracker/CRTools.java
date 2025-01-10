package crtracker;

import java.time.Instant;
import java.util.ArrayList;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.gson.Gson;

import scala.Console;
import scala.Tuple2;

public class CRTools {
    public static final int WEEKS = 9;

    public static JavaRDD<Battle> getDistinctRawBattles(JavaSparkContext sc, int weeks, String path) {

        if(path == null || path.isEmpty()) {
            System.out.println("No path specified, using default path");
            path = "/user/auber/data_ple/clashroyale2024/clash_big.nljson";
            System.out.println("Path: " + path);
        } else {
            System.out.println("Using path: " + path);
        }

        JavaRDD<String> rdd = sc.textFile(path).filter((x) -> {
            return !x.isEmpty();
        });
        System.out.println("battles " + rdd.count());

        // distinct battles
        JavaRDD<Battle> rddpair = rdd.mapToPair((x) -> {
            Gson gson = new Gson();
            Battle d = gson.fromJson(x, Battle.class);
            String u1 = d.players.get(0).utag;
            String u2 = d.players.get(1).utag;
            return new Tuple2<>(d.date + "_" + d.round + "_"
                    + (u1.compareTo(u2) < 0 ? u1 + u2 : u2 + u1), d);
        }).distinct().values();

        Instant sliding_window = Instant.now().minusSeconds(3600 * 24 * 7 * weeks);
        Instant collect_start = Instant.parse("2024-09-26T09:00:00Z");

		rddpair = rddpair.filter((Battle x) -> {
			Instant inst = Instant.parse(x.date);
			return inst.isAfter(sliding_window) && inst.isAfter(collect_start);
    	});

        JavaPairRDD<String, Iterable<Battle>> rddbattles = rddpair.mapToPair((d) -> {
            String u1 = d.players.get(0).utag;
            String u2 = d.players.get(1).utag;
            double e1 = d.players.get(0).elixir;
            double e2 = d.players.get(1).elixir;
            return new Tuple2<>(d.round + "_"
                    + (u1.compareTo(u2) < 0 ? u1 + e1 + u2 + e2 : u2 + e2 + u1 + e1), d);
        }).groupByKey();//Optimisable avec combineByKey ?

        JavaRDD<Battle> clean = rddbattles.values().flatMap((it) -> {
            ArrayList<Battle> lbattles = new ArrayList<>();
            ArrayList<Battle> rbattles = new ArrayList<>();
            for (Battle bi : it)
                lbattles.add(bi);
            lbattles.sort((Battle x, Battle y) -> {
                if (Instant.parse(x.date).isAfter(Instant.parse(y.date)))
                    return 1;
                if (Instant.parse(y.date).isAfter(Instant.parse(x.date)))
                    return -1;
                return 0;
            });
            rbattles.add(lbattles.get(0));
            for (int i = 1; i < lbattles.size(); ++i) {
                long i1 = Instant.parse(lbattles.get(i - 1).date).getEpochSecond();
                long i2 = Instant.parse(lbattles.get(i).date).getEpochSecond();
                if (Math.abs(i1 - i2) > 10)
                    rbattles.add(lbattles.get(i));
            }
            return rbattles.iterator();
        });
       
        return clean;
    }

    
}
