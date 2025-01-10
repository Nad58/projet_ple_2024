//mvn package;~/spark-3.5.0-bin-hadoop3/bin/spark-submit --master local --deploy-mode client target/TPSpark-0.0.1.jar

/*
 * result
 * [Deck 08355b count: 1311.0 winrate: 64% nbplayers : 101 strengthW: 0.233451536643026, 
 *  Deck 082935 count: 1342.0 winrate: 64% nbplayers : 101 strengthW: 0.2571531791907514, 
 *  Deck 0c2137 count: 1441.0 winrate: 64% nbplayers : 101 strengthW: -0.008802816901408451, 
 *  Deck 2f444f count: 1111.0 winrate: 64% nbplayers : 101 strengthW: -0.008438818565400843, 
 *  Deck 2f4448 count: 1340.0 winrate: 63% nbplayers : 101 strengthW: 0.026848591549295774, 
 *  Deck 292f4f count: 1297.0 winrate: 63% nbplayers : 101 strengthW: 0.009784587378640778, Deck 2f484f count: 1105.0 winrate: 63% nbplayers : 101 strengthW: 0.00641025641025641, Deck 3d5a6d count: 1449.0 winrate: 63% nbplayers : 101 strengthW: 0.1800789760348584, Deck 08646b count: 1394.0 winrate: 63% nbplayers : 101 strengthW: 0.25736126840317103, Deck 0c2940 count: 1633.0 winrate: 63% nbplayers : 101 strengthW: 0.30680164888457806, Deck 022f6d count: 1147.0 winrate: 63% nbplayers : 101 strengthW: 0.271667817679558, Deck 08355e count: 1865.0 winrate: 63% nbplayers : 101 strengthW: 0.21659940526762958, Deck 122f6d count: 1075.0 winrate: 62% nbplayers : 101 strengthW: 0.19534711964549484, Deck 071637 count: 1589.0 winrate: 62% nbplayers : 101 strengthW: 0.2124248496993988, Deck 406265 count: 2575.0 winrate: 62% nbplayers : 101 strengthW: 0.3887600494743352, Deck 0b0c37 count: 7277.0 winrate: 62% nbplayers : 101 strengthW: 0.3228983998246383, Deck 0c3750 count: 2637.0 winrate: 62% nbplayers : 101 strengthW: 0.2405852994555354, Deck 0c2a2b count: 2301.0 winrate: 62% nbplayers : 101 strengthW: 0.17747395833333332, Deck 2a2b37 count: 1948.0 winrate: 62% nbplayers : 101 strengthW: 0.09336483155299918, Deck 071137 count: 4246.0 winrate: 62% nbplayers : 101 strengthW: 0.2523331447963801]
 */

package crtracker;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class DeckGenerator {
	private static int factorial(int num) {
		if (num < 2) {
			return 1;
		}
		return num * factorial(num - 1);
	}

	// Calcul de C(n, k)
	public static int combinations(int n, int k) {
		int numerator = factorial(n);
		int denominator = factorial(k) * factorial(n - k);
		// Le résultat final
		int result = Math.round((float) numerator / denominator);
		return result;
	}

	public static ArrayList<ArrayList<Integer>> generateCombinations(int n, int k) {
		ArrayList<Integer> elements = new ArrayList<Integer>();
		for (int x = 0; x < n; ++x)
			elements.add(x);

		ArrayList<ArrayList<Integer>> result = new ArrayList<ArrayList<Integer>>();
		generate(new ArrayList<Integer>(), 0, elements, k, result);
		return result;
	}

	private static void generate(ArrayList<Integer> current, int start, ArrayList<Integer> elements, int k,
			ArrayList<ArrayList<Integer>> result) {
		if (current.size() == k) {
			result.add(new ArrayList<>(current));
			return;
		}
		for (int i = start; i < elements.size(); i++) {
			current.add(elements.get(i));
			generate(current, i + 1, elements, k, result);
			current.remove(current.size() - 1);
		}
	}



/*
{"date":"2024-09-26T11:29:09Z","game":"gdc","mode":"CW_Battle_1v1","round":0,"type":"riverRacePvP","winner":1,"players":[{"utag":"#YLUGLG29L","ctag":"#YCCQULJ0","trophies":9000,"ctrophies":5066,"exp":57,"league":8,"bestleague":9,"deck":"000b2f3138565a5e","evo":"0b","tower":"","strength":14.4375,"crown":1,"elixir":1.01,"touch":1,"score":100},{"utag":"#JU0VQRQU","ctag":"#YC8R0RJ0","trophies":9000,"ctrophies":5442,"exp":63,"league":9,"bestleague":9,"deck":"0d25374045596062","evo":"3740","tower":"","strength":15.25,"crown":2,"elixir":1.48,"touch":1,"score":200}],"warclan":{"day":3,"hour_seg":0,"period":"112-3","training":[false,false]}}
{"date":"2024-09-26T11:29:08Z","game":"gdc","mode":"CW_Battle_1v1","round":0,"type":"riverRacePvP","winner":0,"players":[{"utag":"#JU0VQRQU","ctag":"#YC8R0RJ0","trophies":9000,"ctrophies":5442,"exp":63,"league":9,"bestleague":9,"deck":"0d25374045596062","evo":"3740","tower":"","strength":15.25,"crown":2,"elixir":1.48,"touch":1,"score":200},{"utag":"#YLUGLG29L","ctag":"#YCCQULJ0","trophies":9000,"ctrophies":5066,"exp":57,"league":8,"bestleague":9,"deck":"000b2f3138565a5e","evo":"0b","tower":"","strength":14.4375,"crown":1,"elixir":1.01,"touch":1,"score":100}],"warclan":{"day":3,"hour_seg":0,"period":"112-3","training":[false,false]}}
*/
	/*
	 * Read battles from the master data sets then compute statistics for each decks
	 * subdecks can be also computes (1 2 3 4 ... ) cards by passing the argument.
	 * each time statistics about the best evolution and best towers card are
	 * provided
	 */

	public static void main(String[] args) {
		final int[] CARDSGRAMS = { 4,  6, 7, 8 };
		//final int[] CARDSGRAMS = { 1,  2, 3,  4, 3, 6, 7, 8 };
		final int[] CARDSCOMBI = { 8, 28, 56, 70, 56, 28, 8, 1 };
		String path = null;

		if(args.length > 0) {
			path = args[0];
		} else {
			System.out.println("No path specified, using default path clash_middle.nljson");
			path = "";
		}

		ArrayList<ArrayList<ArrayList<Integer>>> combs = new ArrayList<ArrayList<ArrayList<Integer>>>();

		for (int k : CARDSGRAMS) {
			combs.add(generateCombinations(8, k));
		}

		SparkConf conf = new SparkConf().setAppName("Deck Generator");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<Battle> clean = CRTools.getDistinctRawBattles(sc, CRTools.WEEKS, path);

		JavaPairRDD<String, Deck> rdddecks = clean.flatMapToPair((x) -> {
			if (x.players.get(0).deck.length() != 16 || x.players.get(1).deck.length() != 16
					|| (Math.abs(x.players.get(0).strength - x.players.get(1).strength)) > 0.75
					|| x.players.get(0).touch != 1 || x.players.get(1).touch != 1
					)
				return new ArrayList<Tuple2<String, Deck>>().iterator();

			if ((x.players.get(0).bestleague < 6 || x.players.get(1).bestleague < 6))
				return new ArrayList<Tuple2<String, Deck>>().iterator();

			ArrayList<String> tmp1 = new ArrayList<>();
			for (int i = 0; i < 8; ++i)
				tmp1.add(x.players.get(0).deck.substring(i * 2, i * 2 + 2));
			ArrayList<String> tmp2 = new ArrayList<>();
			for (int i = 0; i < 8; ++i)
				tmp2.add(x.players.get(1).deck.substring(i * 2, i * 2 + 2));
			ArrayList<Tuple2<String, Deck>> res = new ArrayList<>();
			for (ArrayList<ArrayList<Integer>> aa : combs)
				for (ArrayList<Integer> cmb : aa)
					treatCombination(x, tmp1, tmp2, res, cmb);
			return res.iterator();
		});

		//System.out.println("rdd decks generated : " + rdddecks.count());

		final int PLAYERS = 10;
		final int BATTLES = 80;

		JavaRDD<Deck> stats = rdddecks.reduceByKey((x, y) -> x.merge(y)).values()
				.filter((Deck x) -> {
					if (x.id.length() == 16)
						return x.players.size() >= PLAYERS && x.count >= BATTLES;
					else
						return x.players.size() >= PLAYERS && x.count >= BATTLES;
				}); // Modifier le if - else 
				/* .filter((Deck x) -> {
    if (x.id.length() == 16)
        return x.players.size() >= PLAYERS && x.count >= BATTLES;
    else
        return x.players.size() >= PLAYERS && x.count >= BATTLES;
});
 */

		//System.out.println("rdd decks reduced : " + stats.count());

		ArrayList<JavaRDD<Deck>> statistics = new ArrayList<JavaRDD<Deck>>();
		for (int cn : CARDSGRAMS) {
			statistics.add(stats.filter((Deck x) -> x.id.length() / 2 == cn));
		}
		class WinrateComparator implements Comparator<Deck>, Serializable {
			@Override
			public int compare(Deck x, Deck y) {
				if (y.count == 0 && x.count != 0)
					return 1;
				if (x.count == 0 && y.count != 0)
					return -1;
				if (x.count == 0 && y.count == 0)
					return 0;
				if ((double) x.win / x.count > (double) y.win / y.count)
					return 1;
				else if ((double) x.win / x.count < (double) y.win / y.count)
					return -1;
				return 0;
			}
		}

		final int NB_DECKS = 100000;

		BufferedWriter writer = null;

		try {
			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream("best_deck.json"), "utf-8"));
			writer.write("{\n");

			boolean firsta = true;

			for (int i = 0; i < CARDSGRAMS.length; ++i) {
				//System.out.println("kgram " + i + " > " + statistics.get(i).count());
			}
			
			for (int i = 0; i < CARDSGRAMS.length; ++i) {
				if (!firsta)
					writer.write(",\n");
				firsta = false;
				writer.write("\"" + CARDSGRAMS[i] + "\": {\n");
				writer.write("\"cards\":" + combs.get(i) + ",\n");
				writer.write("\"decks\":[\n");
				boolean first = true;
				for (Deck d : statistics.get(i).top(NB_DECKS, new WinrateComparator())) {
					if (!first)
						writer.write(",\n");
					first = false;
					writer.write((d.toString()).replace("'", "\""));
				}
				writer.write("]\n}");
			}
			writer.write("}\n");
		} catch (IOException ex) {
			// Report
		} finally {
			try {
				writer.close();
			} catch (Exception ex) {
				/* ignore */}
		}
		System.out.println("OK !!!!!!!!!!!!");
		sc.close();
	}

	private static void treatCombination(Battle x, ArrayList<String> tmp1, ArrayList<String> tmp2,
			ArrayList<Tuple2<String, Deck>> res, ArrayList<Integer> cmb) {
		StringBuffer c1 = new StringBuffer();
		StringBuffer c2 = new StringBuffer();
		for (int i : cmb) {
			c1.append(tmp1.get(i));
			c2.append(tmp2.get(i));
		}
		Deck d1;
		Deck d2;
	if (cmb.size() == 8) {		
		d1 = new Deck(c1.toString(), x.players.get(0).evo, x.players.get(0).tower, 1, x.winner,
				x.players.get(0).strength - x.players.get(1).strength, x.players.get(0).utag,
				x.players.get(0).league, x.players.get(0).ctrophies);
		d2 = new Deck(c2.toString(), x.players.get(1).evo, x.players.get(1).tower, 1, 1 - x.winner,
				x.players.get(1).strength - x.players.get(0).strength, x.players.get(1).utag,
				x.players.get(1).league, x.players.get(1).ctrophies);
	}
	else {
		d1 = new Deck(c1.toString(), "", "", 1, x.winner,
				x.players.get(0).strength - x.players.get(1).strength, x.players.get(0).utag,
				x.players.get(0).league, x.players.get(0).ctrophies);
		d2 = new Deck(c2.toString(), "", "", 1, 1 - x.winner,
				x.players.get(1).strength - x.players.get(0).strength, x.players.get(1).utag,
				x.players.get(1).league, x.players.get(1).ctrophies);
	}
		res.add(new Tuple2<>(d1.id, d1));
		res.add(new Tuple2<>(d2.id, d2));
	}

}
