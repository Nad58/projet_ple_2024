package crtracker;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

class Deck implements Serializable {
	Set<String> players = new TreeSet<String>();
	Map<String, Integer> evos = new TreeMap<String, Integer>();
	Map<String, Integer> wevos = new TreeMap<String, Integer>();
	Map<String, Integer> towers = new TreeMap<String, Integer>();
	Map<String, Integer> wtowers = new TreeMap<String, Integer>();
	public String id;
	int count = 0;
	int win = 0;
	double strength = 0;
	int nplayers = 0;
	int league = 0;
	int trophy = 0;

	public Deck(String str, String evo, String tower, int count, int win, double strength, String player, int league,
			int trophy) {
		ArrayList<String> tmp = new ArrayList<>();
		for (int i = 0; i < str.length() / 2; ++i)
			tmp.add(str.substring(i * 2, i * 2 + 2));
		tmp.sort((x, y) -> x.compareTo(y));
		StringBuffer tmpS = new StringBuffer();
		for (String s : tmp)
			tmpS.append(s);
		id = tmpS.toString();
		this.count = count;
		this.win = win;
		this.strength = strength;
		this.league = league;
		this.trophy = trophy;
		players.add(player);
		for (int i = 0; i < evo.length() / 2; ++i) {
			String key = evo.substring(i * 2, i * 2 + 2);
			evos.put(key, 1);
			if (win > 0)
				wevos.put(key, 1);
			else
				wevos.put(key, 0);
		}
		for (int i = 0; i < tower.length() / 2; ++i) {
			String key = tower.substring(i * 2, i * 2 + 2);
			towers.put(key, 1);
			if (win > 0)
				wtowers.put(key, 1);
			else
				wtowers.put(key, 0);
		}
	}

	Deck merge(Deck b) {
		this.count += b.count;
		this.win += b.win;
		this.strength += b.strength;
		this.league = Math.max(b.league, this.league);
		this.trophy = Math.max(b.trophy, this.trophy);

		for (String x : b.players) {
			if (players.size() > 10) break;
			players.add(x);
		}

		for (String key : b.evos.keySet()) {
			if (!this.evos.containsKey(key)) {
				this.evos.put(key, b.evos.get(key));
				this.wevos.put(key, b.wevos.get(key));
			} else {
				this.evos.put(key, this.evos.get(key) + b.evos.get(key));
				this.wevos.put(key, this.wevos.get(key) + b.wevos.get(key));
			}
		}
		for (String key : b.towers.keySet()) {

			if (!this.towers.containsKey(key)) {
				this.towers.put(key, b.towers.get(key));
				this.wtowers.put(key, b.wtowers.get(key));
			} else {
				this.towers.put(key, this.towers.get(key) + b.towers.get(key));
				this.wtowers.put(key, this.wtowers.get(key) + b.wtowers.get(key));
			}

		}

		return this;
	}

	public String toString() {
		double winrate = 0;
		double stren = 0;
		if (count > 0) {
			winrate = Math.round(win * 1000 / count) / 10.;
			stren = strength / count;
		}

		String sevo = "[";
		boolean first = true;
		for (String key : evos.keySet()) {
			if (!first)
				sevo += ",";
			first = false;
			sevo += "['" + key + "'," + evos.get(key) + "," + Math.round(wevos.get(key) * 1000. / evos.get(key)) / 10.
					+ "]";
		}
		sevo += ']';

		String stower = "[";
		first = true;
		for (String key : towers.keySet()) {
			if (!first)
				stower += ",";
			first = false;
			stower += "['" + key + "'," + towers.get(key) + ","
					+ Math.round(wtowers.get(key) * 1000. / towers.get(key)) / 10. + "]";
		}
		stower += ']';

		return "{'id':'" + id + "', 'evos':" + sevo + ", 'towers':" + stower
				+ ", 'count':" + count + ", 'winrate':" + winrate + ", 'nbplayers':" + players.size() + ", 'win':" + win
				+ ", 'strength':" + stren
				+ ", 'league':" + league
				+ ", 'ctrophy':" + trophy + "}";
	}

}