package crtracker;

import java.io.Serializable;
import java.util.ArrayList;

import org.codehaus.jackson.annotate.JsonProperty;
/*
{"date":"2024-09-13T07:27:05Z","game":"gdc","mode":"Rage_Ladder","round":0,"type":"riverRacePvP",
"winner":0,
"players":[{"utag":"#C82LPR8J","ctag":"#QQ9QGJYJ","trophies":9000,"ctrophies":1100,"exp":67,"league":6,
"bestleague":10,
"deck":"0a18323d4a4c616b",
"evo":"0a18",
"tower":"",
"strength":15.375,
"crown":2,
"elixir":1.56,
"touch":1,
"score":200},
{"utag":"#9UC2GUJVP","ctag":"#QQJCR9CP","trophies":7160,"ctrophies":1012,"exp":46,"league":1,"bestleague":4,"deck":"05070c14171f445e","evo":"","tower":"","strength":13,"crown":0,"elixir":6.14,"touch":1,"score":100}],"warclan":{"day":3,"hour_seg":3,"period":"112-1","training":[false,false]}}
*/


class Player implements Serializable{
	@JsonProperty("utag")
	public String utag;
	@JsonProperty("ctag")
	public String ctag;
	@JsonProperty("trophies")
	public int trophies;
	@JsonProperty("ctrophies")
	public int ctrophies;
	@JsonProperty("exp")
	public int exp;
	@JsonProperty("league")
	public int league;
	@JsonProperty("bestleague")
	public int bestleague;
	@JsonProperty("deck")
	public String deck;
	@JsonProperty("evo")
	public String evo;
	@JsonProperty("tower")
	public String tower;
	@JsonProperty("strength")
	public double strength;
	@JsonProperty("elixir")
	public double elixir;
	@JsonProperty("touch")
	public int touch;
	@JsonProperty("score")
	public int score;
}
class WarClan implements Serializable{
	@JsonProperty("day")
	public int day=0;
	@JsonProperty("hourd_seg")
	public int hour_seg=0;
	@JsonProperty("period")
	public String period;
	@JsonProperty("training")
	ArrayList<Boolean> training;	
}

class Battle implements Serializable{
	@JsonProperty("date")
	public String date;
	@JsonProperty("game")
	public String game;
	@JsonProperty("mode")
	public String mode;
	@JsonProperty("round")
	public int round;
	@JsonProperty("type")
	public String type;
	@JsonProperty("winner")
	public int winner;
	@JsonProperty("players")
	ArrayList<Player> players;
	@JsonProperty("warclan")
	WarClan warclan;

}