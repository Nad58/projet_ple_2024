package com.croyale;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.json.JSONObject;
import org.json.JSONArray;

public class ClashRoyaleDataCleaner extends Configured implements Tool {

    // Classe personnalisée pour stocker les informations de nettoyage
    public static class GameRecord implements Writable, Cloneable {
        private String normalizedKey;
        private String originalRecord;
        private long timestamp;

        public GameRecord() {}

        public GameRecord(String normalizedKey, String originalRecord, long timestamp) {
            this.normalizedKey = normalizedKey;
            this.originalRecord = originalRecord;
            this.timestamp = timestamp;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(normalizedKey);
            out.writeUTF(originalRecord);
            out.writeLong(timestamp);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            normalizedKey = in.readUTF();
            originalRecord = in.readUTF();
            timestamp = in.readLong();
        }

        public String getNormalizedKey() {
            return normalizedKey;
        }

        public String getOriginalRecord() {
            return originalRecord;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

    // Mapper pour le nettoyage des données
    public static class DataCleanerMapper extends Mapper<Object, Text, Text, GameRecord> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                JSONObject gameRecord = new JSONObject(value.toString());
                
                // Validation du format JSON
                if (!isValidJsonFormat(gameRecord)) {
                    context.getCounter("DataCleaning", "InvalidJsonFormat").increment(1);
                    return;
                }
                
                // Vérification du nombre de cartes dans les decks
                JSONArray players = gameRecord.getJSONArray("players");
                if (!validateDeckSize(players)) {
                    context.getCounter("DataCleaning", "InvalidDeckSize").increment(1);
                    return;
                }
                
                // Génération d'une clé normalisée
                String normalizedKey = generateNormalizedKey(gameRecord);
                long timestamp = ZonedDateTime.parse(gameRecord.getString("date")).toInstant().toEpochMilli();
                
                GameRecord cleanedRecord = new GameRecord(normalizedKey, value.toString(), timestamp);
                context.write(new Text(normalizedKey), cleanedRecord);
                
            } catch (Exception e) {
                context.getCounter("DataCleaning", "ParseErrors").increment(1);
            }
        }
        
        private boolean isValidJsonFormat(JSONObject gameRecord) {
            return gameRecord.has("date") && 
                   gameRecord.has("game") && 
                   gameRecord.has("players") && 
                   gameRecord.getJSONArray("players").length() == 2;
        }
        
        private boolean validateDeckSize(JSONArray players) {
            for (int i = 0; i < players.length(); i++) {
                JSONObject player = players.getJSONObject(i);
                String deck = player.getString("deck");
                // Vérification que chaque deck a exactement 8 cartes (16 caractères en hex)
                if (deck.length() != 16) return false;
            }
            return true;
        }
        
        private String generateNormalizedKey(JSONObject gameRecord) {
            ZonedDateTime gameTime = ZonedDateTime.parse(gameRecord.getString("date"));
            JSONArray players = gameRecord.getJSONArray("players");
            
            // Tri des joueurs par leur tag pour normalisation
            String[] playerTags = {
                players.getJSONObject(0).getString("utag"),
                players.getJSONObject(1).getString("utag")
            };
            java.util.Arrays.sort(playerTags);
            
            // Clé composée des tags de joueurs et de l'horodatage tronqué
            return playerTags[0] + "_" + playerTags[1] + "_" + 
                   gameTime.truncatedTo(ChronoUnit.SECONDS).toString();
        }
    }

    // Reducer pour ajouter les clés normalisées directement au JSON en sortie
public static class DataCleanerReducer extends Reducer<Text, GameRecord, Text, Text> {
    private static final long MAX_TIME_DIFF_MS = 10000; // 10 secondes

    @Override
    public void reduce(Text key, Iterable<GameRecord> values, Context context) 
            throws IOException, InterruptedException {
        
        Set<GameRecord> uniqueRecords = new HashSet<>(); // Pour conserver les enregistrements uniques temporairement

        for (GameRecord record : values) {
            String normalizedKey = record.getNormalizedKey();
            long timestamp = record.getTimestamp();

            boolean isDuplicate = false;

            // Comparer à toutes les parties déjà acceptées pour détecter les doublons proches
            for (GameRecord otherRecord : uniqueRecords) {
                if (areRecordsSimilar(otherRecord, record)) {
                    isDuplicate = true;
                    break;
                }
            }

            if (isDuplicate) {
                context.getCounter("DataCleaning", "SimilarGames").increment(1);
                continue;
            }

            // Ajouter comme partie unique
            uniqueRecords.add(record);
            context.write(null, new Text(record.getOriginalRecord()));
            /* 
            // Ajouter la clé normalisée au JSON original
            try {
                JSONObject originalJson = new JSONObject(record.getOriginalRecord());
                originalJson.put("normalizedKey", normalizedKey);

                // Écrire le JSON enrichi dans le contexte
                context.write(null, new Text(originalJson.toString()));
            } catch (Exception e) {
                context.getCounter("DataCleaning", "JsonWriteErrors").increment(1);
            }*/

        }
    }

    private boolean areRecordsSimilar(GameRecord record1, GameRecord record2) {
        String matchKey1 = generateMatchKey(record1.getNormalizedKey());
        String matchKey2 = generateMatchKey(record2.getNormalizedKey());

        // Clés similaires et différence de temps inférieure à MAX_TIME_DIFF_MS
        return matchKey1.equals(matchKey2) &&
               Math.abs(record1.getTimestamp() - record2.getTimestamp()) < MAX_TIME_DIFF_MS;
    }

    // Méthode utilitaire pour générer une clé standardisée A_B (avec A < B)
    private String generateMatchKey(String normalizedKey) {
        String[] parts = normalizedKey.split("_");
        return parts[0] + "_" + parts[1]; // A_B
    }
}


    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: ClashRoyaleDataCleaner <input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Clash Royale Data Cleaning");
        job.setJarByClass(ClashRoyaleDataCleaner.class);

        // Configuration des entrées/sorties
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Configuration du job
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Configuration du Mapper
        job.setMapperClass(DataCleanerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(GameRecord.class);

        // Configuration du Reducer
        job.setReducerClass(DataCleanerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Lancement du job
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ClashRoyaleDataCleaner(), args));
    }
}
