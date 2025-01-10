# projet-ple-2024

## Spark

### Usage

Pour lancer le programme:

1. ````mvn clean package``` pour générer le jar

2. Copier le jar sur la plateforme

3. Depuis la plateforme LSD : 
``` spark-submit --num-executors 20 --executor-cores 10 --executor-memory 32G --class crtracker.DeckGenerator  --master yarn --deploy-mode client CRTrackerSpark-0.0.1.jar "/user/auber/data_ple/clashroyale2024/clash_huge.nljson" ``` 

