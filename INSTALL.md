# Install

## Datasets
Dataset already committed to git
```
./download.sh
```

## Upgrade to Java 15
```
sudo add-apt-repository ppa:linuxuprising/java
sudo apt update
sudo apt install oracle-java15-installer

sudo update-alternatives --config java
```


## Maven

### Initialize
```
mvn archetype:generate \
-DgroupId=com.jamesmcguigan.kdt \
-DartifactId=kaggle-disaster-tweets-nlp-java \
-DarchetypeArtifactId=maven-archetype-quickstart \
-DarchetypeVersion=1.4 \
-DinteractiveMode=false \
-DoutputDirectory=../
```

### Run
```
mvn compile exec:java --quiet
mvn compile exec:java -Dexec.mainClass="com.jamesmcguigan.kdt.App" -q
java -cp target/classes/ com.jamesmcguigan.kdt.App
mvn package; java -jar target/kaggle-disaster-tweets-nlp-java-1.0-SNAPSHOT.jar
```
