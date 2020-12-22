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
.bashrc
```
# sudo update-alternatives --config java
# export JAVA_HOME="/usr/lib/jvm/java-15-oracle"
export JAVA_HOME=$(update-alternatives --query java | grep Value | cut -d" " -f2 | sed 's!\(\/.*\)jre\(.*\)!\1!g; ; s!/bin/java!!g')
export JDK_HOME=${JAVA_HOME}
export JRE_HOME=${JDK_HOME}/jre/
```

## Maven

### Reinstall 
BUGFIX: `WARNING: An illegal reflective access operation has occurred`
- https://linuxize.com/post/how-to-install-apache-maven-on-ubuntu-20-04/
```
wget https://www-us.apache.org/dist/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz -P /tmp
sudo tar xf /tmp/apache-maven-*.tar.gz -C /opt
sudo ln -s /opt/apache-maven-3.6.3 /opt/maven
sudo update-alternatives --install /usr/bin/mvn mvn /opt/apache-maven-3.6.3/bin/mvn 100
sudo update-alternatives --config mvn
```
.bashrc
```
export M2_HOME=/opt/maven
export MAVEN_HOME=/opt/maven
export PATH=${M2_HOME}/bin:${PATH}
```


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
