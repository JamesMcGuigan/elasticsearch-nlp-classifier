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
-DartifactId=kaggle-nlp-disaster-tweets-java \
-DarchetypeArtifactId=maven-archetype-quickstart \
-DarchetypeVersion=1.4 \
-DinteractiveMode=false \
-DoutputDirectory=../
```


### Run
```
mvn compile exec:java --quiet
mvn compile exec:java -Dexec.mainClass="com.jamesmcguigan.nlp.App" -q
java -cp target/classes/ com.jamesmcguigan.nlp.App
mvn package; java -jar target/elasticsearch-nlp-classifier-1.0-SNAPSHOT.jar
```


## Apace Zeppelin
- https://quicknotepadtutorial.blogspot.com/2019/12/how-to-install-apache-zeppelin-on.html

```
lsb_release -cd ; hostname ; hostname -I ; whoami ; getconf LONG_BIT ; java -version ; echo $JAVA_HOME
apt install -y build-essential software-properties-common curl gdebi net-tools wget sqlite3 dirmngr nano lsb-release apt-transport-https -y

wget -c https://apache.mirror.wearetriple.com/zeppelin/zeppelin-0.9.0-preview2/zeppelin-0.9.0-preview2-bin-all.tgz
sudo tar -vxf zeppelin-*-bin-all.tgz -C /opt
sudo mv  /opt/zeppelin-*-bin-all /opt/zeppelin
sudo chown -Rv jamie:jamie /opt/zeppelin/

/opt/zeppelin/bin/zeppelin-daemon.sh start
/opt/zeppelin/bin/zeppelin-daemon.sh stop
/opt/zeppelin/bin/zeppelin-daemon.sh reload
http://127.0.0.1:8080/
```
