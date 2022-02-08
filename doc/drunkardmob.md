## DrunkardMob

### How to run

1. download the repository
```
git clone git@github.com:GraphChi/graphchi-java.git
```

2. set the java version = 1.8
3. change scala-library version

```xml
<dependency>
  <groupId>org.scala-lang</groupId>
  <artifactId>scala-library</artifactId>
  <version>2.12.1</version>
</dependency>
```

4. compile

```
mvn assembly:assembly -DdescriptorId=jar-with-dependencies
```

5. run PersonalizedPageRank application

```
java -Xmx4096m -cp target/graphchi-java-0.2.2-jar-with-dependencies.jar edu.cmu.graphchi.apps.randomwalks.PersonalizedPageRank  --graph=/home/hsc/dataset/livejournal/soc-LiveJournal1.txt --nshards=4 --niters=5 --nsources=10000 --firstsource=0 --walkspersource=4000
```