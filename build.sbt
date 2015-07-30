import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._


assemblySettings

name := "BigdataElement"

version := "1.0"

scalaVersion := "2.10.4"

javacOptions ++= Seq("-encoding","UTF-8")


libraryDependencies ++= Seq(
  ("org.apache.spark" % "spark-assembly_2.10" % "1.1.0-cdh5.2.0-SNAPSHOT" % "provided"),
  "org.apache.hadoop" % "hadoop-client" % "2.5.0-mr1-cdh5.2.0"%  "provided"
//  "net.debasishg" % "redisclient_2.10" % "2.12",
//  "redis.clients" % "jedis" % "2.1.0",
//  "com.esotericsoftware.kryo" % "kryo" % "2.24.0",
//  "org.mybatis" % "mybatis" % "3.1.1",
//  "org.scalatest" % "scalatest_2.10" % "2.1.0" % "test"
//  "org.antlr" % "antlr-runtime" % "3.4",
//  "dom4j" % "dom4j" % "1.6.1",
//  "mysql" % "mysql-connector-java" % "5.1.14",
//  "jaxen" % "jaxen" % "1.1.6",
//  "redis.clients" % "jedis" % "2.1.0",
//  "org.apache.zookeeper" % "zookeeper" % "3.4.5-cdh5.2.0",
//  "org.apache.thrift" % "libthrift" % "0.9.0",
//  "org.apache.hbase" % "hbase-client" % "0.98.6-cdh5.2.0",
//  "org.apache.hbase" % "hbase-common" % "0.98.6-cdh5.2.0"
)


//resolvers += "Sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/"

//resolvers += Resolver.url("cloudera", url("https://repository.cloudera.com/artifactory/cloudera-repos/"))

resolvers +="cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

//resolvers += Resolver.url("MavenOfficial", url("http://repo1.maven.org/maven2"))

//resolvers += Resolver.url("springside", url("http://springside.googlecode.com/svn/repository"))

//resolvers += Resolver.url("jboss", url("http://repository.jboss.org/nexus/content/groups/public-jboss"))

//resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

//resolvers += "Local Maven Repository" at "file:///Users/wq/lib/m2/repository"

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case PathList("com", "esotericsoftware", "minlog", xs @ _*) => MergeStrategy.first
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", xs @ _*)=> MergeStrategy.first
  case PathList("org", "eclipse", xs @ _*) => MergeStrategy.first
  case PathList("akka",  xs @ _*)=> MergeStrategy.first
  case PathList("org","jboss", xs @ _*) => MergeStrategy.first
  case PathList("*","netty", xs @ _*) => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last endsWith "jboss-beans.xml" => MergeStrategy.filterDistinctLines
  case "application.conf" => MergeStrategy.concat
  case "unwanted.txt"     => MergeStrategy.discard
  case x => old(x)
}
}
