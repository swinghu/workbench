

### run with `hadoop jar`
package:

    mvn package

run:

    hadoop jar target/testdrive-hadoop-0.0.1-SNAPSHOT.jar <input> <output>

### run with `java`
package:

    mvn -f pom2.xml clean package

run:

    java -jar target/testdrive-hadoop-0.0.1-SNAPSHOT.jar <input> <output>


###
example data is located at ${this.project}/dataset/input_example.txt
