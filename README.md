# Usage

N.B. the url/username/password are hard-coded in the code. change them
to appropriate values for your test.

```
mvn clean install -DskipTests
java -jar target/postgres-jdbc-test-1.0-SNAPSHOT-executable.jar
```

# expected output

```
execute query with extended protocol and fetch size of 0
1500491767
took: 15576 ms
execute query with extended protocol and fetch size of 1000
1500491767
took: 19519 ms
execute query with simple query mode and fetch size of 0
1500491767
took: 4250 ms
execute query with simple query mode and fetch size of 1000
1500491767
took: 10996 ms
```
