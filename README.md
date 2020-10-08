# Simple SQL Builders

Light weight, zero dependency java.sql.PreparedStatement builders.

## Installation
You can find this library on maven central
#### Maven
```xml
<dependency>
  <groupId>com.evanwht.simple-sql-builder</groupId>
  <artifactId>simple-sql-builder</artifactId>
  <version>1.1</version>
</dependency>
```
#### gradle
```groovy
compile 'com.evanwht.simple-sql-builder:simple-sql-builder:1.1' 
```

## Usage

You can build simple SELECT, DELETE, UPDATE, and INSERT statements. To interact with the builders and make sure
the sql statements they generate will work correctly, table definitions need to be created. The easiest way is
to create a enum that implements the Column interface.

```java
enum MyDBColumns implements Column {
    ID("id", Types.INTEGER),
    NUM("num", Types.INTEGER),
    NAME("name", Types.VARCHAR);
    
    // implement methods
}
```
#### INSERT
```java
OptionalLong insertedId = new InsertBuilder<>()
    .table("my_table")
    .value(MyDBColumns.NUM, 42)
    .value(MyDBColumns.NAME, "DeepThought")
    .execute(connection);
```
#### SELECT
```java
Optional<String> name = new SelectBuilder<>(rs -> rs.getString(MyDBColumns.NAME))
    .table("my_table")
    .select(MyDBColumns.NAME)
    .where(MyDBColumns.ID, 1)
    .where(MyDBColumns.NUM, 42)
    .getOne(connection);
```
## License
[MIT](https://choosealicense.com/licenses/mit/)