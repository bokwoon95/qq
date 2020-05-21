initially borne out of a frustration with the stdlib's NullInt64/NullString. It's super gross when my HTML templates sometimes have {{.Uid}} and sometimes it's {{.Uid.Int64}}. When can you tell which one you must use? You have to trace the HTML template back to go file that seeded the template data. There's not 'jump to definition' for that. Plus HTML templates kind of have non-existent typechecking so you only find errors at runtime. It suuuper doesn't scale.

The alternative is to stick to strings and ints in my structs, and unmarshal possible null values into an intermediate NullInt64/NullString and then populate the struct accordingly. Super verbose, and basically I can't use sqlx's StructScan anymore. And hey, doesn't Go already have perfectly fine 'null' values?! Just give me the zero value! But what if I need to know if I really received an empty string or if it was because it was null? I'll just ask separately instead. And thus the rows.Int/rows.IsNullInt split was born.

+ when I was manually unmarshaling, I realized I could just stick with plain database/sql because I only used sqlx for StructScan.

+ sqlx only maps fields 1-to-1, it can't help when I need to set a a 'Valid' flag to true or false depending on whether I received a null uid.

+ sqlx handles nested/embedded structs, but doesn't handle multiple nested/embedded struct of the same type because it only maps 1-to-1.

+ Oh my god stop making me use your struct tag DSL to unmarshal my values just let me use code. This is what rows.Int/rows.IsNullInt does. Code is always preferable to a complicated configuration.

### Why ugly MY\_COLUMN\_NAME instead of the idiomatic MyColumnName?
- Capitalized fields are public in Go. SQL uses snake\_case. ALL\_CAPS\_WITH\_UNDERSCORES are not only public fields, they also follow the SQL naming convention. It doesn't follow the Go naming convention, but that's fine because we are dealing with the SQL domain specific language in Go.
- To avoid the uglier 'My\_column\_name'.
- [JOOQ](https://www.jooq.org/doc/latest/manual/getting-started/use-cases/jooq-as-a-sql-builder-with-code-generation/) does it.
- To avoid clashing with interface methods. The Table interface consists of methods like GetName, GetSchema, etc. It means you can no longer have any columns called 'GetName' or 'GetSchema' because they would clash with an interface method. You can sidestep all this by simply following an entirely different naming scheme instead i.e. ALL\_CAPS\_WITH\_UNDERSCORES.
- Honestly clearly highlighting column names by making them SHOUT\_AT\_YOU makes code a lot easier to read than having a mess of CamelHumpsEverywhere.


### Why can't I just do MY\_TABLE.MY\_COLUMN\_NAME? Why myTable := MY\_TABLE(); myTable.MY\_COLUMN\_NAME?

```
Q: How should the syntax MY_TABLE.MY_COLUMN_NAME formulated Go?
A: MY_TABLE is a struct type, and MY_COLUMN_NAME is a struct field.

Q: If I don't instantiate the struct values, MY_TABLE.MY_COLUMN_NAME might be
   some empty string instead of "my_column_name" (the zero value).
A: Then all tables structs should be instantiated from constructor functions
   instead of zero value initialization (MY_TABLE{}).

Q: How do I reference the same table twice in a query e.g. in a self join?
A: Perfect, all tables are already created using a constructor function. You
   simply have to create two tables from the same constructor function in order
   simulate two tables with the same underlying table.
```
```Go
   // Go
   myTableV1, myTableV2 := MY_TABLE(), MY_TABLE()
   From(myTableV1).Join(
       // self-join
       myTableV2,
       myTableV2.ROW_ID.Eq(myTableV1.ROW_ID),
   ).Select(
       myTableV1.COLUMN_NAME,
       myTableV2.COLUMN_NAME,
   )
```
```
Q: How do I alias tables so I don't have to type the long_verbose_table_name?
A: Perfect, tables are already assigned to whatever variable name of your
   choosing. Simply assign it to some variable that is appropriately abbreviated
   to simulate SQL aliases.
```
```SQL
   -- SQL
   FROM my_table AS t SELECT t.my_column_name;
```
```Go
   // Go
   t := MY_TABLE()
   From(t).Select(t.MY_COLUMN_NAME)
```

### Enums are -not- supported
Enums are almost never what you want. Will you ever want to add a new value to the enum? If so, don't use postgres enums. You can't add or remove any enum values without dropping the enum type completely and recreating it.
Even if you are completely sure your enum values will never change, you can still replicate the effect using foreign key references.
Enums are kinda shit really :/.

### Arrays are not supported
I don't know how often people use arrays, but I'd like to support it. I just don't know how to translate that into the type system.
