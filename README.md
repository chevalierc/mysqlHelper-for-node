# Node Mysql-Helper Utility
### Node utility for simplifying interactions with a mysql database.

### About:

Built to make DB interaction as simple as possible, while still handling all basic application needs.
Automaticly detects primary key and table columns when needed then stores them for future method calls. Will prevent sql injection.

### Install:

Uses the official [mysql driver](https://www.npmjs.com/package/mysql). Make sure that is installed. Otherwise simply use the require() method to reference this file when needed.

### Prefer Oracle?

I have built an [additional helper for oracle databses](https://github.com/chevalierc/oracleHelper-for-node). It is not as tested and not injection-proof at the moment.


# Basic Usage
```
var dbConfig = database: {
        host: "localhost",
        user: "admin",
        password: 'pw',
        database: "pinballMachine",
        connectionLimit: 20,
    }
    
sqlHelper.connect( dbConfig );

sqlHelper.config( {
    log_sql: true,
    log_errors: true
} );

var id = 233
sqlhelper.get({
        table:"users",
        id: id
}, function(err, rows, cols){
    if(!err){
        var user = rows[0]
    }
})
```
# All Functions

## Configuration

The most recent connected database will be your default database. You can change that using the config file. It will be used for any query you do not specify a database in the query object. 

```
var config = {
        host: "localhost",
        user: "admin",
        password: 'pw',
        database: "pinballMachine",
        connectionLimit: 20,
}
sqlHelper.connect( config, function(){
        query_db()
})

//The Callback is recomended but not required. During connection the helper grabs table columns information for its use in several methods, specificaly: get(), create(), update(), remove() & populate()
```
```
sqlHelper.config( {
    log_sql: true,                      //log executed sql
    log_errors: true,                   //log errors
    default_db: "customer_backup_db"    //default Databse, that will be used if a database is not specified in a query_object
} )
```

## Querying

All methods follow the format: `sqlHelper.method( query_object, callback )`. If you wish to query on a specific database add its name to the `query_object` with the parameter `db_name`.

See example for how to use a find_object

`sqlHelper.create( {table, object}, callback)`

`sqlHelper.update( {table, object}, callback)`

`sqlHelper.remove( {table, id}, cb)`

`sqlHelper.get( {table, id}, callback)`

`sqlHelper.find( {table, find_object} , callback)`

`sqlHelper.findOne( {tableName, find}, callback)`

`sqlHeper.all( {table}, callback)`

`sqlhelper.populate( {join_structure, sql, values}, callback)`

`sqlHelper.query( {sql, values}, callback)`

## Object Manipulation

`sqlHelper.pivot(object, pivot_column )`

`sqlHelper.join(parentObject, childrenArray, foreignKey)`

# More Examples

## FindObject Example
```
var find_obj = {
        gender: "male",
        age: {">": 21},
        name: {"!=": "Jeff"}
}
mysqlHelper.find({
        table: "users",
        find: find_obj
}, function(err, rows, cols){
        console.log(rows)
}
```

## Populate Example (extreme joining)

```
var join_structure = {
        table: "car",
        children: [ {
            table: "wheel",
            fk: "car_fk",
            children: [ {
                table: "bolt",
                fk: "wheel_fk"
            }, {
                table: "rubber",
                fk: "wheel_fk"
            } ]
        } ]
}
var extra_sql = "where card.make = ?"
var values = "Volvo"

mysqlHelper.populate( {
        structure: join_structure,
        sql: extra_sql,
        values: values
}, function(err, cars, cols){
        console.log(cars)
}
```

## Query with values Example
Using the values array you can avoid sqlInjection. Use `??` for column and table names and `?` for values
```
mysqlHelper.query({
        sql: "Select * from ?? where id = ?",
        values: ["users", user_id]
}, function(err, rows, cols){
        console.log(rows)
}
```

## Multiple Database Conections
```
var dbConfig1 = database: {
        host: "localhost",
        user: "admin",
        password: 'pw',
        database: "pinballMachine",
        connectionLimit: 20,
    }
var dbConfig2 = database: {
        host: "localhost",
        user: "admin",
        password: 'pw',
        database: "sodaPopDatabase",
        connectionLimit: 30,
    }
    
sqlHelper.connect( dbConfig1 );
sqlHelper.connect( dbConfig2 );

mysqlHelper.query({
        db_name: "pinballMachine",
        sql: "Select * from users"
}, function(err, rows, cols){
        console.log(rows)
}
```


