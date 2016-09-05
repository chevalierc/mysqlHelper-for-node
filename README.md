# Node Mysql-Helper Utility
Node utility for simplifying interactions with a mysql database
Automaticly detects primary key and table columns when needed then stores them for future method calls.

#Basic Usage
```
var dbConfig = database: {
        host: "localhost",
        user: "admin",
        password: 'pw',
        database: "pinballMachine",
        connectionLimit: 20,
    }
sqlHelper.connect( config.database );
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
#All Functions

Configuration
The most recent connected database will be your default database. You can change that using the config file. It will be used for any query you do not specify a database in the query object.
```
sqlHelper.connect( {
        host: "localhost",
        user: "admin",
        password: 'pw',
        database: "pinballMachine",
        connectionLimit: 20,
    } )
```
```
sqlHelper.config( {
    log_sql: true,
    log_errors: true,
    default_db: "customer_backup_db"
} )
```

Querying

`sqlHelper.create( {table, object}, callback)`

`sqlHelper.update( {table, object}, callback)`

`sqlHelper.remove( {table, id}, cb)`

`sqlHelper.get( {table, id}, callback)`

`sqlHelper.find( {table, find_object} , callback)`

`sqlHelper.findOne( {tableName, find_object}, callback)`

`sqlheper.all( {table}, callback)`

`sqlHelper.query( {sql, values}, callback)`

Object Manipulation

`sqlhelper.pivot(object, pivot_column )`

`sqlHelper.join(parentObject, childrenArray, foreignKey)`

#FindObject Example
```
var find_obj = {
        gender: "male",
        age: {">": 21},
        name: {"!=": "Jeff"}
}
mysqlHelper.find({
        table: "users",
        find_object: find_obj
}, function(err, rows, cols){
        console.log(rows)
}
```

