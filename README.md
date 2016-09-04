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
    logSQL: true,
    logErros: true
} );

var id = 233
sqlhelper.get("users", id, function(err, rows, cols){
    if(!err){
        var user = rows[0]
    }
})
```
#All Functions

Configuration

`sqlHelper.connect( config )`

`sqlHelper.config( config )`

Querying

`sqlHelper.create( tableName, object, callback)`

`sqlHelper.update( tableName, object, callback)`

`sqlHelper.remove( tableName, id, cb)`

`sqlHelper.get( tableName, id, callback)`

`sqlHelper.find( tableName, conditions as object {field: val, ... } , callback)`

`sqlHelper.find( tableName, conditions as object {field: {operator: val}}, ... } , callback)`

`sqlHelper.findOne( tableName, condition object, callback)`

`sqlheper.all(tableName, callback)`

`sqlHelper.query(sql, callback)`

Object Manipulation

`sqlhelper.pivot(object, pivot_column )`

`sqlHelper.join(parentObject, childrenArray, foreignKey)`

#Find Example
```
var findConditions = {
        gender: "male",
        age: {">": 21},
        name: {"!=": "Jeff"}
}
mysqlHelper.find("users", findConditions, function(err, rows, cols){
        console.log(rows)
}
```

