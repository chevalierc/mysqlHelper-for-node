var mysql = require( 'mysql' );

//constructorzzz
// Pivot  (data, pivot_column )
// join   (parent, children, foreignKey)
// create ( tableName, object, callback)
// update ( tableName, object, callback)
// remove ( tableName, id, cb)
// get    ( tableName, id(or null), callback)
// find   ( tableName, conditions as object {field: val, ... } , callback) field = val
// find   ( tableName, conditions as object {field: {op: val}}, ... } , callback) field {{op}} val
// query  (sql, callback) --wrapper to hide error handling and logging

var DBfields = {}
var pool = {}
var DBname = ""
var logSQL = false;
var logErrors = true;

var connect = function( config ) {
    pool = mysql.createPool( config )
    DBname = config.database;
    console.log( "MySQL connected!" )
}

var config = function( config ) {
    if ( config.logSQL ) {
        logSQL = config.logSQL;
    }
    if ( config.logErrors ) {
        logErrors = logErrors
    }
}

var checkForTableInformation = function( tableName, cb ) {
    if ( ( DBfields[ tableName ] == null ) || ( DBfields[ tableName ] == undefined ) ) {
        var sql = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '" + tableName + "' and TABLE_SCHEMA = '" + DBname + "';"
        query( sql, function( err, rows, cols ) {
            if ( !err ) {
                DBfields[ tableName ] = {}
                DBfields[ tableName ].params = [];
                for ( var i = 0; i < rows.length; i++ ) {
                    var field = rows[ i ].COLUMN_NAME
                    var isPK = false;
                    if ( rows[ i ].COLUMN_KEY != undefined ) {
                        isPK = ( rows[ i ].COLUMN_KEY == "PRI" )
                    }

                    if ( isPK ) {
                        DBfields[ tableName ].pk = field;
                    } else {
                        DBfields[ tableName ].params.push( field )
                    }

                }
                cb()
            } else {
                error( err, null )
                cb()
            }
        } )
    } else {
        cb()
    }
}

var cleanDates = function( object ) {
    for ( field in object ) {
        if ( date_columns.indexOf( field ) != -1 ) {
            var value = object[ field ]
            if ( value && value != "" ) {
                object[ field ] = moment( value ).format( 'YYYY-MM-DD' )
            } else {
                object[ field ] = null
            }
        }
    }
    return object
}

var error = function( err, sql ) {
    if ( logErrors ) {
        console.log( "ERROR: " )
        console.log( err )
        console.log( "SQL:  " )
        console.log( sql )
    }
}

var pivot = function( data, pivot_column ) {
    var response = {}
    for ( var i = 0; i < data.length; i++ ) {
        var pivot_value = data[ i ][ pivot_column ]
        var row_data = data[ i ]
        if ( response[ pivot_value ] ) {
            response[ pivot_value ].push( row_data )
        } else {
            response[ pivot_value ] = [ row_data ]
        }
    }
    return response
}

var join = function( parent, children, foreignKey ) {
    var response = parent;
    if ( foreignKey && response && children ) {
        response[ foreignKey ] = [];
        response[ foreignKey ] = children
    }
    return response
}

var get = function( table, id, cb ) {
    checkForTableInformation( table, function() {
        var pk = DBfields[ table ].pk
        var sql = "select * from " + table
        if ( id ) {
            sql += " where " + pk + " = " + id + " Limit 1;";
        }
        query( sql, function( err, rows, cols ) {
            cb( err, rows, cols )
        } );
    } );
}

var all = function( table, cb ) {
    var sql = "select * from " + table;

    query( sql, function( err, rows, cols ) {
        cb( err, rows, cols )
    } );
}

var find = function( table, conditions, cb ) {
    var sql = "select * from " + table + " where "
    for ( field in conditions ) {
        if ( typeof conditions[ field ] === 'object' ) {
            for ( operator in conditions[ field ] ) {
                sql += "( " + field + " " + operator + " '" + conditions[ field ][ operator ] + "') AND "
            }
        } else {
            sql += "( " + field + " = '" + conditions[ field ] + "') AND "
        }

    }
    sql += "true;" //escape last AND

    query( sql, function( err, rows, cols ) {
        cb( err, rows, cols )
    } );
}

var findOne = function( table, conditions, cb ) {
    var sql = "select * from " + table + " where "
    for ( field in conditions ) {
        if ( typeof conditions[ field ] === 'object' ) {
            for ( operator in conditions[ field ] ) {
                sql += "( " + field + " " + operator + " '" + conditions[ field ][ operator ] + "') AND "
            }
        } else {
            sql += "( " + field + " = '" + conditions[ field ] + "') AND "
        }
    }
    sql += "true limit 1;" //escape last AND , and limti to 1

    query( sql, function( err, rows, cols ) {
        if ( cols == 0 ) {
            cb( err, null, cols )
        } else {
            cb( err, rows[ 0 ], cols )
        }

    } );
}


var create = function( table, object, cb ) {
    // object = cleanDates( object )
    var usedFields = [];
    var vals = [];
    checkForTableInformation( table, function() {
        var fields = DBfields[ table ].params
        for ( var i = 0; i < fields.length; i++ ) {
            var curField = fields[ i ];
            if ( object[ curField ] ) {
                usedFields.push( fields[ i ] )
            }
        }

        var sql = "Insert into " + table + " ( "
        for ( var i = 0; i < usedFields.length; i++ ) {
            var curField = usedFields[ i ];
            sql += curField
            if ( i != usedFields.length - 1 ) {
                sql += " , "
            }
        }

        sql += ") values ( "

        for ( var i = 0; i < usedFields.length; i++ ) {
            var curField = usedFields[ i ];
            vals.push( object[ curField ] )
            sql += "?"
            if ( i != usedFields.length - 1 ) {
                sql += ", "
            }
        }

        sql += " );"

        //replace the ?'s with the values stored in vals while formatting them for mysql
        sql = mysql.format( sql, vals );

        query( sql, function( err, rows, cols ) {
            cb( err, rows, cols )
        } );

    } );
}


var update = function( table, object, cb ) {
    checkForTableInformation( table, function() {
        var update = [];
        var fields = DBfields[ table ].params
        var pk = DBfieldss[ table ].pk

        //object = cleanDates( object )

        for ( var i = 0; i < fields.length; i++ ) {
            var curField = fields[ i ];
            if ( curField in object ) {
                var curVal = object[ curField ]

                //mysql needs null date values as null not 'null' or ''
                if ( curVal == "" || curVal == null ) {
                    update.push( curField + " = null " )
                } else {
                    update.push( curField + " = '" + object[ curField ] + "'" )
                }

            }
        }

        var sql = "Update " + table + " Set "
        for ( var i = 0; i < update.length; i++ ) {
            sql += update[ i ]
            if ( i != update.length - 1 ) {
                sql += ", "
            }
        }

        sql += " where " + pk + " = " + object.id;
        query( sql, function( err, rows, cols ) {
            cb( err, rows, cols )
        } );
    } );
}

var remove = function( table, id, cb ) {
    checkForTableInformation( table, function() {
        var pk = DBfieldss[ table ].pk
        var sql = "delete from " + table + " where " + pk + " = " + id
        query( sql, function( err, rows, cols ) {
            cb( err, rows, cols )
        } );
    } );
}

var query = function( sql, cb ) {
    if ( pool ) {
        if ( logSQL ) console.log( sql )
        pool.query( sql, function( err, rows, cols ) {
            if ( err ) {
                error( err, sql )
            }
            cb( err, rows, cols )
        } )
    } else {
        cb( null, null, null )
    }
}

module.exports = {
    config: config,
    connect: connect,
    pivot: pivot,
    join: join,
    all: all,
    get: get,
    find: find,
    findOne: findOne,
    create: create,
    update: update,
    remove: remove,
    query: query
}
