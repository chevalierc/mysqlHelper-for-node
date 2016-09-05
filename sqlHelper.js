var mysql = require( 'mysql' );


// db_data {
//     {{db_name}}: {
//         pool: pool,
//         {{tale_name}}{
//              columns: [],
//              pk: ""
//          }
//     }
// }

var db_data = {}
var default_db_name = ""
var log_sql = false;
var log_errors = true;


var connect = function( config ) {
    var pool = mysql.createPool( config )
    var db_name = config.database;
    db_data[ db_name ] = {
        'pool': pool
    };
    default_db_name = db_name;

    console.log( "MySQL connected to database %s.", db_name )
    console.log( "%s is now the current Database.", db_name )
}

var config = function( config ) {
    if ( config.log_sql ) {
        log_sql = config.log_sql;
    }
    if ( config.log_errors ) {
        log_errors = config.log_errors
    }
    if ( config.default_db ) {
        default_db_name = config.default_db
        console.log( "%s is now the current Database.", default_db )
    }
}

var checkForTableInformation = function( query_obj, cb ) {
    var db_name = query_obj.db_name
    var table = query_obj.table
    var columns = db_data[ db_name ][ table ]
    if ( ( columns == null ) || ( columns == undefined ) ) {
        query( {
            db_name: query_obj.db_name,
            sql: "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? and TABLE_SCHEMA = ?;",
            values: [ query_obj.table, query_obj.db_name ]
        }, function( err, rows ) {
            if ( !err ) {

                var table_info = {}
                table_info.params = [];

                for ( var i = 0; i < rows.length; i++ ) {
                    var field = rows[ i ].COLUMN_NAME
                    var isPK = false;
                    if ( rows[ i ].COLUMN_KEY != undefined ) {
                        isPK = ( rows[ i ].COLUMN_KEY == "PRI" )
                    }

                    if ( isPK ) {
                        table_info.pk = field;
                    } else {
                        table_info.params.push( field )
                    }
                }

                db_data[ db_name ][ table ] = table_info
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

var query = function( query_obj, cb ) {
    query_obj = clean_query_obj( query_obj )
    var sql = query_obj.sql;
    var db_name = query_obj.db_name
    var values = null;
    if ( query_obj.values ) {
        values = query_obj.values
    }

    var pool = db_data[ db_name ].pool

    if ( pool ) {
        if ( values ) {
            sql = mysql.format( sql, values );
        }
        if ( log_sql ) {
            console.log( sql )
        }
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

var get = function( query_obj, cb ) {
    query_obj = clean_query_obj( query_obj )
    checkForTableInformation( query_obj, function() {
        var id = query_obj.id
        var table = query_obj.table
        var db_name = query_obj.db_name
        var pk = db_data[ db_name ][ table ].pk

        query_obj.sql = "select * from ?? where ?? = ? Limit 1;";
        query_obj.values = [ table, pk, id ]
        query( query_obj, cb );
    } );
}

var all = function( query_obj, cb ) {
    query_obj = clean_query_obj( query_obj )
    query_obj.sql = "select * from ??";
    query_obj.values = [ query_obj.table ]

    query( sql, function( err, rows, cols ) {
        cb( err, rows, cols )
    } );
}

var find = function( query_obj, cb ) {
    query_obj = clean_query_obj( query_obj )
    query_obj = buildFindStatement( query_obj )
    query( query_obj, cb );
}

var findOne = function( query_obj, cb ) {
    query_obj = clean_query_obj( query_obj )
    query_obj = buildFindStatement( query_obj )
    query_obj.sql += " limit 1;" //escape last AND , and limti to 1

    query( query_obj, function( err, rows, cols ) {
        if ( cols == 0 ) {
            cb( err, null, null )
        } else {
            cb( err, rows[ 0 ], cols )
        }
    } );
}

var buildFindStatement = function( query_obj ) {
    var table = query_obj.table
    var conditions = query_obj.find_object
    query_obj.sql = "select * from ?? where "
    query_obj.balues = [ table ]
    for ( field in conditions ) {
        if ( typeof conditions[ field ] === 'object' ) {
            for ( operator in conditions[ field ] ) {
                query_obj.sql += " ( ?? ?? ?) AND "
                var value = conditions[ field ][ operator ]
                query_obj.values.push( [ field, operator, value ] )
            }
        } else {
            query_obj.sql += "( ?? = ?) AND "
            var val = conditions[ field ]
            query_obj.values.push( [ field, val ] )
        }

    }
    query_obj.sql += "true "

    return query_obj
}


var create = function( query_obj, cb ) {
    query_obj = clean_query_obj( query_obj )
    checkForTableInformation( query_obj, function() {
        query_obj.sql = "Insert into ?? SET ?"
        query_obj.values = [ query_obj.table, query_obj.object ]
        query( query_obj, cb );
    } );
}

var update = function( query_obj, cb ) {
    query_obj = clean_query_obj( query_obj )
    checkForTableInformation( query_obj, function() {
        var table = query_obj.table
        var db_name = query_obj.db_name
        var object = query_obj.object
        var pk = db_data[ db_name ][ table ].pk
        var id = object[ pk ]

        query_obj.sql = "update ?? set ? where ?? = ? "
        query_obj.values = [ table, object, pk, id ]

        query( query_obj, cb );
    } );
}

var remove = function( query_obj, cb ) {
    query_obj = clean_query_obj( query_obj )
    checkForTableInformation( query_obj, function() {
        var id = query_obj.id
        var table = obj.table
        var db_name = query_obj.db_name
        var pk = db_data[ db_name ][ table ].pk
        var id = query_obj[ pk ]
        query_obj.sql = "delete from ?? where ?? = ?"
        query_obj.values = [ table, pk, id ]
        query( query_obj, cb );
    } );
}

var error = function( err, sql ) {
    if ( log_errors ) {
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

var clean_query_obj = function( query_obj ) {
    var isNull = ( !query_obj.db_name )
    var isUndefined = ( query_obj.db_name == undefined )
    if ( isNull || isUndefined ) {
        query_obj.db_name = default_db_name
    }
    if ( query_obj.values == undefined ) {
        query_obj.values = null
    }
    return query_obj
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
