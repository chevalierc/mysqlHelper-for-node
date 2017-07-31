//REQUIRED MODULES

var mysql = require( 'mysql' );

//PRIVATE VARIABLES

var db_data = {}
var default_db_name = ""
var log_sql = false;
var log_errors = true;

//CONFIGURATION PUBLIC METHODS

var connect = function( config, cb ) {
    var db_name = config.database;
    if ( db_data[ db_name ] == undefined ) {
        var pool = mysql.createPool( config )
        db_data[ db_name ] = {
            'pool': pool,
            tables: {}
        };
        default_db_name = db_name;

        console.log( "MySQL connected to database %s.", db_name )
        console.log( "%s is now the current Database.", db_name )

        get_db_columns( db_name, cb )
    } else {
        cb( db_data )
    }
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

var get_db_columns = function( db_name ) {
    return db_data[ db_name ].tables
}

var is_connected_to = function( db_name ) {
    if ( db_data[ db_name ] == undefined ) return false
    if ( db_data[ db_name ].pool = undefined ) return false
    return true
}

//CONFIGURATION PRIVATE METHODS

var get_db_columns = function( db_name, cb ) {
    query( {
        db_name: db_name,
        sql: "SELECT * FROM INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA = ?;",
        values: [ db_name ]
    }, function( err, rows, cols ) {
        if ( !err ) {
            for ( var i = 0; i < rows.length; i++ ) {
                var column_name = rows[ i ].COLUMN_NAME
                var table_name = rows[ i ].TABLE_NAME
                var column_type = rows[ i ].DATA_TYPE

                var is_pk = false;
                if ( rows[ i ].COLUMN_KEY != undefined ) {
                    is_pk = ( rows[ i ].COLUMN_KEY == "PRI" )
                }

                //create object for new table in db_data
                if ( db_data[ db_name ].tables[ table_name ] == undefined ) {
                    db_data[ db_name ].tables[ table_name ] = {
                        columns: []
                    }
                }

                if ( is_pk ) {
                    db_data[ db_name ].tables[ table_name ].pk = column_name;
                }

                var column_data = {
                    name: column_name,
                    type: column_type
                }
                db_data[ db_name ].tables[ table_name ].columns.push( column_data )

            }
            cb( db_data )
        } else {
            console.log( err )
            cb()
        }
    } )
}

//QUERY PUBLIC METHODS

var query = function( query_obj, cb ) {
    query_obj = clean_query_obj( query_obj )
    var sql = query_obj.sql;
    var db_name = query_obj.db_name
    var pool = db_data[ db_name ].pool
    var query_values = null;
    if ( query_obj.values ) {
        query_values = query_obj.values
    }

    if ( pool ) {
        if ( query_values ) {
            sql = mysql.format( sql, query_values );
        }
        if ( log_sql ) {
            console.log( sql )
        }
        console.log(sql)
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
    var id = query_obj.id
    var table = query_obj.table
    var db_name = query_obj.db_name
    var pk = db_data[ db_name ].tables[ table ].pk

    query_obj.sql = "select * from ?? where ?? = ? Limit 1;";
    query_obj.values = [ table, pk, id ]
    query( query_obj, cb );
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
    query_obj.sql += " limit 1;"

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
    var conditions = query_obj.find
    query_obj.sql = "select * from ?? where "
    query_obj.values = [ table ]
    for ( field in conditions ) {
        if ( typeof conditions[ field ] === 'object' ) {
            for ( operator in conditions[ field ] ) {
                query_obj.sql += " ( ?? ?? ?) AND "
                var value = conditions[ field ][ operator ]
                query_obj.values.push( field, operator, value )
            }
        } else {
            query_obj.sql += " ( ?? = ?) AND "
            var value = conditions[ field ]
            query_obj.values.push( field, value )
        }

    }
    query_obj.sql += "true "

    return query_obj
}

var create = function( query_obj, cb ) {
    query_obj = clean_query_obj( query_obj )
    var table = query_obj.table
    var object = query_obj.object
    var db = query_obj.db_name
    object = clean_object_for_insertion( table, object, db )
    query_obj.sql = "Insert into ?? SET ?"
    query_obj.values = [ table, object ]
    query( query_obj, cb );
}

var update = function( query_obj, cb ) {
    query_obj = clean_query_obj( query_obj )
    var table = query_obj.table
    var object = query_obj.object
    var db = query_obj.db_name
    var pk_column_name = db_data[ db ].tables[ table ].pk
    var id = object[ pk_column_name ]
    object = clean_object_for_insertion( table, object, db )
    query_obj.sql = "update ?? set ? where ?? = ? "
    query_obj.values = [ table, object, pk, id ]

    query( query_obj, cb );
}

var remove = function( query_obj, cb ) {
    query_obj = clean_query_obj( query_obj )
    var id = query_obj.id
    var table = query_obj.table
    var db_name = query_obj.db_name
    var pk_column_name = db_data[ db_name ].tables[ table ].pk
    query_obj.sql = "delete from ?? where ?? = ?"
    query_obj.values = [ table, pk_column_name, id ]
    query( query_obj, cb );
}

var populate = function( query_obj, cb ) {
    //
    cb()
}

//QUERY PRIVATE METHODS

var clean_object_for_insertion = function( table_name, dirty_object, db_name ) {
    var table_columns = db_data[ db_name ].tables[ table_name ].columns
    var pk_name = db_data[ db_name ].tables[ table_name ].pk
    table_columns.push( {
        name: pk_name
    } )
    var clean_object = {}
    for ( var i = 0; i < table_columns.length; i++ ) {
        var column_name = table_columns[ i ].name
        if ( dirty_object[ column_name ] != undefined ) {
            clean_object[ column_name ] = dirty_object[ column_name ]
        }
    }
    return clean_object
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

var error = function( err, sql ) {
    if ( log_errors ) {
        console.log( "ERROR: " )
        console.log( err )
        console.log( "SQL:  " )
        console.log( sql )
    }
}


//EXPORT OF PUBLIC METHODS TO USER
module.exports = {
    config: config,
    connect: connect,
    get_db_columns: get_db_columns,
    is_connected_to: is_connected_to,
    all: all,
    get: get,
    find: find,
    findOne: findOne,
    create: create,
    update: update,
    remove: remove,
    query: query,
    populate: populate
}
