//REQUIRED MODULES

var mysql = require( 'mysql' );

//PRIVATE VARIABLES

var db_data = {}
var default_db_name = ""
var log_sql = false;
var log_errors = true;

//CONFIGURATION PUBLIC METHODS

var connect = function ( config, cb ) {
    var pool = mysql.createPool( config )
    var db_name = config.database;
    db_data[ db_name ] = {
        'pool': pool
    };
    default_db_name = db_name;

    console.log( "MySQL connected to database %s.", db_name )
    console.log( "%s is now the current Database.", db_name )

    get_db_columns( db_name, cb )
}

var config = function ( config ) {
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

//CONFIGURATION PRIVATE METHODS

var get_db_columns = function ( db_name, cb ) {
    query( {
        db_name: db_name,
        sql: "SELECT * FROM INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA = ?;",
        values: [ db_name ]
    }, function ( err, rows, cols ) {
        if ( !err ) {
            for ( var i = 0; i < rows.length; i++ ) {
                var column_name = rows[ i ].COLUMN_NAME
                var table = rows[ i ].TABLE_NAME
                var type = rows[ i ].DATA_TYPE
                var isPK = false;
                if ( rows[ i ].COLUMN_KEY != undefined ) {
                    isPK = ( rows[ i ].COLUMN_KEY == "PRI" )
                }

                //create object for new table in db_data
                if ( db_data[ db_name ][ table ] == undefined ) {
                    db_data[ db_name ][ table ] = {
                        columns: []
                    }
                }

                if ( isPK ) {
                    db_data[ db_name ][ table ].pk = column_name;
                } else {
                    var column = {
                        name: column_name,
                        type: type
                    }
                    db_data[ db_name ][ table ].columns.push( column )
                }
            }
            cb()
        } else {
            console.log( err )
            cb()
        }
    } )
}

//QUERY PUBLIC METHODS

var query = function ( query_obj, cb ) {
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
        pool.query( sql, function ( err, rows, cols ) {
            if ( err ) {
                error( err, sql )
            }
            cb( err, rows, cols )
        } )
    } else {
        cb( null, null, null )
    }
}

var get = function ( query_obj, cb ) {
    query_obj = clean_query_obj( query_obj )
    var id = query_obj.id
    var table = query_obj.table
    var db_name = query_obj.db_name
    var pk = db_data[ db_name ][ table ].pk

    query_obj.sql = "select * from ?? where ?? = ? Limit 1;";
    query_obj.values = [ table, pk, id ]
    query( query_obj, cb );
}

var all = function ( query_obj, cb ) {
    query_obj = clean_query_obj( query_obj )
    query_obj.sql = "select * from ??";
    query_obj.values = [ query_obj.table ]

    query( sql, function ( err, rows, cols ) {
        cb( err, rows, cols )
    } );
}

var find = function ( query_obj, cb ) {
    query_obj = clean_query_obj( query_obj )
    query_obj = buildFindStatement( query_obj )
    query( query_obj, cb );
}

var findOne = function ( query_obj, cb ) {
    query_obj = clean_query_obj( query_obj )
    query_obj = buildFindStatement( query_obj )
    query_obj.sql += " limit 1;"

    query( query_obj, function ( err, rows, cols ) {
        if ( cols == 0 ) {
            cb( err, null, null )
        } else {
            cb( err, rows[ 0 ], cols )
        }
    } );
}

var buildFindStatement = function ( query_obj ) {
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

var create = function ( query_obj, cb ) {
    query_obj = clean_query_obj( query_obj )
    var table = query_obj.table
    var object = query_obj.object
    var db = query_obj.db_name
    object = clean_object_for_insertion( table, object, db )
    query_obj.sql = "Insert into ?? SET ?"
    query_obj.values = [ table, object ]
    query( query_obj, cb );
}

var update = function ( query_obj, cb ) {
    query_obj = clean_query_obj( query_obj )
    var table = query_obj.table
    var object = query_obj.object
    var db = query_obj.db_name
    var pk_column_name = db_data[ db_name ][ table ].pk
    var id = object[ pk_column_name ]
    object = clean_object_for_insertion( table, object, db )
    query_obj.sql = "update ?? set ? where ?? = ? "
    query_obj.values = [ table, object, pk, id ]

    query( query_obj, cb );
}

var remove = function ( query_obj, cb ) {
    query_obj = clean_query_obj( query_obj )
    var id = query_obj.id
    var table = obj.table
    var db_name = query_obj.db_name
    var pk_column_name = db_data[ db_name ][ table ].pk
    query_obj.sql = "delete from ?? where ?? = ?"
    query_obj.values = [ table, pk_column_name, id ]
    query( query_obj, cb );
}

//QUERY PRIVATE METHODS

var clean_object_for_insertion = function ( table_name, dirty_object, db_name ) {
    var table_columns = db_data[ db_name ][ table_name ].columns
    var pk_name = db_data[ db_name ][ table_name ].pk
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

var clean_query_obj = function ( query_obj ) {
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

var error = function ( err, sql ) {
    if ( log_errors ) {
        console.log( "ERROR: " )
        console.log( err )
        console.log( "SQL:  " )
        console.log( sql )
    }
}

//POPULATE PUBLIC METHOD FOLLOWED BY ITS HELPERS -- located here due to amount of private methods

var populate = function ( query_obj, cb ) {
    query_obj = clean_query_obj( query_obj )
    var structure = query_obj.structure
    var db_name = query_obj.db_name
    var extra_sql = query_obj.sql
    var values = query_obj.values

    if ( query_obj.values ) {
        extra_sql = mysql.format( extra_sql, values );
    }

    var sql = "select * from " + structure.table
    sql += build_join_sql( structure )
    sql += extra_sql + " ;"

    var pool = db_data[ db_name ].pool
    if ( log_sql ) {
        console.log( sql )
    }
    pool.query( {
        sql: sql,
        nestTables: true
    }, function ( err, rows, cols ) {
        if ( !err ) {
            if ( rows.length > 0 ) {
                obj = build_object( rows, structure, db_name, null, null )
                cb( err, obj, cols )
            } else {
                cb( err, null, null )
            }
        } else {
            cb( err, null, null )
        }
    } )
}

var build_join_sql = function ( structure ) {
    var sql = ""
    if ( structure.children != undefined ) {
        for ( var i = 0; i < structure.children.length; i++ ) {
            var parent_table = structure.table
            var child_obj = structure.children[ i ]
            var child_table = child_obj.table
            var fk = child_table + "." + child_obj.fk
            sql += " left join " + child_table + " on " + fk + "=" + parent_table + ".id "
            sql += build_join_sql( child_obj ) //recursion
        }
    }
    return sql
}

var build_object = function ( data, structure, db_name, parent_id, parent_fk ) {
    var obj = []
    var table_name = structure.table
    var table_id_column = db_data[ db_name ][ table_name ].pk

    var unique_ids = []
    for ( var i = 0; i < data.length; i++ ) {
        var row = data[ i ]
            //go row by row looking for children
        var fk_val
        var is_child = false;
        if ( structure.fk != undefined ) {
            var fk_column = structure.fk
            fk_val = row[ table_name ][ fk_column ]
            is_child = ( fk_val == parent_id )
        }
        if ( parent_id == null ) {
            is_child = true //top most node of join structure
        }

        var child_id = row[ table_name ][ table_id_column ]

        var notUsedYet = ( unique_ids.indexOf( child_id ) == -1 )

        if ( notUsedYet && is_child ) {
            unique_ids.push( child_id )
            var cur_obj = clean_object_for_insertion( table_name, row[ table_name ], db_name )

            //look for children
            if ( structure.children != undefined ) {
                for ( var j = 0; j < structure.children.length; j++ ) {
                    var child = structure.children[ j ]
                    cur_obj[ child.table ] = build_object( data, child, db_name, child_id ) //recursion
                }
            }
            obj.push( cur_obj )
        }
    }
    return obj
}

//OBJECT MANIPULATION PUBLIC METHODS
var pivot = function ( data, pivot_column ) {
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

var join = function ( parent, children, foreignKey ) {
    var response = parent;
    if ( foreignKey && response && children ) {
        response[ foreignKey ] = [];
        response[ foreignKey ] = children
    }
    return response
}

//EXPORT OF PUBLIC METHODS TO USER
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
    query: query,
    populate: populate
}
