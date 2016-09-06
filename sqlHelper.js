var mysql = require( 'mysql' );

var db_data = {}
var default_db_name = ""
var log_sql = false;
var log_errors = true;


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

var get_db_columns = function ( db_name, cb ) {
    query( {
        db_name: db_name,
        sql: "SELECT * FROM INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA = ?;",
        values: [ db_name ]
    }, function ( err, rows, cols ) {
        if ( !err ) {
            for ( var i = 0; i < rows.length; i++ ) {
                var column = rows[ i ].COLUMN_NAME
                var table = rows[ i ].TABLE_NAME
                var type = rows[ i ].DATA_TYPE
                var isPK = false;
                if ( rows[ i ].COLUMN_KEY != undefined ) {
                    isPK = ( rows[ i ].COLUMN_KEY == "PRI" )
                }

                //create object for new table in db_data
                if ( !db_data[ db_name ][ table ] ) {
                    db_data[ db_name ][ table ] = {
                        params: []
                    }
                }

                if ( isPK ) {
                    db_data[ db_name ][ table ].pk = column;
                } else {
                    var entry = {
                        name: column,
                        type: type
                    }
                    db_data[ db_name ][ table ].params.push( entry )
                }
            }
            cb()
        } else {
            console.log( err )
            cb()
        }
    } )
}

var query = function ( query_obj, cb ) {
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
    query_obj.sql += " limit 1;" //escape last AND , and limti to 1

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


var create = function ( query_obj, cb ) {
    query_obj = clean_query_obj( query_obj )
    query_obj.sql = "Insert into ?? SET ?"
    query_obj.values = [ query_obj.table, query_obj.object ]
    query( query_obj, cb );
}

var update = function ( query_obj, cb ) {
    query_obj = clean_query_obj( query_obj )
    var table = query_obj.table
    var db_name = query_obj.db_name
    var object = query_obj.object
    var pk = db_data[ db_name ][ table ].pk
    var id = object[ pk ]

    query_obj.sql = "update ?? set ? where ?? = ? "
    query_obj.values = [ table, object, pk, id ]

    query( query_obj, cb );
}

var remove = function ( query_obj, cb ) {
    query_obj = clean_query_obj( query_obj )
    var id = query_obj.id
    var table = obj.table
    var db_name = query_obj.db_name
    var pk = db_data[ db_name ][ table ].pk
    var id = query_obj[ pk ]
    query_obj.sql = "delete from ?? where ?? = ?"
    query_obj.values = [ table, pk, id ]
    query( query_obj, cb );
}

var error = function ( err, sql ) {
    if ( log_errors ) {
        console.log( "ERROR: " )
        console.log( err )
        console.log( "SQL:  " )
        console.log( sql )
    }
}

//--Populate and its helpers

var populate = function ( structure, db_name, extra, cb ) {
    var sql = "select "
    sql += build_select_sql( structure, db_name )
    sql += "null from " + structure.table;
    sql += build_join_sql( structure )
    sql += extra + " ;"

    query( {
        sql: sql
    }, function ( err, rows, cols ) {
        if ( rows.length > 0 || err ) {
            obj = build_object( rows, structure, db_name, null, null )
            cb( err, obj, cols )
        } else {
            cb( err, null, null )
        }

    } )
}

var to_sql = function ( table, column ) {
    return ( table + "__" + column )
}

var build_select_sql = function ( structure, db_name ) {
    var sql = ""
    var table = structure.table;
    var columns = db_data[ db_name ][ table ].params;
    var pk = db_data[ db_name ][ table ].pk;
    //add table members to sql select statement
    for ( var i = 0; i < columns.length; i++ ) {
        sql += table + "." + columns[ i ].name + " as " + to_sql( table, columns[ i ].name ) + ", "
    }
    sql += table + "." + pk + " as " + to_sql( table, pk ) + ", "

    if ( structure.children != undefined ) {
        for ( var i = 0; i < structure.children.length; i++ ) {
            var child = structure.children[ i ]
            sql += build_select_sql( child, db_name )
        }
    }

    return sql
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
            sql += build_join_sql( child_obj )
        }
    }
    return sql
}

var build_object = function ( data, structure, db_name, parent_id, parent_fk ) {
    var obj = []
    var table = structure.table //srid
    var table_id_column = db_data[ db_name ][ table ].pk
    table_id_column = to_sql( table, table_id_column ) //srid__id

    var unique_ids = []
    for ( var i = 0; i < data.length; i++ ) {
        //go row by row looking for children
        var fk_val
        var is_child = false;
        if ( structure.fk != undefined ) {
            var fk_column = to_sql( structure.table, structure.fk ) //srid__compound_id_fk
            fk_val = data[ i ][ fk_column ] //1
            is_child = ( fk_val == parent_id )
        }
        if ( parent_id == null ) {
            is_child = true
        }

        var child_id = data[ i ][ table_id_column ] //1

        var notUsedYet = ( unique_ids.indexOf( child_id ) == -1 )

        if ( notUsedYet && is_child ) {
            unique_ids.push( child_id )
            var cur_obj = cleanObject( data[ i ], table, db_name )
                //look for children
            if ( structure.children != undefined ) {
                for ( var j = 0; j < structure.children.length; j++ ) {
                    var child = structure.children[ j ]
                    cur_obj[ child.table ] = build_object( data, child, db_name, child_id )
                }
            }
            obj.push( cur_obj )
        }
    }
    return obj
}

var cleanObject = function ( data, table, db_name ) {
    var members = db_data[ db_name ][ table ].params
    var id = db_data[ db_name ][ table ].pk
    var obj = {};
    for ( var i = 0; i < members.length; i++ ) {
        var member = to_sql( table, members[ i ].name )
        if ( data[ member ] ) {
            if ( members[ i ].type == "date" ) {
                obj[ members[ i ].name ] = new Date( data[ member ] ).toISOString().substring( 0, 10 );
            } else {
                obj[ members[ i ].name ] = data[ member ]
            }
        }
    }
    obj[ id ] = data[ to_sql( table, id ) ]
    return obj
}

//------------------------------------------------------------------------------

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
