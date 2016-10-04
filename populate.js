var populate = function( query_obj, cb ) {
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
    }, function( err, rows, cols ) {
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

var build_join_sql = function( structure ) {
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

var build_object = function( data, structure, db_name, parent_id, parent_fk ) {
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

// //OBJECT MANIPULATION PUBLIC METHODS
// var pivot = function( data, pivot_column ) {
//     var response = {}
//     for ( var i = 0; i < data.length; i++ ) {
//         var pivot_value = data[ i ][ pivot_column ]
//         var row_data = data[ i ]
//         if ( response[ pivot_value ] ) {
//             response[ pivot_value ].push( row_data )
//         } else {
//             response[ pivot_value ] = [ row_data ]
//         }
//     }
//     return response
// }
//
// var join = function( parent, children, foreignKey ) {
//     var response = parent;
//     if ( foreignKey && response && children ) {
//         response[ foreignKey ] = [];
//         response[ foreignKey ] = children
//     }
//     return response
// }
