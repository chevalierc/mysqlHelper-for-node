var chai = require( 'chai' );
var assert = chai.assert;
var expect = chai.expect;

var sqlHelper = require( '../sqlHelper.js' )

var db_config = {
    host: "localhost",
    user: "root",
    password: 'password',
    database: "mysqlhelper-tests",
    connectionLimit: 20,
}

describe( 'Connecting to the database', function() {

    it( 'Should connect to the database', function( done ) {
        sqlHelper.connect( db_config, function( db_data ) {

            expect( db_data[ db_config.database ] ).to.not.be.undefined;
            expect( db_data[ db_config.database ].pool ).to.not.be.undefined;

            done()
        } );
    } )

    it( 'Should parse the Database for column names correctly', function( done ) {
        sqlHelper.connect( db_config, function( db_data ) {
            var table_info = db_data[ db_config.database ].tables

            expect( table_info ).to.not.be.undefined;
            expect( table_info[ "test-table" ] ).to.not.be.undefined;

            done()
        } );
    } )

} )

describe( 'Be able to so basic querying', function() {

    it( 'Should be able to execute a basic query', function( done ) {
        sqlHelper.connect( db_config, function() {
            sqlHelper.query( {
                sql: "SELECT '42' as foo"
            }, function( err, rows, cols ) {
                expect( rows ).to.not.be.undefined;
                expect( rows[0]['foo'] ).to.not.be.undefined;
                expect( rows[0]['foo'] ).to.be.equal( "42" );
                done()
            } )
        } )
    } )

    it( 'Should be able to execute a basic query with a user value (one value)', function( done ) {
        sqlHelper.connect( db_config, function() {
            sqlHelper.query( {
                sql: "SELECT ? as foo",
                values: "42"
            }, function( err, rows, cols ) {

                expect( rows ).to.not.be.undefined
                expect( rows[0]['foo'] ).to.be.equal( "42" )
                done()
            } )
        } )
    } )

    it( 'Should be able to execute a basic query with a user values (more than one value)', function( done ) {
        sqlHelper.connect( db_config, function() {
            sqlHelper.query( {
                sql: "SELECT ? as ?",
                values: [ "42", "foo" ]
            }, function( err, rows, cols ) {
                expect( rows ).to.not.be.undefined
                expect( rows[0].foo ).to.be.equal( "42" )
                done()
            } )
        } )
    } )

    it( 'Should be able to execute a basic query with a user table value', function( done ) {
        sqlHelper.connect( db_config, function() {
            sqlHelper.query( {
                sql: "SELECT * from ??",
                values: [ "test-table" ]
            }, function( err, rows, cols ) {
                expect( err ).to.be.null
                expect( rows ).to.not.be.undefined
                done()
            } )
        } )
    } )

} )

describe( 'Be able to so ORM style querying', function() {

    var foobar = {
        foo: "42",
        bar: "42"
    }

    it( 'Should be able to execute create()', function( done ) {
        sqlHelper.connect( db_config, function() {
            sqlHelper.create( {
                table: "test-table",
                object: foobar
            }, function( err, rows, cols ) {
                expect( err ).to.be.undefined;
                done()
            } )
        } )
    } )

    it( 'Should be able to execute all()', function( done ) {
        sqlHelper.connect( db_config, function() {
            sqlHelper.all( {
                table: "test-table"
            }, function( err, rows, cols ) {
                expect( err ).to.not.be.undefined;
                expect( rows[0] ).to.not.be.undefined;
                done()
            } )
        } )
    } )

} )
