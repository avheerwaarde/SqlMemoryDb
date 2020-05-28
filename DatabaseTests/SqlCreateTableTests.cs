using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlMemoryDb;

namespace DatabaseTests
{
    [TestClass]
    public class SqlCreateTableTests
    {
        [TestMethod]
        public async Task OpenConnection_CreateTable_Ok( )
        {
            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = SqlStatements.SqlCreateTableApplication;
            await command.PrepareAsync( );
            await command.ExecuteNonQueryAsync( );

            var schema = connection.GetSchema( "Tables" );
            schema.Rows.Count.Should( ).Be( 1 );
            schema.Rows[ 0 ][ "TABLE_CATALOG" ].Should( ).Be( "Memory" );
            schema.Rows[ 0 ][ "TABLE_SCHEMA" ].Should( ).Be( "dbo" );
            schema.Rows[ 0 ][ "TABLE_NAME" ].Should( ).Be( "application" );
            schema.Rows[ 0 ][ "TABLE_TYPE" ].Should( ).Be( "BASE TABLE" );

            var schemaColumns = connection.GetSchema( "Columns" );
            schemaColumns.Rows.Count.Should( ).Be( 2 );
            schemaColumns.Rows[ 0 ][ "TABLE_CATALOG" ].Should( ).Be( "Memory" );
            schemaColumns.Rows[ 0 ][ "TABLE_SCHEMA" ].Should( ).Be( "dbo" );
            schemaColumns.Rows[ 0 ][ "TABLE_NAME" ].Should( ).Be( "application" );
            schemaColumns.Rows[ 0 ][ "COLUMN_NAME" ].Should( ).Be( "Id" );
            schemaColumns.Rows[ 1 ][ "TABLE_CATALOG" ].Should( ).Be( "Memory" );
            schemaColumns.Rows[ 1 ][ "TABLE_SCHEMA" ].Should( ).Be( "dbo" );
            schemaColumns.Rows[ 1 ][ "TABLE_NAME" ].Should( ).Be( "application" );
            schemaColumns.Rows[ 1 ][ "COLUMN_NAME" ].Should( ).Be( "Name" );
        }
    }
}
