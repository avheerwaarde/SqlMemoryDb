using System.Linq;
using System.Threading.Tasks;
using Dapper;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlMemoryDb;

namespace DatabaseTests
{
    [TestClass]
    public class CreateTableTests
    {
        [TestMethod]
        public async Task OpenConnection_CreateTable_Ok( )
        {
            await using var connection = new MemoryDbConnection( );
            connection.GetMemoryDatabase( ).Clear(  );
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
            schemaColumns.Rows.Count.Should( ).Be( 4 );
            schemaColumns.Rows[ 0 ][ "TABLE_CATALOG" ].Should( ).Be( "Memory" );
            schemaColumns.Rows[ 0 ][ "TABLE_SCHEMA" ].Should( ).Be( "dbo" );
            schemaColumns.Rows[ 0 ][ "TABLE_NAME" ].Should( ).Be( "application" );
            schemaColumns.Rows[ 0 ][ "COLUMN_NAME" ].Should( ).Be( "Id" );
            schemaColumns.Rows[ 1 ][ "TABLE_CATALOG" ].Should( ).Be( "Memory" );
            schemaColumns.Rows[ 1 ][ "TABLE_SCHEMA" ].Should( ).Be( "dbo" );
            schemaColumns.Rows[ 1 ][ "TABLE_NAME" ].Should( ).Be( "application" );
            schemaColumns.Rows[ 1 ][ "COLUMN_NAME" ].Should( ).Be( "Name" );
            schemaColumns.Rows[ 2 ][ "TABLE_CATALOG" ].Should( ).Be( "Memory" );
            schemaColumns.Rows[ 2 ][ "TABLE_SCHEMA" ].Should( ).Be( "dbo" );
            schemaColumns.Rows[ 2 ][ "TABLE_NAME" ].Should( ).Be( "application" );
            schemaColumns.Rows[ 2 ][ "COLUMN_NAME" ].Should( ).Be( "User" );
            schemaColumns.Rows[ 3 ][ "TABLE_CATALOG" ].Should( ).Be( "Memory" );
            schemaColumns.Rows[ 3 ][ "TABLE_SCHEMA" ].Should( ).Be( "dbo" );
            schemaColumns.Rows[ 3 ][ "TABLE_NAME" ].Should( ).Be( "application" );
            schemaColumns.Rows[ 3 ][ "COLUMN_NAME" ].Should( ).Be( "DefName" );
        }

        [TestMethod]
        public async Task CreateJoinedTable_Constraint_JoinCreated( )
        {
            await using var connection = new MemoryDbConnection( );
            var db = connection.GetMemoryDatabase( );
            db.Tables.Clear( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = SqlStatements.SqlCreateTableApplication + "\n" + SqlStatements.SqlCreateTableApplicationAction;
            await command.PrepareAsync( );
            await command.ExecuteNonQueryAsync( );
            var fk = db.Tables[ "dbo.application_action" ].ForeignKeyConstraints.Single( );
            fk.ReferencedTableName.Should( ).Be( "dbo.application" );
        }

        [TestMethod]
        public async Task CreateJoinedTable_AlterTable_JoinCreated( )
        {
            await using var connection = new MemoryDbConnection( );
            var db = connection.GetMemoryDatabase( );
            db.Tables.Clear( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = SqlStatements.SqlCreateTableApplication + "\n" + SqlStatements.SqlCreateTableApplicationAction2;
            await command.PrepareAsync( );
            await command.ExecuteNonQueryAsync( );
            var fk = db.Tables[ "dbo.application_action" ].ForeignKeyConstraints.Single( );
            fk.ReferencedTableName.Should( ).Be( "dbo.application" );
        }

        [TestMethod]
        public void CreateTable_MultiColumnPrimaryKey_IsCreated( )
        {
            using var connection = new MemoryDbConnection( );
            var db = connection.GetMemoryDatabase( );
            db.Tables.Clear( );
            connection.Execute( SqlStatements.SqlCreateCustomerCustomerDemo );
            var table = db.Tables[ "dbo.CustomerCustomerDemo" ];
            table.PrimaryKeyConstraints.Should( ).ContainKey( "PK_CustomerCustomerDemo" );
            table.PrimaryKeyConstraints[ "PK_CustomerCustomerDemo" ].Count.Should( ).Be( 2 );
            table.PrimaryKeys.Count.Should( ).Be( 2 );
            foreach ( var column in table.Columns )
            {
                column.IsPrimaryKey.Should( ).BeTrue( );
            }
        }

    }
}
