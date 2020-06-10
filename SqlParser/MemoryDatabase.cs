using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using Force.DeepCloner;
using Microsoft.SqlServer.Management.SqlParser.Parser;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Exceptions;
using SqlMemoryDb.Helpers;
using SqlParser;

namespace SqlMemoryDb
{
    public class MemoryDatabase
    {
        public Dictionary<string, Table> Tables = new Dictionary<string, Table>();
        public Decimal? LastIdentitySet;
        public Stack<Dictionary<string, Table>> TablesStack = new Stack<Dictionary<string, Table>>();
        public Stack<Dictionary<string, Table>> TablesTransactionStack = new Stack<Dictionary<string, Table>>();

        public int ExecuteSqlStatement( string commandText, MemoryDbCommand command )
        {
            var result = Parser.Parse( commandText );
            if ( result.Errors.Any())
            {
                throw new SqlServerParserException( result.Errors );
            }

            foreach ( var batch in result.Script.Batches )
            {
                command.LastIdentitySet = null;
                foreach ( var child in batch.Children )
                {
                    ExecuteStatement( command, child );
                }
            }

            return 1;
        }

        public void ExecuteStatement( MemoryDbCommand command, SqlCodeObject child )
        {
            switch ( child )
            {
                case SqlCreateTableStatement createTable:
                    new TableInfo( this ).Add( createTable );
                    break;
                case SqlInsertStatement insertStatement:
                    new ExecuteNonQueryStatement( this, command ).Execute( Tables, insertStatement );
                    break;
                case SqlUpdateStatement updateStatement:
                    new ExecuteUpdateStatement( this, command ).Execute( Tables, updateStatement );
                    break;
                case SqlNullStatement nullStatement:
                    new ExecuteNullStatement( this, command ).Execute( Tables, nullStatement );
                    break;
                case SqlIfElseStatement ifElseStatement:
                    new ExecuteNonQueryStatement( this, command ).Execute( Tables, ifElseStatement );
                    break;
                case SqlSelectStatement selectStatement:
                    new ExecuteQueryStatement( this, command, command.DataReader ).Execute( Tables, selectStatement );
                    break;
                case SqlCompoundStatement compoundStatement:
                    foreach ( var compoundChild in compoundStatement.Children )
                    {
                        ExecuteStatement( command, compoundChild );
                    }
                    break;
            }
        }

        public DbDataReader ExecuteSqlReader( string commandText, MemoryDbCommand command, CommandBehavior behavior )
        {
            var result = Parser.Parse( commandText );
            if ( result.Errors.Any())
            {
                throw new SqlServerParserException( result.Errors );
            }

            command.DataReader = new MemoryDbDataReader( behavior );
            foreach ( var batch in result.Script.Batches )
            {
                command.LastIdentitySet = null;
                foreach ( var child in batch.Children )
                {
                    ExecuteStatement( command, child );
                }
            }
            return command.DataReader;
        }

        public object ExecuteSqlScalar( string commandText, MemoryDbCommand command )
        {
            var result = Parser.Parse( commandText );
            if ( result.Errors.Any())
            {
                throw new SqlServerParserException( result.Errors );
            }


            command.DataReader = new MemoryDbDataReader( CommandBehavior.SingleResult );
            foreach ( var batch in result.Script.Batches )
            {
                command.LastIdentitySet = null;
                foreach ( var child in batch.Children )
                {
                    ExecuteStatement( command, child );
                    if ( command.DataReader.IsScalarResult )
                    {
                        command.DataReader.Read( );
                        var value = command.DataReader.GetValue( 0 );
                        command.DataReader.Dispose(  );
                        command.DataReader = null;
                        return value;
                    }
                    throw new SqlNoScalarResultException( );
                }
            }

            return null;
        }

        public void SaveSnapshot( )
        {
            var savedTables = Tables.DeepClone( );
            TablesStack.Push( savedTables );
        }

        public void RemoveSnapshot( )
        {
            TablesStack.Pop( );
        }

        public void RestoreSnapshot( )
        {
            Tables = TablesStack.Pop( );
        }

        public void SaveSnapshotForTransaction( )
        {
            var savedTables = Tables.DeepClone( );
            TablesTransactionStack.Push( savedTables );
        }

        public void RestoreSnapshotForTransaction( )
        {
            Tables = TablesTransactionStack.Pop( );
        }

        public void RemoveSnapshotForTransaction( )
        {
            TablesTransactionStack.Pop( );
        }
    }
}
