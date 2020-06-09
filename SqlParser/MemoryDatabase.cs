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
                    switch ( child )
                    {
                        case SqlCreateTableStatement createTable: new TableInfo( this ).Add( createTable ); break;
                        case SqlInsertStatement insertStatement : new ExecuteNonQueryStatement( command ).Execute( Tables, insertStatement ); break; 
                        case SqlUpdateStatement updateStatement : new ExecuteUpdateStatement( command ).Execute( Tables, updateStatement ); break; 
                        case SqlNullStatement nullStatement     : new ExecuteNullStatement( command ).Execute( Tables, nullStatement ); break;
                    }
                }
            }

            return 1;
        }

        public DbDataReader ExecuteSqlReader( string commandText, MemoryDbCommand command, CommandBehavior behavior )
        {
            var result = Parser.Parse( commandText );
            if ( result.Errors.Any())
            {
                throw new SqlServerParserException( result.Errors );
            }

            using ( var reader = new MemoryDbDataReader( behavior ) )
            {
                foreach ( var batch in result.Script.Batches )
                {
                    command.LastIdentitySet = null;
                    foreach ( var child in batch.Children )
                    {
                        switch ( child )
                        {
                            case SqlSelectStatement selectStatement: new ExecuteQueryStatement( command, reader ).Execute( Tables, selectStatement ); break; 
                        }
                    }
                }

                return reader;
            }
        }

        public object ExecuteSqlScalar( string commandText, MemoryDbCommand command )
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
                    switch ( child )
                    {
                        case SqlSelectStatement selectStatement:
                            using ( var reader = new MemoryDbDataReader( CommandBehavior.SingleResult ) )
                            {
                                new ExecuteQueryStatement( command, reader ).Execute( Tables, selectStatement );
                                if ( reader.IsScalarResult )
                                {
                                    reader.Read( );
                                    return reader.GetValue( 0 );
                                }
                                else
                                {
                                    throw new SqlNoScalarResultException( );
                                }
                            }

                        default:
                            throw new NotImplementedException();
                    }
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
