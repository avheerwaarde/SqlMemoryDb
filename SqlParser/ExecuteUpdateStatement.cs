using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Exceptions;
using SqlMemoryDb.Helpers;
using SqlParser;

namespace SqlMemoryDb
{
    public class ExecuteUpdateStatement
    {
        private readonly MemoryDbCommand _Command;
        private readonly MemoryDatabase _Database;

        private class UpdatedRow
        {
            public ArrayList Row;
            public List<Column> Columns;
        }

        public ExecuteUpdateStatement( MemoryDatabase memoryDatabase, MemoryDbCommand command )
        {
            _Database = memoryDatabase;
            _Command = command;
        }

        public void Execute( Dictionary<string, Table> tables, SqlUpdateStatement updateStatement )
        {
            var rawData = new RawData( _Command );

            var specification = updateStatement.UpdateSpecification;
            rawData.AddTable( specification.Target, tables );
            if ( specification.WhereClause != null )
            {
                rawData.ExecuteWhereClause( specification.WhereClause );
            }

            var updatedRows = new List<UpdatedRow>( );
            foreach ( var assignment in specification.SetClause.Assignments )
            {
                switch ( assignment )
                {
                    case SqlColumnAssignment columnAssignment:
                        var tableColumn = Helper.GetTableColumn( (SqlObjectIdentifier)(columnAssignment.Column.MultipartIdentifier), rawData );
                        if ( tableColumn.Column.IsIdentity && tableColumn.Column.ParentTable.IsIdentityInsertForbidden || tableColumn.Column.IsRowVersion )
                        {
                            throw new SqlUpdateColumnForbiddenException( tableColumn.Column.Name );
                        }

                        foreach ( var row in rawData.RawRowList )
                        {
                            var value = Helper.GetValue(columnAssignment.Value, tableColumn.Column.NetDataType, rawData, row );
                            var updateTable = row.Single( r => r.Name == tableColumn.TableName );
                            updateTable.Row[ tableColumn.Column.Order ] = value;
                            if ( updatedRows.Any( u => u.Row == updateTable.Row ) == false )
                            {
                                var updateRow = new UpdatedRow{ Columns =  updateTable.Table.Columns, Row = updateTable.Row };
                                updatedRows.Add( updateRow );
                            }
                        }
                        break;
                    default:
                        throw new NotImplementedException();
                }
            }

            UpdateRowVersions( updatedRows );
            _Command.RowsAffected = rawData.RawRowList.Count;
        }

        private void UpdateRowVersions( List<UpdatedRow> updatedRows )
        {
            foreach ( var row in updatedRows )
            {
                foreach ( var column in row.Columns.Where( c => c.IsRowVersion ) )
                {
                    row.Row[ column.Order ] = _Database.NextRowVersion( );
                }
            }
        }
    }
}