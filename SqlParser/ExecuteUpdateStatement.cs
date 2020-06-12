using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Helpers;
using SqlParser;

namespace SqlMemoryDb
{
    public class ExecuteUpdateStatement
    {
        private readonly MemoryDbCommand _Command;
        private readonly MemoryDatabase _Database;

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

            foreach ( var assignment in specification.SetClause.Assignments )
            {
                switch ( assignment )
                {
                    case SqlColumnAssignment columnAssignment:
                        var tableColumn = Helper.GetTableColumn( (SqlObjectIdentifier)(columnAssignment.Column.MultipartIdentifier), rawData );
                        var value = Helper.GetValue( columnAssignment.Value, tableColumn.Column.NetDataType, rawData, new List<RawData.RawDataRow>( ) );
                        foreach ( var row in rawData.RawRowList )
                        {
                            var updateTable = row.Single( r => r.Name == tableColumn.TableName );
                            updateTable.Row[ tableColumn.Column.Order ] = value;
                        }
                        break;
                    default:
                        throw new NotImplementedException();
                }
            }

            _Command.RowsAffected = rawData.RawRowList.Count;
        }
    }
}