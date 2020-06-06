﻿using System;
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

        public ExecuteUpdateStatement( MemoryDbCommand command )
        {
            _Command = command;
        }

        public void Execute( Dictionary<string, Table> tables, SqlUpdateStatement updateStatement )
        {
            var rawData = new RawData{ Parameters = _Command.Parameters };

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
                            updateTable.Row[ tableColumn.Column.Order - 1 ] = value;
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