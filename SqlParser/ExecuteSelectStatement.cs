﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Helpers;
using SqlMemoryDb.SelectData;
using SqlParser;

namespace SqlMemoryDb
{
    class ExecuteSelectStatement
    {
        internal class RawData
        {
            public class RawDataRow
            {
                public string Name;
                public Table Table;
                public ArrayList Row;
            }
            public List<List<RawDataRow>> TableRows = new List<List<RawDataRow>>();
            public List<ISelectData> SelectFieldData = new List<ISelectData>();
            public DbParameterCollection Parameters { get; set; }
            public Dictionary<string,Table> TableAliasList = new Dictionary<string, Table>();
        }

        private readonly MemoryDbCommand _Command;
        private readonly MemoryDbDataReader _Reader;


        public ExecuteSelectStatement( MemoryDbCommand command, MemoryDbDataReader reader )
        {
            _Command = command;
            _Reader = reader;
        }

        public void Execute( Dictionary<string, Table> tables, SqlSelectStatement selectStatement )
        {
            var rawData = new RawData{ Parameters = _Command.Parameters };

            var expression = (SqlQuerySpecification)selectStatement.SelectSpecification.QueryExpression;
            if (expression.FromClause != null )
            {
                AddTablesFromClause( expression.FromClause, tables, rawData );
                if ( expression.WhereClause != null )
                {
                    ExecuteWhereClause( rawData, expression.WhereClause );
                }
            }

            var batch = new MemoryDbDataReader.ResultBatch(  );
            InitializeFields( batch, expression.SelectClause.Children.ToList(  ), rawData );
            AddDataToBatch( batch, rawData );
            _Reader.AddResultBatch( batch );
        }


        private void InitializeFields( MemoryDbDataReader.ResultBatch batch, List<SqlCodeObject> columns, RawData rawData )
        {
            foreach ( var column in columns )
            {
                if ( column is SqlSelectScalarExpression scalarExpression )
                {
                    var name = Helper.GetColumnAlias( scalarExpression );
                    switch ( scalarExpression.Expression )
                    {
                        case SqlColumnRefExpression columnRef: AddFieldFromColumn( columnRef, name, batch, rawData ); break;
                        case SqlLiteralExpression literalExpression: AddFieldFromLiteral( literalExpression, name, batch, rawData ); break;
                    }
                }
                else if ( column is SqlTopSpecification topSpecification )
                {
                    batch.MaxRowsCount = int.Parse(topSpecification.Value.Sql);
                }
                else
                {
                    throw new NotImplementedException();
                }
            }

        }

        private void AddFieldFromColumn( SqlColumnRefExpression columnRef, string name,
                                        MemoryDbDataReader.ResultBatch batch, RawData rawData )
        {
            var tableColumn = Helper.GetTableColumn( columnRef, rawData );
            var readerField = new MemoryDbDataReader.ReaderField
            {
                Name = name,
                DbType = tableColumn.Column.DbDataType.ToString(),
                NetType = tableColumn.Column.NetDataType,
                FieldIndex = batch.Fields.Count
            };
            batch.Fields.Add( readerField );
            rawData.SelectFieldData.Add( new SelectDataFromColumn( tableColumn ) );
        }

        private void AddFieldFromLiteral( SqlLiteralExpression literalExpression, string name, MemoryDbDataReader.ResultBatch batch, RawData rawData )
        {
            var readerField = Helper.BuildFieldFromStringValue( literalExpression.Value, name, batch.Fields.Count );
            var value = Helper.GetValueFromString( readerField.NetType, literalExpression.Value );
            batch.Fields.Add( readerField );
            rawData.SelectFieldData.Add( new SelectDataFromObject( value ) );
        }


        private void AddTablesFromClause(SqlFromClause fromClause, Dictionary<string, Table> tables, RawData rawData )
        {
            foreach (var expression in fromClause.TableExpressions)
            {
                switch (expression)
                {
                    case SqlTableRefExpression tableRef:
                    {
                        var name = Helper.GetAliasName(tableRef);
                        var table = tables[Helper.GetQualifiedName(tableRef.ObjectIdentifier)];
                        AddAllTableRows( rawData, table, name );
                        break;
                    }
                    case SqlQualifiedJoinTableExpression joinExpression:
                    {
                        var name = Helper.GetAliasName((SqlTableRefExpression)joinExpression.Left);
                        var table = tables[Helper.GetQualifiedName(((SqlTableRefExpression)joinExpression.Left).ObjectIdentifier)];
                        AddAllTableRows( rawData, table, name );
                        var nameJoin = Helper.GetAliasName((SqlTableRefExpression)joinExpression.Right);
                        var tableJoin = tables[Helper.GetQualifiedName(((SqlTableRefExpression)joinExpression.Right).ObjectIdentifier)];
                        AddAllTableJoinRows( rawData, tableJoin, nameJoin, joinExpression.OnClause );
                        break;
                    }
                }
            }
        }

        private static void AddAllTableRows( RawData rawData, Table table, string name )
        {
            if ( rawData.TableAliasList.ContainsKey( name ) == false )
            {
                rawData.TableAliasList.Add( name, table );
            }
            foreach ( var row in table.Rows )
            {
                var tableRow = new RawData.RawDataRow
                {
                    Name = name,
                    Table = table,
                    Row = row
                };
                var rows = new List<RawData.RawDataRow>( ) {tableRow};
                rawData.TableRows.Add( rows );
            }
        }

        private void AddAllTableJoinRows( RawData rawData, Table table, string name, SqlConditionClause onClause )
        {
            if ( rawData.TableAliasList.ContainsKey( name ) == false )
            {
                rawData.TableAliasList.Add( name, table );
            }

            var newTableRows = new List<List<RawData.RawDataRow>>( );
            var filter = Helper.GetRowFilter( onClause.Expression, rawData );
            foreach ( var currentRawRows in rawData.TableRows )
            {
                foreach ( var row in table.Rows )
                {
                    var newRows = new List<RawData.RawDataRow>( currentRawRows );
                    var tableRow = new RawData.RawDataRow
                    {
                        Name = name,
                        Table = table,
                        Row = row
                    };
                    newRows.Add( tableRow );
                    if ( filter.IsValid( newRows ) )
                    {
                        newTableRows.Add( newRows );
                    }
                }            
            }

            rawData.TableRows = newTableRows;
        }


        private void AddDataToBatch( MemoryDbDataReader.ResultBatch batch, RawData rawData )
        {
            foreach ( var row in rawData.TableRows )
            {
                var resultRow = new ArrayList();
                foreach ( var selectData in rawData.SelectFieldData )
                {
                    var value = selectData.Select( row );
                    resultRow.Add( value );
                }
                batch.ResultRows.Add( resultRow );
            }

            if ( batch.MaxRowsCount.HasValue )
            {
                batch.ResultRows = batch.ResultRows.Take( batch.MaxRowsCount.Value ).ToList(  );
            }
        }

        private void ExecuteWhereClause( RawData rawData, SqlWhereClause whereClause )
        {
            foreach ( var child in whereClause.Children )
            {
                var filter = Helper.GetRowFilter( ( SqlBooleanExpression ) child, rawData );
                rawData.TableRows = rawData.TableRows.Where( r => filter.IsValid( r )  ).ToList(  );
            }
        }
    }
}
