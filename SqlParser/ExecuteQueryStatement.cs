using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Exceptions;
using SqlMemoryDb.Helpers;
using SqlMemoryDb.SelectData;
using SqlParser;

namespace SqlMemoryDb
{
    class ExecuteQueryStatement
    {

        private readonly MemoryDbCommand _Command;
        private readonly MemoryDbDataReader _Reader;
        private readonly MemoryDatabase _Database;


        public ExecuteQueryStatement( MemoryDatabase memoryDatabase, MemoryDbCommand command,
            MemoryDbDataReader reader )
        {
            _Database = memoryDatabase;
            _Command = command;
            _Reader = reader;
        }

        public void Execute( Dictionary<string, Table> tables, SqlSelectStatement selectStatement )
        {
            var rawData = new RawData( _Command );

            var sqlQuery = (SqlQuerySpecification)selectStatement.SelectSpecification.QueryExpression;
            Execute( tables, rawData, sqlQuery, selectStatement.SelectSpecification.OrderByClause );
        }

        public void Execute( Dictionary<string, Table> tables, RawData rawData, SqlQuerySpecification sqlQuery, SqlOrderByClause orderByClause = null )
        {
            if (sqlQuery.FromClause != null )
            {
                rawData.AddTablesFromClause( sqlQuery.FromClause, tables );
                if ( sqlQuery.WhereClause != null )
                {
                    rawData.ExecuteWhereClause( sqlQuery.WhereClause );
                }
            }
            else
            {
                // We do not select data from any table, so we insert an empty row to trigger a result in AddData().
                rawData.RawRowList.Add( new List<RawData.RawDataRow>() );
            }

            var batch = new MemoryDbDataReader.ResultBatch(  );
            InitializeFields( batch, sqlQuery.SelectClause.Children.ToList(  ), rawData );
            if ( sqlQuery.GroupByClause != null )
            {
                rawData.AddGroupByClause( sqlQuery.GroupByClause );
            }
            
            rawData.HavingClause = sqlQuery.HavingClause?.Expression;
            rawData.SortOrder =  GetSortOrder( orderByClause, sqlQuery );

            new QueryResultBuilder( rawData ).AddData( batch );            
            _Reader.AddResultBatch( batch );
        }

        private static SqlOrderByItemCollection GetSortOrder( SqlOrderByClause orderByClause,
                                                                SqlQuerySpecification sqlQuery )
        {
            if ( sqlQuery.OrderByClause != null && orderByClause == null )
            {
                orderByClause = sqlQuery.OrderByClause;
                if ( IsPartialQuery( sqlQuery.Parent ) && IsMissingTopOffsetOrForXml( sqlQuery ) )
                {
                    throw new SqlOrderByException( );
                }
            }
            return orderByClause?.Items;
        }

        private static bool IsPartialQuery( SqlCodeObject sqlQuery )
        {
            return sqlQuery is SqlViewDefinition
                   || sqlQuery is SqlCommonTableExpression
                   || sqlQuery is SqlScalarSubQueryExpression;
        }

        private static bool IsMissingTopOffsetOrForXml( SqlQuerySpecification sqlQuery )
        {
            return sqlQuery.Children.Any( c => c is SqlTopSpecification ) == false
                   && sqlQuery.Children.Any( c => c is SqlForXmlClause ) == false
                   && sqlQuery.Children.Any( c => c is SqlOffsetFetchClause ) == false;
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
                        case SqlGlobalScalarVariableRefExpression globalRef:
                            AddFieldFromGlobalVariable( globalRef, name, batch, rawData );
                            break;
                        case SqlScalarVariableRefExpression variableRef:
                            AddFieldFromVariable( variableRef, name, batch, rawData );
                            break;
                        case SqlScalarRefExpression scalarRef:
                            AddFieldFromColumn( ( SqlObjectIdentifier ) scalarRef.MultipartIdentifier, name, batch,
                                rawData );
                            break;
                        case SqlLiteralExpression literalExpression:
                            AddFieldFromLiteral( literalExpression, name, batch, rawData );
                            break;
                        case SqlBuiltinScalarFunctionCallExpression functionCall:
                            AddFieldForFunctionCall( functionCall, name, batch, rawData );
                            break;
                        case SqlSearchedCaseExpression caseExpression:
                            AddFieldFromCaseExpression( caseExpression, name, batch, rawData );
                            break;
                    }
                }
                else if ( column is SqlTopSpecification topSpecification )
                {
                    batch.MaxRowsCount = int.Parse( topSpecification.Value.Sql );
                }
                else if ( column is SqlSelectStarExpression selectStarExpression )
                {
                    foreach ( var tableAlias in rawData.TableAliasList )
                    {
                        foreach ( var valueColumn in tableAlias.Value.Columns )
                        {
                            AddFieldFromColumn( valueColumn, tableAlias.Key, batch, rawData );
                        }
                    }
                }
                else
                {
                    throw new NotImplementedException( $"Not implemented column specification {column}" );
                }
            }
        }

        private void AddFieldFromGlobalVariable( SqlGlobalScalarVariableRefExpression globalRef, string name, MemoryDbDataReader.ResultBatch batch, RawData rawData )
        {
            var select = new SelectDataBuilder(  ).BuildGlobalVariable( globalRef.VariableName, rawData );
            AddFieldFromSelectData( name, batch, select );
        }

        private void AddFieldFromVariable( SqlScalarVariableRefExpression variableRef, string name, MemoryDbDataReader.ResultBatch batch, RawData rawData )
        {
            var select = new SelectDataFromVariables( variableRef, _Command );
            AddFieldFromSelectData( name, batch, select );
        }

        private void AddFieldFromColumn( SqlObjectIdentifier objectIdentifier, string name, MemoryDbDataReader.ResultBatch batch, RawData rawData )
        {
            var tableColumn = Helper.GetTableColumn( objectIdentifier, rawData );
            var readerField = new MemoryDbDataReader.ReaderFieldData
            {
                Name = name,
                DbType = tableColumn.Column.DbDataType.ToString(),
                NetType = tableColumn.Column.NetDataType,
                FieldIndex = batch.Fields.Count,
                SelectFieldData = new SelectDataFromColumn( tableColumn )
            };
            batch.Fields.Add( readerField );
        }


        private void AddFieldFromColumn( Column column, string tableName, MemoryDbDataReader.ResultBatch batch, RawData rawData )
        {
            var readerField = new MemoryDbDataReader.ReaderFieldData
            {
                Name = column.Name,
                DbType = column.DbDataType.ToString(),
                NetType = column.NetDataType,
                FieldIndex = batch.Fields.Count,
                SelectFieldData = new SelectDataFromColumn( new TableColumn{ Column = column, TableName = tableName } )
            };
            batch.Fields.Add( readerField );
        }


        private void AddFieldFromLiteral( SqlLiteralExpression literalExpression, string name, MemoryDbDataReader.ResultBatch batch, RawData rawData )
        {
            var readerField = Helper.BuildFieldFromStringValue( literalExpression.Value, name, batch.Fields.Count );
            var value = Helper.GetValueFromString( readerField.NetType, literalExpression.Value );
            readerField.SelectFieldData = new SelectDataFromObject( value );
            batch.Fields.Add( readerField );
        }

        private void AddFieldForFunctionCall( SqlBuiltinScalarFunctionCallExpression functionCall, string name, MemoryDbDataReader.ResultBatch batch, RawData rawData )
        {
            var select = new SelectDataBuilder(  ).Build( functionCall, rawData );
            AddFieldFromSelectData( name, batch, select );
        }

        private void AddFieldFromCaseExpression( SqlSearchedCaseExpression caseExpression, string name, MemoryDbDataReader.ResultBatch batch, RawData rawData )
        {
            var select = new SelectDataFromCaseExpression( caseExpression, rawData );
            AddFieldFromSelectData( name, batch, select );
        }


        private static void AddFieldFromSelectData( string name, MemoryDbDataReader.ResultBatch batch, ISelectDataFunction select )
        {
            var readerField = new MemoryDbDataReader.ReaderFieldData
            {
                Name = name,
                DbType = select.DbType,
                NetType = select.ReturnType,
                FieldIndex = batch.Fields.Count,
                SelectFieldData = select
            };
            batch.Fields.Add( readerField );
        }
    }
}
