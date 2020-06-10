using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
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
            var rawData = new RawData{ Parameters = _Command.Parameters };

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
            rawData.SortOrder =  orderByClause?.Items;

            new QueryResultBuilder( rawData ).AddData( batch );            
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
                        case SqlGlobalScalarVariableRefExpression globalRef     : AddFieldFromGlobalVariable( globalRef, name, batch, rawData ); break;
                        case SqlScalarRefExpression scalarRef                   : AddFieldFromColumn( (SqlObjectIdentifier)scalarRef.MultipartIdentifier, name, batch, rawData ); break;
                        case SqlLiteralExpression literalExpression             : AddFieldFromLiteral( literalExpression, name, batch, rawData ); break;
                        case SqlBuiltinScalarFunctionCallExpression functionCall: AddFieldForFunctionCall( functionCall, name, batch, rawData ); break;
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

        private void AddFieldFromGlobalVariable( SqlGlobalScalarVariableRefExpression globalRef, string name, MemoryDbDataReader.ResultBatch batch, RawData rawData )
        {
            var select = new SelectDataBuilder(  ).BuildGlobalVariable( globalRef.VariableName, rawData );
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
