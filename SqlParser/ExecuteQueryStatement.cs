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


        public ExecuteQueryStatement( MemoryDbCommand command, MemoryDbDataReader reader )
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
                rawData.AddTablesFromClause( expression.FromClause, tables );
                if ( expression.WhereClause != null )
                {
                    rawData.ExecuteWhereClause( expression.WhereClause );
                }
            }

            var batch = new MemoryDbDataReader.ResultBatch(  );
            InitializeFields( batch, expression.SelectClause.Children.ToList(  ), rawData );
            if ( expression.GroupByClause != null )
            {
                rawData.AddGroupByClause( expression.GroupByClause );
            }

            rawData.HavingClause = expression.HavingClause?.Expression;
            rawData.SortOrder =  selectStatement.SelectSpecification.OrderByClause?.Items;

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
