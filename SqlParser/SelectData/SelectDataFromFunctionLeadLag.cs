using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Microsoft.SqlServer.Management.SqlParser.Parser;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Exceptions;
using SqlMemoryDb.Helpers;

namespace SqlMemoryDb.SelectData
{
    class SelectDataFromFunctionLeadLag: ISelectDataFunction
    {
        public bool IsAggregate => false;
        Type ISelectDataFunction.ReturnType => _ReturnType;
        DbType ISelectDataFunction.DbType => _DbType;
        
        private readonly Type _ReturnType;
        private readonly DbType _DbType;

        private readonly SqlBuiltinScalarFunctionCallExpression _FunctionCall;
        private readonly RawData _RawData;
        private readonly SqlSelectStatement _SelectExpression;
        private string _PartitionField;

        public SelectDataFromFunctionLeadLag( SqlBuiltinScalarFunctionCallExpression functionCall, RawData rawData, SelectDataFunctionInfo info )
        {
            _FunctionCall = functionCall;
            _RawData = rawData;
            if ( info.ReturnDbType.HasValue )
            {
                _ReturnType = info.ReturnType;
                _DbType = info.ReturnDbType.Value;
            }

            _SelectExpression = ParseAsSelect( _FunctionCall.Tokens );
            var batch = ExecuteSelect( _SelectExpression, _RawData.RawRowList.FirstOrDefault() );
            _DbType = ( DbType ) Enum.Parse( typeof( DbType ), batch.Fields[ 0 ].DbType, true );
            _ReturnType = batch.Fields[ 0 ].NetType;
        }

        public object Select( List<RawData.RawDataRow> rows )
        {
            var leap = ( int ) Helper.GetValue( _FunctionCall.Arguments[ 1 ], typeof( int ), _RawData, rows );
            if ( _FunctionCall.FunctionName.ToUpper() == "LAG" )
            {
                leap = 0 - leap;
            }

            var batch = ExecuteSelect( _SelectExpression, rows );
            var index = batch.RawRows.IndexOf( rows );
            if ( index + leap >= 0 && index + leap < batch.RawRows.Count  )
            {
                return batch.ResultRows[ index + leap ][0];
            }

            if ( _FunctionCall.Arguments.Count > 2 )
            {
                return Helper.GetValue( _FunctionCall.Arguments[ 2 ], _ReturnType, _RawData, rows );
            }
            return null;
        }

        private MemoryDbDataReader.ResultBatchWithRawRows ExecuteSelect( SqlSelectStatement selectExpression,
            List<RawData.RawDataRow> rows )
        {
            var querySpecification = ( SqlQuerySpecification ) selectExpression.SelectSpecification.QueryExpression;
            var db = ( ( MemoryDbConnection ) _RawData.Command.Connection ).GetMemoryDatabase( );
            var command = new MemoryDbCommand( _RawData.Command.Connection );
            var batch = new MemoryDbDataReader.ResultBatchWithRawRows( );
            var rawData = new RawData( command, batch )
            {
                RawRowList = _RawData.RawRowList,
                TableAliasList = _RawData.TableAliasList,
                SortOrder = selectExpression.SelectSpecification.OrderByClause.Items
            };
            var statement = new ExecuteQueryStatement( db, command );
            statement.InitializeFields( batch, querySpecification.SelectClause.Children.ToList( ), rawData );
            if ( string.IsNullOrWhiteSpace(_PartitionField) == false )
            {
                AddWhereParameter( batch, rawData, rows, querySpecification.WhereClause );
            }
            new QueryResultBuilder( rawData ).AddData( batch );
            return batch;
        }

        private void AddWhereParameter( MemoryDbDataReader.ResultBatchWithRawRows batch, RawData rawData,
            List<RawData.RawDataRow> rows, SqlWhereClause whereClause )
        {
            var tableColumn = Helper.FindTableAndColumn( null, _PartitionField, rawData.TableAliasList );
            var value = new SelectDataFromColumn( tableColumn ).Select( rows );
            rawData.Parameters.Add( new MemoryDbParameter { ParameterName = _PartitionField, Value = value } );
            rawData.WhereClause = whereClause.Expression;
        }

        private SqlSelectStatement ParseAsSelect( IEnumerable<Token> tokens )
        {
            var finder = new TokenFinder( tokens.ToList(  ) );
            var id = finder.GetIdAfterToken( "(" );
            var indexOrder = finder.FindToken( "TOKEN_ORDER" );
            var order = finder.GetTokensBetween( "TOKEN_BY", ")", indexOrder );
            var sql = $"SELECT {id}";

            var indexPartition = finder.FindToken( "TOKEN_s_PARTITION" );
            if ( indexPartition != -1 )
            {
                _PartitionField = finder.GetTokensBetween( "TOKEN_BY", "TOKEN_ORDER", indexPartition );
                sql += $" WHERE {_PartitionField} = @{_PartitionField}";
            }
            sql += $" ORDER BY {order}";
            var result = Parser.Parse( sql );
            if ( result.Errors.Any())
            {
                throw new SqlServerParserException( result.Errors );
            }

            return (SqlSelectStatement)result.Script.Batches.First( ).Children.First( );
        }


        public object Select( List<List<RawData.RawDataRow>> rows )
        {
            throw new NotImplementedException( );
        }
    }
}
