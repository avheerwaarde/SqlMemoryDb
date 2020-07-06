using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.Parser;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Exceptions;
using SqlMemoryDb.Helpers;

namespace SqlMemoryDb.SelectData
{
    class SelectDataFromFunctionLead: ISelectDataFunction
    {
        public bool IsAggregate => false;
        Type ISelectDataFunction.ReturnType => _ReturnType;
        DbType ISelectDataFunction.DbType => _DbType;
        
        private readonly Type _ReturnType = typeof(bool);
        private readonly DbType _DbType = DbType.Boolean;

        private readonly SqlBuiltinScalarFunctionCallExpression _FunctionCall;
        private readonly RawData _RawData;
        
        public SelectDataFromFunctionLead( SqlBuiltinScalarFunctionCallExpression functionCall, RawData rawData, SelectDataFunctionInfo info )
        {
            _FunctionCall = functionCall;
            _RawData = rawData;
            if ( info.ReturnDbType.HasValue )
            {
                _ReturnType = info.ReturnType;
                _DbType = info.ReturnDbType.Value;
            }
        }

        public object Select( List<RawData.RawDataRow> rows )
        {
            var selectExpression = ParseAsSelect( _FunctionCall.Tokens );
            var batch = ExecuteSelect( rows, selectExpression );
            var index = batch.RawRows.IndexOf( rows );
            return batch;
        }

        private MemoryDbDataReader.ResultBatchWithRawRows ExecuteSelect( List<RawData.RawDataRow> rows, SqlSelectStatement selectExpression )
        {
            var querySpecification = ( SqlQuerySpecification ) selectExpression.SelectSpecification.QueryExpression;
            var db = ( ( MemoryDbConnection ) _RawData.Command.Connection ).GetMemoryDatabase( );
            var reader = new MemoryDbDataReader( CommandBehavior.SingleResult );
            var batch = new MemoryDbDataReader.ResultBatchWithRawRows( );
            var rawData = new RawData( _RawData.Command, batch )
            {
                RawRowList = _RawData.RawRowList,
                TableAliasList = _RawData.TableAliasList,
                SortOrder = selectExpression.SelectSpecification.OrderByClause.Items
            };
            var statement = new ExecuteQueryStatement( db, _RawData.Command, reader );
            statement.InitializeFields( batch, querySpecification.SelectClause.Children.ToList( ), rawData );
            new QueryResultBuilder( rawData, false ).AddData( batch );
            return batch;
        }

        private SqlSelectStatement ParseAsSelect( IEnumerable<Token> tokens )
        {
            var finder = new TokenFinder( tokens.ToList(  ) );
            var id = finder.GetIdAfterToken( "(" );
            var order = finder.GetTokensBetween( "TOKEN_BY", ")" );
            var result = Parser.Parse( $"SELECT {id} ORDER BY {order}" );
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
