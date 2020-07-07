using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.Parser;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Exceptions;

namespace SqlMemoryDb.SelectData
{
    class SelectDataFromNullScalarExpression : ISelectDataFunction
    {
        public bool IsAggregate => false;
        Type ISelectDataFunction.ReturnType => _ReturnType;
        DbType ISelectDataFunction.DbType => _DbType;
        
        private Type _ReturnType = typeof(bool);
        private DbType _DbType = DbType.Boolean;

        private readonly SqlNullScalarExpression _FunctionCall;
        private readonly RawData _RawData;

        public SelectDataFromNullScalarExpression( SqlNullScalarExpression functionCall, RawData rawData )
        {
            _FunctionCall = functionCall;
            _RawData = rawData;
        }

        public object Select( List<RawData.RawDataRow> rows )
        {
            var selectExpression = ParseAsSelect( _FunctionCall.Sql );
            var querySpecification = (SqlQuerySpecification)selectExpression.SelectSpecification.QueryExpression;

            var db = (( MemoryDbConnection ) _RawData.Command.Connection ).GetMemoryDatabase( );
            var batch = new MemoryDbDataReader.ResultBatch(  );
            var rawData = new RawData( _RawData.Command, batch  );
            rawData.RawRowList.Add( rows );
            var statement = new ExecuteQueryStatement( db, _RawData.Command );
            statement.InitializeFields( batch, querySpecification.SelectClause.Children.ToList(  ), rawData );
            new QueryResultBuilder( rawData, false ).AddData( batch );

            var row = batch.ResultRows[ 0 ];
            for ( int fieldIndex = 0; fieldIndex < batch.Fields.Count; fieldIndex++ )
            {
                if ( row[ fieldIndex ] != null )
                {
                    _DbType = (DbType) Enum.Parse(typeof(DbType), batch.Fields[ fieldIndex ].DbType, true);
                    _ReturnType = batch.Fields[ fieldIndex ].NetType;
                    return row[ fieldIndex ];
                }
            }
            return null;
        }

        private SqlSelectStatement ParseAsSelect( string sql )
        {
            sql = sql.Replace( "COALESCE", "SELECT" );
            sql = ReplaceFirstOccurrence( sql, "(", " " );
            sql = ReplaceLastOccurrence( sql, ")", " " );

            var result = Parser.Parse( sql );
            if ( result.Errors.Any())
            {
                throw new SqlServerParserException( result.Errors );
            }

            return (SqlSelectStatement)result.Script.Batches.First( ).Children.First( );
        }

        private string ReplaceFirstOccurrence( string sql, string find, string replace )
        {
            var index = sql.IndexOf( find, StringComparison.InvariantCultureIgnoreCase );
            if ( index >= 0 )
            {
                return sql.Substring( 0, index ) + replace + sql.Substring( index + find.Length );
            }

            return sql;
        }

        private string ReplaceLastOccurrence( string sql, string find, string replace )
        {
            var index = sql.LastIndexOf( find, StringComparison.InvariantCultureIgnoreCase );
            if ( index >= 0 )
            {
                return sql.Substring( 0, index ) + replace + sql.Substring( index + find.Length );
            }

            return sql;
        }

        public object Select( List<List<RawData.RawDataRow>> rows )
        {
            throw new NotImplementedException( );
        }


    }
}
