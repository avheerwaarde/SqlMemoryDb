using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;

namespace SqlMemoryDb.Helpers
{
    class RowFilterIn : IRowFilter
    {
        private readonly SqlInBooleanExpression _Expression;
        private readonly RawData _RawData;
        private readonly bool _InvertResult;

        public RowFilterIn(RawData rawData, SqlInBooleanExpression expression )
        {
            _Expression = expression;
            _RawData = rawData;
            _InvertResult = _Expression.HasNot;
        }

        public bool IsValid( List<RawTableRow> rawDataRows )
        {
            var scalarValue = Helper.GetValue( _Expression.InExpression, typeof( int ), _RawData, rawDataRows );
            var type = scalarValue.GetType( );
            var valueList = new List<object>( );
            foreach ( var child in _Expression.ComparisonValue.Children )
            {
                if ( child is SqlScalarExpression scalarExpression )
                {
                    var compareObject = Helper.GetValue( scalarExpression, type, _RawData, rawDataRows );
                    valueList.Add( compareObject );
                }
                else if ( child is SqlQueryExpression queryExpression )
                {
                    var database = ((MemoryDbConnection )_RawData.Command.Connection).GetMemoryDatabase( );
                    var command = new MemoryDbCommand( _RawData.Command.Connection, _RawData.Command.Parameters, _RawData.Command.Variables );
                    var reader =  database.ExecuteSqlReader( queryExpression.Sql, command, CommandBehavior.SingleResult );
                    while ( reader.Read(  ) )
                    {
                        valueList.Add( reader[ 0 ] );
                    }
                }
                else
                {
                    throw new NotImplementedException( $"IN is not implemented for type {child}");
                }
            }

            var isValid = valueList.Any( o => scalarValue.Equals( o ) ) ^ _InvertResult;
            return isValid;
        }

        public bool IsValid( List<RawTableJoinRow> rawDataRowList, List<MemoryDbDataReader.ReaderFieldData> fields )
        {
            throw new NotImplementedException( );
        }
    }
}
