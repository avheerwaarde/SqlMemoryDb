using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;

namespace SqlMemoryDb.SelectData
{
    internal class SelectDataFromColumn : ISelectData
    {
        public readonly TableColumn TableColumn;

        Type ISelectData.ReturnType => _ReturnType;
        DbType ISelectData.DbType => _DbType;
        public SqlScalarExpression Expression => null;
        
        private readonly Type _ReturnType;
        private readonly DbType _DbType;
        private readonly RawData _RawData;

        internal SelectDataFromColumn( TableColumn tableColumn, RawData rawData )
        {
            TableColumn = tableColumn;
            _ReturnType = tableColumn.Column.NetDataType;
            _DbType = tableColumn.Column.DbDataType;
            _RawData = rawData;
        }

        public object Select( List<RawData.RawDataRow> rows )
        {
            if ( rows == null )
            {
                return null;
            }

            var tableRow = rows.SingleOrDefault( r => r.Name == TableColumn.TableName );
            if ( tableRow == null )
            {
                return null;
            }
            object value;
            if ( TableColumn.Column.ComputedExpression != null )
            {
                var select = new SelectDataBuilder(  ).Build( TableColumn.Column.ComputedExpression, _RawData );
                value = select.Select( rows );
                if ( value.GetType(  ) != TableColumn.Column.NetDataType )
                {
                    value = Convert.ChangeType( value, TableColumn.Column.NetDataType );
                }
            }
            else
            {
                value = tableRow.Row[ TableColumn.Column.Order ];
            }
            // pad string if we have a string of a fixed length
            if ( TableColumn.Column.NetDataType == typeof(string) && TableColumn.Column.IsFixedSize )
            {
                value = value.ToString( ).PadRight( TableColumn.Column.Size );
            }
            return value;
        }
    }
}
