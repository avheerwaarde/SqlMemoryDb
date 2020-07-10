using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace SqlMemoryDb.SelectData
{
    internal class SelectDataFromColumn : ISelectData
    {
        public readonly TableColumn TableColumn;

        Type ISelectData.ReturnType => _ReturnType;
        DbType ISelectData.DbType => _DbType;
        
        private readonly Type _ReturnType;
        private readonly DbType _DbType;


        internal SelectDataFromColumn( TableColumn tableColumn )
        {
            TableColumn = tableColumn;
            _ReturnType = tableColumn.Column.NetDataType;
            _DbType = tableColumn.Column.DbDataType;
        }

        public object Select( List<RawData.RawDataRow> rows )
        {
            if ( rows == null )
            {
                return null;
            }

            var tableRow = rows.Single( r => r.Name == TableColumn.TableName );
            return tableRow.Row[ TableColumn.Column.Order ];
        }
    }
}
