using System.Collections.Generic;
using System.Linq;

namespace SqlMemoryDb.SelectData
{
    internal class SelectDataFromColumn : ISelectData
    {
        private readonly TableColumn _TableColumn;

        internal SelectDataFromColumn( TableColumn tableColumn )
        {
            _TableColumn = tableColumn;
        }

        public object Select( List<ExecuteSelectStatement.RawData.RawDataRow> rows )
        {
            if ( rows == null )
            {
                return null;
            }

            var tableRow = rows.Single( r => r.Name == _TableColumn.TableName );
            return tableRow.Row[ _TableColumn.Column.Order - 1 ];
        }
    }
}
