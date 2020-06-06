using System.Collections.Generic;
using System.Linq;

namespace SqlMemoryDb.SelectData
{
    internal class SelectDataFromColumn : ISelectData
    {
        public readonly TableColumn TableColumn;

        internal SelectDataFromColumn( TableColumn tableColumn )
        {
            TableColumn = tableColumn;
        }

        public object Select( List<RawData.RawDataRow> rows )
        {
            if ( rows == null )
            {
                return null;
            }

            var tableRow = rows.Single( r => r.Name == TableColumn.TableName );
            return tableRow.Row[ TableColumn.Column.Order - 1 ];
        }
    }
}
