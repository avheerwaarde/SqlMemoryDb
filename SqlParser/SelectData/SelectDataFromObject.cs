using System.Collections.Generic;

namespace SqlMemoryDb.SelectData
{
    internal class SelectDataFromObject : ISelectData
    {
        private readonly object _Value;

        public SelectDataFromObject( object value )
        {
            _Value = value;
        }

        public object Select( List<ExecuteQueryStatement.RawData.RawDataRow> rows )
        {
            return _Value;
        }
    }
}
