using System;
using System.Collections.Generic;
using System.Text;

namespace SqlMemoryDb.SelectData
{
    internal class SelectDataFromGlobalVariables : ISelectDataFunction
    {
        private readonly string _GlobalVariableName;
        private readonly RawData _RawData;

        public SelectDataFromGlobalVariables( string globalVariableName, RawData rawData )
        {
            _GlobalVariableName = globalVariableName;
            _RawData = rawData;
        }

        public object Select( List<RawData.RawDataRow> rows )
        {
            if ( _GlobalVariableName.ToUpper() == "@@IDENTITY" )
            {
                return MemoryDbConnection.GetMemoryDatabase( ).LastIdentitySet;
            }
            throw new NotImplementedException( );
        }

        public bool IsAggregate => false;
        public Type ReturnType => typeof( int );
        public string DbType => "int";

        public object Select( List<List<RawData.RawDataRow>> rows )
        {
            throw new NotImplementedException( );
        }
    }
}
