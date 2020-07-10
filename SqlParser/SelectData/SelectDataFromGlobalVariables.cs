using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace SqlMemoryDb.SelectData
{
    internal class SelectDataFromGlobalVariables : ISelectData
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
            switch ( _GlobalVariableName.ToUpper( ) )
            {
                case "@@IDENTITY": return ((MemoryDbConnection )_RawData.Command.Connection).GetMemoryDatabase( ).LastIdentitySet;
                case "@@VERSION": return ((MemoryDbConnection )_RawData.Command.Connection).ServerVersion;
                default:
                    throw new NotImplementedException( );
            }
        }

        public Type ReturnType => typeof( int );
        public DbType DbType => DbType.Int32;
    }
}
