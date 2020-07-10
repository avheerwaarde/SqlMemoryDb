﻿using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Helpers;

namespace SqlMemoryDb.SelectData
{
    internal class SelectDataFromVariables : ISelectData
    {
        private readonly MemoryDbParameter _Parameter;

        public SelectDataFromVariables( SqlScalarExpression scalarExpression, MemoryDbCommand command )
        {
            _Parameter = Helper.GetParameter( command, scalarExpression );
        }

        public object Select( List<RawData.RawDataRow> rows )
        {
            return _Parameter.Value;
        }

        public Type ReturnType => _Parameter.NetDataType;
        public DbType DbType => _Parameter.DbType;

        public object Select( List<List<RawData.RawDataRow>> rows )
        {
            throw new NotImplementedException( );
        }
    }
}
