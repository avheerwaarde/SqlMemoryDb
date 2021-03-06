﻿using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;

namespace SqlMemoryDb.SelectData
{
    internal class SelectDataFromObject : ISelectData
    {
        private readonly object _Value;
        Type ISelectData.ReturnType => _ReturnType;
        DbType ISelectData.DbType => _DbType;
        public SqlScalarExpression Expression => null;
        
        private readonly Type _ReturnType;
        private readonly DbType _DbType;

        public SelectDataFromObject( object value, string dbType )
        {
            _Value = value;
            _DbType = ( DbType ) Enum.Parse( typeof( DbType ), dbType, true );
            if ( value == null )
            {
                _ReturnType = typeof( DBNull );
                return;
            }
            _ReturnType = value.GetType( );
        }

        public object Select( RawTableJoinRow rows )
        {
            return _Value;
        }
    }
}
