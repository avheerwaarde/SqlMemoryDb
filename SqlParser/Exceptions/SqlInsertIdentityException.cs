using System;
using System.Collections.Generic;
using System.Text;

namespace SqlMemoryDb.Exceptions
{
    public class SqlInsertIdentityException : Exception
    {
        public SqlInsertIdentityException( string table, string column ) : base( $"Cannot insert explicit value for identity column '{column}' in table '{table}' when IDENTITY_INSERT is set to OFF" )
        {

        }
    }
}
