using System;
using System.Collections.Generic;
using System.Text;

namespace SqlMemoryDb.Exceptions
{
    public class SqlInvalidTableNameException : Exception
    {
        public SqlInvalidTableNameException( string tableName ) : base( $"Invalid table with name {tableName ?? ""}" )
        {

        }
    }
}
