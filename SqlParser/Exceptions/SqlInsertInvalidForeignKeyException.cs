using System;
using System.Collections.Generic;
using System.Text;

namespace SqlMemoryDb.Exceptions
{
    public class SqlInsertInvalidForeignKeyException : Exception
    {
        public SqlInsertInvalidForeignKeyException( string constraint, string table, string column )
            : base( $"The INSERT statement conflicted with the FOREIGN KEY constraint '{constraint}'. The conflict occurred in table '{table}', column '{column}'." )
        {

        }
    }
}
