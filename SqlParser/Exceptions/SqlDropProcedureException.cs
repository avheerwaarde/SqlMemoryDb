using System;
using System.Collections.Generic;
using System.Text;

namespace SqlMemoryDb.Exceptions
{
    public class SqlDropProcedureException : Exception
    {
        public SqlDropProcedureException( string name )
            : base( $"Cannot drop the procedure '{name}', because it does not exist or you do not have permission." )
        {

        }
    }
}
