using System;
using System.Collections.Generic;
using Microsoft.SqlServer.Management.SqlParser.Parser;

namespace SqlMemoryDb.Exceptions
{
    public class SqlServerParserException : Exception
    {
        public List<Error> Errors;

        public SqlServerParserException( IEnumerable<Error> errors )
        {
            Errors = new List<Error>(errors);
        }
    }
}
