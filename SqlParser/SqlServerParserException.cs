using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.Parser;

namespace SqlParser
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
