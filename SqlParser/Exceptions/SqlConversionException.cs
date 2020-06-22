using System;
using System.Collections.Generic;
using System.Text;

namespace SqlMemoryDb.Exceptions
{
    public class SqlConversionException : Exception
    {
        public SqlConversionException( string from, string to )
            : base( $"Conversion failed when converting the {from} value to data type {to}.")
        {
        }
    }
}
