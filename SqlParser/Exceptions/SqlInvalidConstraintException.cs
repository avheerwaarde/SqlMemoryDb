using System;

namespace SqlMemoryDb.Exceptions
{
    public class SqlInvalidConstraintException : Exception
    {
        public SqlInvalidConstraintException( string name ) : base ( $"Constraint '{name}' does not exist.")
        {
        }
    }
}
