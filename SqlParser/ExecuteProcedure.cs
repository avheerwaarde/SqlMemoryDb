using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Exceptions;
using SqlMemoryDb.Helpers;
using SqlParser;

namespace SqlMemoryDb
{
    public class ExecuteProcedure
    {
        private readonly MemoryDatabase _MemoryDatabase;

        public ExecuteProcedure( MemoryDatabase memoryDatabase )
        {
            _MemoryDatabase = memoryDatabase;
        }

        public void Execute( SqlCreateProcedureStatement createProcedure )
        {
            var name = Helper.GetQualifiedName( createProcedure.Definition.Name );
            if ( _MemoryDatabase.StoredProcedures.ContainsKey( name ) )
            {
                throw new SqlObjectAlreadyExistsException( name );
            }
            _MemoryDatabase.StoredProcedures.Add( name, createProcedure.Statements );
        }

        public void Execute( SqlAlterProcedureStatement alterProcedure )
        {
            var name = Helper.GetQualifiedName( alterProcedure.Definition.Name );
            if ( _MemoryDatabase.StoredProcedures.ContainsKey( name ) == false )
            {
                throw new SqlInvalidObjectNameException( name );
            }
            _MemoryDatabase.StoredProcedures[ name ] = alterProcedure.Statements ;
        }

        public void Execute( SqlDropProcedureStatement dropProcedure )
        {
            var name = Helper.GetQualifiedName( dropProcedure.Objects.First() );
            if ( _MemoryDatabase.StoredProcedures.ContainsKey( name ) == false )
            {
                throw new SqlDropProcedureException( name );
            }

            _MemoryDatabase.StoredProcedures.Remove( name );
        }
    }
}