using System;
using System.Collections.Generic;
using System.Data.Common;
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
        private readonly MemoryDbCommand _Command;

        public ExecuteProcedure( MemoryDatabase memoryDatabase, MemoryDbCommand command )
        {
            _MemoryDatabase = memoryDatabase;
            _Command = command;
        }

        public void Execute( SqlCreateProcedureStatement createProcedure )
        {
            var name = Helper.GetQualifiedName( createProcedure.Definition.Name );
            if ( _MemoryDatabase.StoredProcedures.ContainsKey( name ) )
            {
                throw new SqlObjectAlreadyExistsException( name );
            }
            _MemoryDatabase.StoredProcedures.Add( name, createProcedure );
        }

        public void Execute( SqlAlterProcedureStatement alterProcedure )
        {
            var name = Helper.GetQualifiedName( alterProcedure.Definition.Name );
            if ( _MemoryDatabase.StoredProcedures.ContainsKey( name ) == false )
            {
                throw new SqlInvalidObjectNameException( name );
            }
            _MemoryDatabase.StoredProcedures[ name ] = alterProcedure;
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

        public void Execute( SqlExecuteModuleStatement executeProcedure )
        {
            var name = Helper.GetQualifiedName( executeProcedure.Module.ObjectIdentifier );
            if ( _MemoryDatabase.StoredProcedures.ContainsKey( name ) == false )
            {
                throw new SqlInvalidObjectNameException( name );
            }
            var storedProcedure = _MemoryDatabase.StoredProcedures[ name ];
            var command = new MemoryDbCommand( _Command.Connection ) { DataReader = _Command.DataReader };
            SetStoredProcedureParameters( command, storedProcedure, executeProcedure.Arguments );
            foreach ( var statement in storedProcedure.Statements )
            {
                _MemoryDatabase.ExecuteStatement( command, statement );          
            }
        }

        private void SetStoredProcedureParameters( MemoryDbCommand command,
            SqlCreateAlterProcedureStatementBase storedProcedure,
            SqlExecuteArgumentCollection arguments )
        {
            if ( arguments != null )
            {
                _MemoryDatabase.AddParameters( command, storedProcedure.Definition.Parameters );
                for ( int argumentIndex = 0; argumentIndex < arguments.Count; argumentIndex++ )
                {
                    var parameter = (MemoryDbParameter)command.Parameters[ argumentIndex ];
                    var argument = arguments[ argumentIndex ];
                    if ( argument.Parameter != null )
                    {
                        parameter.Value = Helper.GetParameter( _Command, argument.Parameter ).Value;
                    }
                    else
                    {
                        parameter.Value = Helper.GetValue( argument.Value, parameter.NetDataType, new RawData( _Command ),
                            new List<RawTableRow>( ) );
                    }
                }
            }
            else
            {
                foreach ( MemoryDbParameter parameter in _Command.Parameters )
                {
                    command.Parameters.Add( parameter );
                }
            }
        }
    }
}