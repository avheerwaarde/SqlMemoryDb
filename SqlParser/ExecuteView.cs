using System.Linq;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Exceptions;
using SqlMemoryDb.Helpers;

namespace SqlMemoryDb
{
    class ExecuteView
    {
        private readonly MemoryDatabase _MemoryDatabase;
        private readonly MemoryDbCommand _Command;

        public ExecuteView( MemoryDatabase memoryDatabase, MemoryDbCommand command )
        {
            _MemoryDatabase = memoryDatabase;
            _Command = command;
        }

        public void Execute( SqlCreateViewStatement createView )
        {
            var name = Helper.GetQualifiedName( createView.Definition.Name );
            if ( _MemoryDatabase.Views.ContainsKey( name ) )
            {
                throw new SqlObjectAlreadyExistsException( name );
            }
            _MemoryDatabase.Views.Add( name, createView );            
        }

        public void Execute( SqlDropViewStatement dropView )
        {
            var name = Helper.GetQualifiedName( dropView.Objects.First() );
            if ( _MemoryDatabase.Views.ContainsKey( name ) == false )
            {
                throw new SqlDropViewException( name );
            }

            _MemoryDatabase.Views.Remove( name );
        }

        public void Execute( SqlAlterViewStatement alterView )
        {
            var name = Helper.GetQualifiedName( alterView.Definition.Name );
            if ( _MemoryDatabase.Views.ContainsKey( name ) == false )
            {
                throw new SqlInvalidObjectNameException( name );
            }
            _MemoryDatabase.Views[ name ] = alterView;
        }
    }
}
