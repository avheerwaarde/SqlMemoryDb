using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace SqlMemoryDb
{
    public class MemoryDataParameterCollection : DbParameterCollection
    {
        private readonly List<MemoryDbDataParameter> _Parameters;

        public MemoryDataParameterCollection( )
        {
            _Parameters = new List<MemoryDbDataParameter>();
            SyncRoot = new Object();
        }

        public override IEnumerator GetEnumerator( )
        {
            return _Parameters.GetEnumerator( );
        }

        protected override DbParameter GetParameter( int index )
        {
            return _Parameters[ index ];
        }

        protected override DbParameter GetParameter( string parameterName )
        {
            return _Parameters[ IndexOf( parameterName ) ];
        }

        public override void CopyTo( Array array, int index )
        {
            _Parameters.CopyTo( ( MemoryDbDataParameter[] ) array, index );
        }

        protected override void SetParameter( int index, DbParameter value )
        {
            _Parameters[ index ] = ( MemoryDbDataParameter ) value;
        }

        protected override void SetParameter( string parameterName, DbParameter value )
        {
            _Parameters[ IndexOf( parameterName ) ] = ( MemoryDbDataParameter ) value;
        }

        public override int Count => _Parameters.Count;
        public override bool IsSynchronized => false;
        public override object SyncRoot { get; }

        public override int Add( object value )
        {
            if ( value is MemoryDbDataParameter parameter )
            {
                _Parameters.Add( parameter );    
            }
            throw new NotSupportedException( "Only parameters of type MemoryDbDataParameter are supported");
        }

        public override void AddRange( Array values )
        {
            _Parameters.AddRange( ( MemoryDbDataParameter[]) values );
        }

        public override void Clear( )
        {
            _Parameters.Clear(  );
        }

        public override bool Contains( object value )
        {
            return _Parameters.Contains( ( MemoryDbDataParameter ) value );
        }

        public override int IndexOf( object value )
        {
            return _Parameters.IndexOf( ( MemoryDbDataParameter ) value );
        }

        public override void Insert( int index, object value )
        {
            _Parameters.Insert( index, ( MemoryDbDataParameter ) value );
        }

        public override void Remove( object value )
        {
            _Parameters.Remove( ( MemoryDbDataParameter ) value );
        }

        public override void RemoveAt( int index )
        {
            _Parameters.RemoveAt( index );
        }

        public override bool IsFixedSize => false;
        public override bool IsReadOnly => false;


        public override bool Contains( string parameterName )
        {
            return _Parameters.Any( p => String.Compare( p.ParameterName, parameterName, StringComparison.CurrentCulture )  == 0);
        }

        public override int IndexOf( string parameterName )
        {
            return _Parameters.FindIndex( p =>  String.Compare( p.ParameterName, parameterName, StringComparison.CurrentCulture )  == 0 );
        }

        public override void RemoveAt( string parameterName )
        {
            _Parameters.RemoveAt( IndexOf( parameterName ) );
        }
    }
}