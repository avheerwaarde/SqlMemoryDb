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
        private readonly List<MemoryDbParameter> _Parameters;

        public MemoryDataParameterCollection( )
        {
            _Parameters = new List<MemoryDbParameter>();
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
            _Parameters.CopyTo( ( MemoryDbParameter[] ) array, index );
        }

        protected override void SetParameter( int index, DbParameter value )
        {
            if ( value is MemoryDbParameter parameter )
            {
                if ( parameter.NetDataType == null && parameter.Value != null )
                {
                    parameter.NetDataType = parameter.Value.GetType();
                }
                _Parameters[ index ] = parameter;
            }
            else
            {
                throw new NotSupportedException( "Only parameters of type MemoryDbParameter are supported" );
            }
        }

        protected override void SetParameter( string parameterName, DbParameter value )
        {
            if ( value is MemoryDbParameter parameter )
            {
                if ( parameter.NetDataType == null && parameter.Value != null )
                {
                    parameter.NetDataType = parameter.Value.GetType();
                }
                _Parameters[ IndexOf( parameterName ) ] = parameter;
            }
            else
            {
                throw new NotSupportedException( "Only parameters of type MemoryDbParameter are supported" );
            }
        }

        public override int Count => _Parameters.Count;
        public override bool IsSynchronized => false;
        public override object SyncRoot { get; }

        public override int Add( object value )
        {
            if ( value is MemoryDbParameter parameter )
            {
                if ( parameter.NetDataType == null && parameter.Value != null )
                {
                    parameter.NetDataType = parameter.Value.GetType();
                }
                _Parameters.Add( parameter );    
            }
            else
            {
                throw new NotSupportedException( "Only parameters of type MemoryDbParameter are supported");
            }

            return _Parameters.Count - 1;
        }

        public override void AddRange( Array values )
        {
            _Parameters.AddRange( ( MemoryDbParameter[]) values );
        }

        public override void Clear( )
        {
            _Parameters.Clear(  );
        }

        public override bool Contains( object value )
        {
            return _Parameters.Contains( ( MemoryDbParameter ) value );
        }

        public override int IndexOf( object value )
        {
            return _Parameters.IndexOf( ( MemoryDbParameter ) value );
        }

        public override void Insert( int index, object value )
        {
            _Parameters.Insert( index, ( MemoryDbParameter ) value );
        }

        public override void Remove( object value )
        {
            _Parameters.Remove( ( MemoryDbParameter ) value );
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