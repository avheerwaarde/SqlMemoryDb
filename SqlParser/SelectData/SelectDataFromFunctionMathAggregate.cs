using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Helpers;

namespace SqlMemoryDb.SelectData
{
    class SelectDataFromFunctionMathAggregate: ISelectDataFunction
    {
        public bool IsAggregate => true;
        Type ISelectDataFunction.ReturnType => _ReturnType;
        string ISelectDataFunction.DbType => _DbType;
        
        private readonly Type _ReturnType = typeof(int);
        private readonly string _DbType = "int32";
        private readonly SqlBuiltinScalarFunctionCallExpression _FunctionCall;
        private readonly RawData _RawData;
        private readonly SqlColumnRefExpression _ColumnRef;

        private readonly List<string> _NeedsColumn = new List<string>
        {
            "MIN", "MAX","AVG", "SUM"
        };

        public SelectDataFromFunctionMathAggregate( SqlBuiltinScalarFunctionCallExpression functionCall, RawData rawData )
        {
            _FunctionCall = functionCall;
            _RawData = rawData;
            if ( _NeedsColumn.Contains( _FunctionCall.FunctionName.ToUpper() ) )
            {
                _ColumnRef = GetSingleColumn(  );
                var tc = Helper.GetTableColumn( _ColumnRef, _RawData );
                _ReturnType = tc.Column.NetDataType;
                _DbType = tc.Column.DbDataType.ToString( );
            }
        }

        public object Select( List<RawData.RawDataRow> rows )
        {
            throw new NotImplementedException( );
        }


        public object Select( List<List<RawData.RawDataRow>> rows )
        {
            switch ( _FunctionCall.FunctionName.ToUpper() )
            {
                case "COUNT":
                    return rows.Count;
                case "MIN":
                    return rows.Select(  r => Helper.GetValue( _ColumnRef, _ReturnType, _RawData, r ) ).Min();
                case "MAX":
                    return rows.Select(  r => Helper.GetValue( _ColumnRef, _ReturnType, _RawData, r ) ).Max();
                case "AVG":
                    var enumerableAvg = rows.Select(  r => Helper.GetValue( _ColumnRef, _ReturnType, _RawData, r ) );
                    return ExecuteGenericListMethod( enumerableAvg, _ReturnType, "Average" );
                case "SUM":
                    var enumerableSum = rows.Select(  r => Helper.GetValue( _ColumnRef, _ReturnType, _RawData, r ) );
                    return ExecuteGenericListMethod( enumerableSum, _ReturnType, "Sum" );
                default:
                    throw new NotImplementedException();
            }

        }

        private static object ExecuteGenericListMethod( IEnumerable<object> objectList, Type t, string methodName )
        {
            var listType = typeof(List<>);
            var constructedListType = listType.MakeGenericType(t);
            var instance = Activator.CreateInstance(constructedListType);
            var methodAdd = constructedListType.GetMethod( "Add" );
            foreach ( var objectValue in objectList )
            {
                methodAdd.Invoke( instance, new object[] { objectValue } );
            }

            Type enumerableT = typeof(Enumerable);
            var methods = enumerableT.GetMethods( );
            var method = methods.First( m => IsMethodForType( m, methodName, t ) );
            return method.Invoke( null, new object[] { instance });
        }

        private static bool IsMethodForType( MethodInfo m, string methodName, Type type )
        {
            return m.Name == methodName && m.GetParameters( ).First( ).ParameterType.GenericTypeArguments[0] == type;
        }

        private SqlColumnRefExpression GetSingleColumn( )
        {
            if ( _FunctionCall.Arguments != null && _FunctionCall.Arguments.Count == 1 && _FunctionCall.Arguments[0] is SqlColumnRefExpression )
            {
                return ( SqlColumnRefExpression )_FunctionCall.Arguments[ 0 ];
            }
            throw new NotImplementedException("Currently we only allow a single column as function parameter");
        }
    }
}
