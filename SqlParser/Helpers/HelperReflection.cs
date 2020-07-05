using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using Generic.Math;

namespace SqlMemoryDb.Helpers
{
    static class HelperReflection
    {
        public static MethodInfo GetMathMethodInfo( string methodName, Type type, int parameterCount = 1 )
        {
            Type enumerableT = typeof(Math);
            var methods = enumerableT.GetMethods( );
            return methods.First( m => m.Name == methodName 
                                       && m.GetParameters(  ).First().ParameterType == type
                                       && m.GetParameters(  ).Length == parameterCount );
        }

        public static bool HasMathMethodInfo( string methodName, Type type, int parameterCount = 1 )
        {
            Type enumerableT = typeof(Math);
            var methods = enumerableT.GetMethods( );
            return methods.Any( m => m.Name == methodName 
                                       && m.GetParameters(  ).First().ParameterType == type
                                       && m.GetParameters(  ).Length == parameterCount );
        }

        public static MethodInfo GetGenericMathMethodInfo( string methodName, Type type )
        {
            var methods = typeof(GenericMath).GetMethods( );
            var method = methods.First( m => m.Name == methodName );
            MethodInfo generic = method.MakeGenericMethod(type);
            return generic;
        }

        public static object Negate( object value )
        {
            MethodInfo generic = GetGenericMathMethodInfo( "Subtract", value.GetType( ) );
            return generic.Invoke( null, new object[] {0, value} );
        }
    }
}
