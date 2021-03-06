﻿using System;using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using Generic.Math;

namespace SqlMemoryDb.Helpers
{
    static class HelperReflection
    {
        private static readonly HashSet<Type> _NumericTypes = new HashSet<Type>
        {
            typeof(int),  typeof(double),  typeof(decimal),
            typeof(long), typeof(short),   typeof(sbyte),
            typeof(byte), typeof(ulong),   typeof(ushort),  
            typeof(uint), typeof(float)
        };

        private static readonly HashSet<Type> _IntegerTypes = new HashSet<Type>
        {
            typeof(int), typeof(long), typeof(sbyte),
            typeof(byte), typeof(ulong),   typeof(ushort),  
            typeof(uint)
        };

        
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
            var type = value.GetType( );
            MethodInfo generic = GetGenericMathMethodInfo( "Subtract", type );
            if ( type == typeof(decimal)) 
            {
                return generic.Invoke( null, new object[] { new decimal( 0 ), value} );
            }
            return generic.Invoke( null, new object[] {0, value} );
        }

        public static bool IsNumeric(Type myType)
        {
            return _NumericTypes.Contains(Nullable.GetUnderlyingType(myType) ?? myType);
        }

        public static bool IsInteger(Type myType)
        {
            return _IntegerTypes.Contains(Nullable.GetUnderlyingType(myType) ?? myType);
        }
    }
}
