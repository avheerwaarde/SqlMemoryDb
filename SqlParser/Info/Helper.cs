using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Exceptions;

namespace SqlMemoryDb.Info
{
    class Helper
    {
        public static string GetQualifiedName( SqlObjectIdentifier identifier )
        {
            return identifier.SchemaName.Value ?? "dbo" + "." + identifier.ObjectName;
        }

        public static object GetValueFromString( Column column, string source )
        {
            if ( column.NetDataType == typeof(string) )
            {
                if ( column.Size > 0 && column.Size < source.Length )
                {
                    throw new SqlDataTruncatedException( column.Size, source.Length );
                }
                return source;
            }
            else
            {
                switch ( column.DbDataType )
                {
                    case DbType.Byte: return  Convert.ToByte( source ); 
                    case DbType.Int16: return  Convert.ToInt16( source );
                    case DbType.Int32: return  Convert.ToInt32( source );
                    case DbType.Int64: return  Convert.ToInt64( source );
                    case DbType.Single: return Convert.ToSingle( source );
                    case DbType.Double: return Convert.ToDouble( source );
                    case DbType.Decimal: return  Convert.ToDecimal( source );
                    default:
                        throw new NotImplementedException( $"Defaults not supported for type {column.DbDataType }" );
                }
            }
        }

        public static string CleanSql( string sourceSql )
        {
            return sourceSql.Replace( '\n', ' ' ).Replace( '\r', ' ' ).Replace( '\t', ' ' ).Trim( );
        }

        public static string GetStringValue( string part )
        {
            if ( (part.StartsWith( "N'" ) || part.StartsWith( "'" )) && part.EndsWith( "'" ))
            {
                return part.TrimStart( new[] {'N', 'n'} ).TrimStart( new[] {'\''} ).TrimEnd( new[] {'\''} );
            }

            return part;
        }

        public static object GetValueFromParameter( Column column, string value, DbParameterCollection parameters )
        {
            var name = value.TrimStart( new []{'@'} );
            if ( parameters.Contains( name ) == false )
            {
                throw new SqlInvalidParameterNameException( value );
            }

            var parameter = parameters[ name ];
            return parameter.Value;
        }
    }
}
