using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Exceptions;
using SqlMemoryDb.SelectData;

namespace SqlMemoryDb.Helpers
{
    class Helper
    {
        public static string GetAliasName( SqlTableRefExpression tableRef )
        {
            return tableRef.Alias == null ? GetQualifiedName(tableRef.ObjectIdentifier) : tableRef.Alias.Value;
        }

        public static string GetQualifiedName( SqlObjectIdentifier identifier )
        {
            return identifier.SchemaName.Value ?? "dbo" + "." + identifier.ObjectName;
        }

        public static string GetColumnName( SqlColumnRefExpression expression )
        {
            return ((SqlObjectIdentifier)((SqlColumnRefExpression)expression).MultipartIdentifier).ObjectName.Value;
        }

        public static string GetColumnAlias( SqlSelectScalarExpression scalarExpression )
        {
            return scalarExpression.Alias != null
                ? scalarExpression.Alias.Value
                : GetColumnName( ( SqlColumnRefExpression ) scalarExpression.Expression );
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
                    case DbType.Byte   : return Convert.ToByte( source ); 
                    case DbType.Int16  : return Convert.ToInt16( source );
                    case DbType.Int32  : return Convert.ToInt32( source );
                    case DbType.Int64  : return Convert.ToInt64( source );
                    case DbType.Single : return Convert.ToSingle( source );
                    case DbType.Double : return Convert.ToDouble( source );
                    case DbType.Decimal: return Convert.ToDecimal( source );
                    default            :
                        throw new NotImplementedException( $"Defaults not supported for type {column.DbDataType }" );
                }
            }
        }

        public static object GetValueFromString( Type type, string source )
        {
            if ( type == typeof(Guid) )
            {
                return Guid.Parse( source );
            }
            switch ( Type.GetTypeCode(type) )
            {
                case TypeCode.Byte   : return Convert.ToByte( source ); 
                case TypeCode.Int16  : return Convert.ToInt16( source );
                case TypeCode.Int32  : return Convert.ToInt32( source );
                case TypeCode.Int64  : return Convert.ToInt64( source );
                case TypeCode.Single : return Convert.ToSingle( source );
                case TypeCode.Double : return Convert.ToDouble( source );
                case TypeCode.Decimal: return Convert.ToDecimal( source );
                case TypeCode.String : return source;
                default:
                    throw new NotImplementedException( $"Defaults not supported for type { type }" );
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

        public static object GetValueFromParameter( string value, DbParameterCollection parameters )
        {
            var name = value.TrimStart( new []{'@'} );
            if ( parameters.Contains( name ) == false )
            {
                throw new SqlInvalidParameterNameException( value );
            }

            var parameter = parameters[ name ];
            return parameter.Value;
        }

        public static TableColumn GetTableColumn( SqlColumnRefExpression expression, ExecuteSelectStatement.RawData rawData )
        {
            var list = new List<TableColumn>( );
            var columnName = GetColumnName( expression );
            foreach ( var row in rawData.TableRows )
            {
                foreach ( var tableRow in row )
                {
                    var column = tableRow.Table.Columns.FirstOrDefault( c => c.Name == columnName );
                    if ( column != null )
                    {
                        list.Add( new TableColumn{ TableName = tableRow.Name, Column = column } );
                    }
                }
            }
            return list.First();
        }

        public static object GetValue( SqlScalarExpression expression, Type type, ExecuteSelectStatement.RawData rawData, List<ExecuteSelectStatement.RawData.RawDataRow> row )
        {
            switch ( expression )
            {
                case SqlColumnRefExpression columnRef :
                    if ( row == null )
                    {
                        return null;
                    }
                    var field = GetTableColumn( columnRef, rawData );
                    return new SelectDataFromColumn( field ).Select( row );

                case SqlLiteralExpression literalExpression : 
                    return GetValueFromString( type, literalExpression.Value ); 

                case SqlScalarVariableRefExpression variableRef:
                    return GetValueFromParameter( variableRef.VariableName, rawData.Parameters );

                default:
                    throw new NotImplementedException( );
            }
        }
    }
}
