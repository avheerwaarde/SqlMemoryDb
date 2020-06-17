using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Exceptions;
using SqlMemoryDb.SelectData;
using SqlParser;

namespace SqlMemoryDb.Helpers
{
    class Helper
    {
        public static string DefaultSchemaName = "dbo";

        public static string GetAliasName( SqlTableRefExpression tableRef )
        {
            return tableRef.Alias == null ? GetQualifiedName(tableRef.ObjectIdentifier) : tableRef.Alias.Value;
        }

        public static string GetQualifiedName( SqlObjectIdentifier identifier )
        {
            return (identifier.SchemaName.Value ?? DefaultSchemaName ) + "." + identifier.ObjectName;
        }

        public static string GetColumnName( SqlScalarRefExpression expression )
        {
            return ((SqlObjectIdentifier)expression.MultipartIdentifier).ObjectName.Value;
        }

        public static string GetColumnAlias( SqlSelectScalarExpression scalarExpression )
        {
            return scalarExpression.Alias != null
                ? scalarExpression.Alias.Value
                : scalarExpression.Expression is SqlScalarRefExpression expression ? GetColumnName( expression ) : "";
        }

        public static object GetValueFromString( Column column, string source )
        {
            if ( column.NetDataType == typeof(string) )
            {
                source = GetStringValue( source );
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
                    case DbType.Boolean   : return source.ToUpper( ) == "TRUE" || source == "1";
                    case DbType.Byte      : return Convert.ToByte( source ); 
                    case DbType.Int16     : return Convert.ToInt16( source );
                    case DbType.Int32     : return Convert.ToInt32( source );
                    case DbType.Int64     : return Convert.ToInt64( source );
                    case DbType.Single    : return Convert.ToSingle( source );
                    case DbType.Double    : return Convert.ToDouble( source );
                    case DbType.Decimal   : return Convert.ToDecimal( source );
                    case DbType.Guid      : return Guid.Parse( source );
                    case DbType.Date      : 
                    case DbType.DateTime  : 
                    case DbType.DateTime2 : return GetValueFromDateString( source );
                    case DbType.Binary    : return ConvertHexStringToByteArray( source );
                    default               :
                        throw new NotImplementedException( $"Defaults not supported for type {column.DbDataType }" );
                }
            }
        }

        public static byte[] ConvertHexStringToByteArray(string hexString)
        {
            if ( hexString.StartsWith( "0x" ) == false )
            {
                throw new ArgumentException("The string must start with 0x");
            }
            if (hexString.Length % 2 != 0)
            {
                throw new ArgumentException(String.Format(CultureInfo.InvariantCulture, "The binary key cannot have an odd number of digits: {0}", hexString));
            }

            byte[] data = new byte[hexString.Length / 2];
            for (int index = 0; index < (data.Length-1); index++)
            {
                string byteValue = hexString.Substring((index+1) * 2, 2);
                data[index] = byte.Parse(byteValue, NumberStyles.HexNumber, CultureInfo.InvariantCulture);
            }

            return data; 
        }

        private static DateTime GetValueFromDateString( string source )
        {
            if ( source.ToUpper() == "CURRENT_TIMESTAMP" )
            {
                return DateTime.Now;
            }
            return DateTime.Parse( source );
        }

        public static object GetValueFromString( Type type, string source )
        {
            if ( type == typeof(Guid) )
            {
                return Guid.Parse( source );
            }
            switch ( Type.GetTypeCode(type) )
            {
                case TypeCode.Boolean: return source.ToUpper( ) == "TRUE" || source == "1";
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

        public static string CleanName( string name )
        {
            if ( name == null )
            {
                name = "";
            }
            else if ( name.StartsWith( "\"" ) && name.EndsWith( "\"" )  )
            {
                name = name.Substring( 1, name.Length - 2 );
            }

            return name;
        }

        public static string CleanSql( string sourceSql )
        {
            return sourceSql.Replace( '\n', ' ' ).Replace( '\r', ' ' ).Replace( '\t', ' ' ).Trim( );
        }

        public static string GetStringValue( string part )
        {
            if ( part.StartsWith( "N'" ) && part.EndsWith( "'" ) )
            {
                return part.Substring( 2, part.Length - 3 );
            }
            else if ( part.StartsWith( "'" ) && part.EndsWith( "'" ))
            {
                return part.Substring( 1, part.Length - 2 );
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

        public static TableColumn GetTableColumn( SqlColumnRefExpression expression, RawData rawData )
        {
            var columnName = GetColumnName( expression );
            var tc = FindTableAndColumn( null, columnName, rawData.TableAliasList );
            return new TableColumn{ TableName = tc.TableName, Column = tc.Column };
        }

        public static TableColumn GetTableColumn( SqlObjectIdentifier objectIdentifier, RawData rawData )
        {
            Table table;
            var tableAlias = objectIdentifier.SchemaName.Value;
            if ( objectIdentifier.SchemaName.Value == null )
            {
                var tables = rawData.TableAliasList
                    .Where( t => t.Value.Columns.Any( c => c.Name == objectIdentifier.ObjectName.Value ) )
                    .ToList(  );
                if ( tables.Count == 0 )
                {
                    throw new SqlInvalidColumnNameException( objectIdentifier.ObjectName.Value );
                }
                if ( tables.Count > 1 )
                {
                    throw new SqlUnqualifiedColumnNameException( objectIdentifier.ObjectName.Value );
                }

                var tableEntry = tables.Single( );
                table = tableEntry.Value;
                tableAlias = tableEntry.Key;
            }
            else
            {
                if ( rawData.TableAliasList.ContainsKey( tableAlias ) )
                {
                    table = rawData.TableAliasList[ tableAlias ];
                }
                else
                {
                    var tableEntry = rawData.TableAliasList.Single( t => t.Value.FullName == tableAlias || t.Value.Name == tableAlias );
                    table = tableEntry.Value;
                    tableAlias = tableEntry.Key;
                }
            }

            var column = table.Columns.Single( c => c.Name == objectIdentifier.ObjectName.Value );
            return new TableColumn {TableName = tableAlias, Column = column};
        }

        public static object GetValue( SqlScalarExpression expression, Type type, RawData rawData, List<RawData.RawDataRow> row )
        {
            switch ( expression )
            {
                case SqlColumnRefExpression columnRef:
                {
                    var field = GetTableColumn( columnRef, rawData );
                    return new SelectDataFromColumn( field ).Select( row );
                }

                case SqlLiteralExpression literal:
                {
                    return GetValueFromString( type, literal.Value );
                }

                case SqlScalarVariableRefExpression variableRef:
                {
                    return GetValueFromParameter( variableRef.VariableName, rawData.Parameters );
                }

                case SqlScalarRefExpression scalarRef:
                {
                    var field = GetTableColumn( (SqlObjectIdentifier)scalarRef.MultipartIdentifier, rawData );
                    return new SelectDataFromColumn( field ).Select( row );
                }
                case SqlGlobalScalarVariableRefExpression globalRef:
                {
                    return new SelectDataFromGlobalVariables( globalRef.VariableName, rawData ).Select( row );
                }

                case SqlBuiltinScalarFunctionCallExpression functionCall:
                {
                    var select = new SelectDataBuilder(  ).Build( functionCall, rawData );
                    return select.Select( row );
                }

                case SqlScalarSubQueryExpression subQuery:
                {
                    var database = MemoryDbConnection.GetMemoryDatabase( );
                    var command = new MemoryDbCommand( rawData.Command.Connection, rawData.Command.Parameters, rawData.Command.Variables );
                    return database.ExecuteSqlScalar( subQuery.QueryExpression.Sql, command );
                }

                default:
                    throw new NotImplementedException( $"Unsupported scalarExpression : '{ expression.GetType(  ) }'" );
            }
        }

        public static object GetValue( SqlScalarExpression expression, Type type, RawData rawData,
            List<List<RawData.RawDataRow>> rows )
        {
            switch ( expression )
            {
                case SqlAggregateFunctionCallExpression functionCall:
                    var selectFunction = new SelectDataBuilder(  ).Build( functionCall, rawData );
                    return selectFunction.Select( rows );
                default:
                    return GetValue( expression, type, rawData, rows.First( ) );
            }
        }

        public static MemoryDbDataReader.ReaderFieldData BuildFieldFromStringValue( string literal, string name, int fieldsCount )
        {
            var readerField = new MemoryDbDataReader.ReaderFieldData
            {
                Name = name,
                DbType = "nvarchar",
                NetType = typeof(string),
                FieldIndex = fieldsCount
            };

            try
            {
                Guid guidValue;
                DateTime dateTimeValue;
                Int64 longValue;

                if ( literal.StartsWith( "N'" ) || literal.StartsWith( "'" ) )
                {
                    var val = GetStringValue( literal );
                    if ( Guid.TryParse( val, out guidValue  ) )
                    {
                        readerField.DbType = "Guid";
                        readerField.NetType = typeof( Guid );
                    }
                    else if ( DateTime.TryParse( val, out dateTimeValue ) )
                    {
                        readerField.DbType = "DateTime";
                        readerField.NetType = typeof( DateTime );
                    }
                }
                else
                {
                    if ( literal.ToLower() == "true" || literal.ToLower() == "false" )
                    {
                        readerField.DbType = "Boolean";
                        readerField.NetType = typeof( bool );
                    }
                    if ( Int64.TryParse( literal, out longValue ) )
                    {
                        if ( longValue >= Int32.MinValue && longValue <= Int32.MaxValue )
                        {
                            readerField.DbType = "Int32";
                            readerField.NetType = typeof( Int32 );
                        }
                        else
                        {
                            readerField.DbType = "Int64";
                            readerField.NetType = typeof( Int64 );
                        }
                    }                           
                }
            }
            catch ( Exception  )
            {
                // Ignore if we fail to determine the type, it will be a string then ;-)
            }
            return readerField;
        }

        public static Type DetermineType( SqlScalarExpression expressionLeft, SqlScalarExpression expressionRight, RawData rawData )
        {
            return DetermineType( expressionLeft, rawData ) ?? DetermineType( expressionRight, rawData );
        }

        public static Type DetermineType( SqlScalarExpression expression, RawData rawData )
        {
            switch ( expression )
            {
                case SqlColumnRefExpression columnRef:
                {
                    var field = Helper.GetTableColumn( columnRef, rawData );
                    return field.Column.NetDataType;
                }
                case SqlScalarVariableRefExpression variableRef:
                {
//                    Helper.GetValueFromParameter( variableRef.VariableName, rawData.Parameters );
                    return null;
                }
                case SqlScalarRefExpression scalarRef:
                {
                    var field = Helper.GetTableColumn( (SqlObjectIdentifier)scalarRef.MultipartIdentifier, rawData );
                    return field.Column.NetDataType;
                }
                case SqlAggregateFunctionCallExpression functionCall:
                {
                    var selectFunction = new SelectDataBuilder(  ).Build( functionCall, rawData );
                    return selectFunction.ReturnType;
                }
            }

            return null;
        }


        public static TableAndColumn FindTableAndColumn( string tableName, string columnName,
            Dictionary<string, Table> tables )
        {
            var result = FindTable( tableName, tables );
            result.Column = FindColumn( result, columnName, tables );
            return result;
        }

        internal static TableAndColumn FindTable( string tableName, Dictionary<string, Table> tables )
        {
            tableName = CleanName( tableName );

            var result = new TableAndColumn(  );
            if ( string.IsNullOrWhiteSpace( tableName ) == false )
            {
                if ( tables.ContainsKey( tableName ) )
                {
                    result.TableName = tableName;
                    result.Table = tables[ tableName ];
                    return result;
                }
                else
                {
                    var foundTables = tables.Where( t => t.Value.Name == tableName ).ToList( );
                    if ( foundTables.Count( ) != 1 )
                    {
                        throw new SqlInvalidTableNameException( tableName );
                    }

                    result.TableName = foundTables[0].Key;
                    result.Table = foundTables[0].Value;
                    return result;
                }
            }

            return result;
        }

        private static Column FindColumn( TableAndColumn tableAndColumn, string columnName, Dictionary<string, Table> tables )
        {
            columnName = CleanName( columnName );

            if ( tableAndColumn.Table == null )
            {
                var foundTables = tables.Where( t => t.Value.Columns.Any( c => c.Name == columnName ) ).ToList( );
                if ( foundTables.Count != 1 )
                {
                    throw new SqlUnqualifiedColumnNameException( columnName );
                }

                tableAndColumn.TableName = foundTables[0].Key;
                tableAndColumn.Table = foundTables[0].Value;
            }

            if ( tableAndColumn.Table.Columns.Any( c => c.Name == columnName) == false )
            {
                throw new SqlInvalidColumnNameException( columnName );
            }

            return tableAndColumn.Table.Columns.Single( c => c.Name == columnName );
        }

        public static MemoryDbParameter GetParameter( MemoryDbCommand command, SqlScalarExpression scalarExpression )
        {
            switch ( scalarExpression )
            {
                case SqlScalarVariableRefExpression variableRef:
                {
                    if ( command.Parameters.Contains( variableRef.VariableName ) )
                    {
                        return ( MemoryDbParameter ) command.Parameters[ variableRef.VariableName ];
                    }
                    if ( command.Variables.Contains( variableRef.VariableName ) )
                    {
                        return ( MemoryDbParameter ) command.Variables[ variableRef.VariableName ];
                    }
                    throw new SqlInvalidVariableException( variableRef.VariableName );
                }
            }
            throw new NotImplementedException();
        }
    }
}
