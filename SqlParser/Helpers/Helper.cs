using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Text;
using Generic.Math;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using MiscUtil;
using SqlMemoryDb.Exceptions;
using SqlMemoryDb.SelectData;
using SqlParser;

namespace SqlMemoryDb.Helpers
{
    class Helper
    {
        public static string DefaultSchemaName = "dbo";
        private static Dictionary< string, string[]> _DateFormats = new Dictionary<string, string[]>
        {
            {"mdy", new []{ "M/d/yyyy h:mm:ss tt", "M-d-yyyy h:mm:ss tt", "M/d/yyyy h:mm:ss", "M-d-yyyy h:mm:ss", "M/d/yyyy", "M-d-yyyy" } },
            {"dmy", new []{ "d/M/yyyy h:mm:ss tt", "d-M-yyyy h:mm:ss tt", "d/M/yyyy h:mm:ss", "d-M-yyyy h:mm:ss", "d/M/yyyy", "d-M-yyyy" } },
            {"ymd", new []{ "yyyy/M/d h:mm:ss tt", "yyyy-M-d h:mm:ss tt", "yyyy/M/d h:mm:ss", "yyyy-M-d h:mm:ss", "yyyy/M/d", "yyyy-M-d" } },
            {"ydm", new []{ "yyyy/d/M h:mm:ss tt", "yyyy-d-M h:mm:ss tt", "yyyy/d/M h:mm:ss", "yyyy-d-M h:mm:ss", "yyyy/d/M", "yyyy-d-M" } },
            {"myd", new []{ "M/yyyy/d h:mm:ss tt", "M-yyyy-d h:mm:ss tt", "M/yyyy/d h:mm:ss", "M-yyyy-d h:mm:ss", "M/yyyy/d", "M-yyyy-d" } },
            {"dym", new []{ "d/yyyy/M h:mm:ss tt", "d-yyyy-M h:mm:ss tt", "d/yyyy/M h:mm:ss", "d-yyyy-M h:mm:ss", "d/yyyy/M", "d-yyyy-M" } }
        };

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
                if ( source.ToUpper() == "NULL" )
                {
                    return null;
                }
                switch ( column.DbDataType )
                {
                    case DbType.Boolean   : return source.ToUpper( ) == "TRUE" || source == "1";
                    case DbType.Byte      : return Convert.ToByte( source ); 
                    case DbType.Int16     : return Convert.ToInt16( source );
                    case DbType.Int32     : return Convert.ToInt32( source );
                    case DbType.Int64     : return Convert.ToInt64( source );
                    case DbType.Single    : return Single.Parse( source, CultureInfo.InvariantCulture );
                    case DbType.Double    : return Double.Parse( source, CultureInfo.InvariantCulture );
                    case DbType.Decimal   : return Convert.ToDecimal( source );
                    case DbType.Guid      : return Guid.Parse( GetStringValue( source ) );
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

            source = GetStringValue( source );
            var formatKey = new MemoryDbConnection().GetMemoryDatabase( ).Options["DATEFORMAT"];
            var dateFormat = _DateFormats[ formatKey ];
            return ToDate(source, dateFormat ).Value;
        }

        private static DateTime? ToDate( string dateTimeStr, params string[] dateFmt)
        {
            // example: var dt = "2011-03-21 13:26".ToDate(new string[]{"yyyy-MM-dd HH:mm", 
            //                                                  "M/d/yyyy h:mm:ss tt"});
            // or simpler: 
            // var dt = "2011-03-21 13:26".ToDate("yyyy-MM-dd HH:mm", "M/d/yyyy h:mm:ss tt");
            const DateTimeStyles style = DateTimeStyles.AllowWhiteSpaces;
            if (dateFmt == null)
            {
                var dateInfo = System.Threading.Thread.CurrentThread.CurrentCulture.DateTimeFormat;
                dateFmt=dateInfo.GetAllDateTimePatterns();
            }
            // Commented out below because it can be done shorter as shown below.
            // For older C# versions (older than C#7) you need it like that:
            // DateTime? result = null;
            // DateTime dt;
            // if (DateTime.TryParseExact(dateTimeStr, dateFmt,
            //    CultureInfo.InvariantCulture, style, out dt)) result = dt;
            // In C#7 and above, we can simply write:
            var result = DateTime.TryParseExact(dateTimeStr, dateFmt, CultureInfo.InvariantCulture,
                style, out var dt) ? dt : null as DateTime?;
            return result;
        }

        public static object GetValueFromString( Type type, string source )
        {
            if ( type == typeof(Guid) )
            {
                return Guid.Parse( source );
            }
            switch ( Type.GetTypeCode(type) )
            {
                case TypeCode.Boolean : return source.ToUpper( ) == "TRUE" || source == "1";
                case TypeCode.Byte    : return Convert.ToByte( source ); 
                case TypeCode.Int16   : return Convert.ToInt16( source );
                case TypeCode.Int32   : return Convert.ToInt32( source );
                case TypeCode.Int64   : return Convert.ToInt64( source );
                case TypeCode.Single  : return Convert.ToSingle( source );
                case TypeCode.Double  : return double.Parse( source, CultureInfo.InvariantCulture );
                case TypeCode.Decimal : return Convert.ToDecimal( source );
                case TypeCode.String  : return source;
                case TypeCode.DateTime: return DateTime.Parse( source );
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


        public static object GetValue( SqlScalarExpression expression, Type type, RawData rawData, List<RawData.RawDataRow> row, bool getTypeFromLiteral = false )
        {
            switch ( expression )
            {
                case SqlColumnRefExpression columnRef:
                {
                    var field = GetTableColumn( columnRef, rawData );
                    return new SelectDataFromColumn( field ).Select( row );
                }

                case SqlUnaryScalarExpression unaryScalarExpression:
                {
                    var value = GetValue( unaryScalarExpression.Expression, type, rawData, row );
                    if ( unaryScalarExpression.Operator == SqlUnaryScalarOperatorType.Negative )
                    {
                        value = HelperReflection.Negate( value );
                    }
                    return value;
                }

                case SqlLiteralExpression literal:
                {
                    if ( literal.Type == LiteralValueType.Null )
                    {
                        return null;
                    }

                    var literalType = getTypeFromLiteral ? GetTypeFromLiteralType( literal.Type ) : type;
                    return GetValueFromString( literalType, literal.Value );
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

                case SqlSearchedCaseExpression caseExpression:
                {
                    var select = new SelectDataFromCaseExpression( caseExpression, rawData );
                    return select.Select( row );
                }

                case SqlScalarSubQueryExpression subQuery:
                {
                    var database = new MemoryDbConnection().GetMemoryDatabase( );
                    var command = new MemoryDbCommand( rawData.Command.Connection, rawData.Command.Parameters, rawData.Command.Variables );
                    return database.ExecuteSqlScalar( subQuery.QueryExpression.Sql, command );
                }

                default:
                    throw new NotImplementedException( $"Unsupported scalarExpression : '{ expression.GetType(  ) }'" );
            }
        }

        public static object GetValue( SqlScalarExpression expression, MemoryDbDataReader.ResultBatch batch, ArrayList row )
        {
            switch ( expression )
            {
                case SqlColumnRefExpression columnRef:
                {
                    var name = GetColumnName( columnRef );
                    var field = batch.Fields.FirstOrDefault( f => f.Name == name );
                    if ( field == null )
                    {
                        throw new SqlInvalidColumnNameException( name );
                    }

                    return row[ field.FieldIndex ];
                }

                case SqlLiteralExpression literal:
                {
                    if ( literal.Type == LiteralValueType.Integer )
                    {
                        var fieldIndex = int.Parse( literal.Value );
                        return row[ fieldIndex ];
                    }
                    throw new NotImplementedException( $"Unsupported scalarExpression : '{ expression.GetType(  ) }'" );
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
                    var selectFunction = new SelectDataBuilder(  ).Build( functionCall, rawData ) as ISelectDataAggregate;
                    return selectFunction.Select( rows );
                default:
                    return GetValue( expression, type, rawData, rows.First( ) );
            }
        }

        public static MemoryDbDataReader.ReaderFieldData BuildFieldFromNullValue( string name, int fieldsCount )
        {
            return new MemoryDbDataReader.ReaderFieldData
            {
                Name = name,
                DbType = DbType.Object.ToString(),
                NetType = typeof(object),
                FieldIndex = fieldsCount,
                SelectFieldData = new SelectDataFromObject( null, DbType.Object.ToString() )
            };
        }

        public static MemoryDbDataReader.ReaderFieldData BuildFieldFromStringValue( string literal, string name, int fieldsCount )
        {
            var readerField = new MemoryDbDataReader.ReaderFieldData
            {
                Name = name,
                DbType = DbType.String.ToString(),
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
                case SqlSearchedCaseExpression caseExpression:
                {
                    var selectFunction =  new SelectDataFromCaseExpression( caseExpression, rawData );
                    return selectFunction.ReturnType;
                }
            }

            return null;
        }


        public static FullTypeInfo DetermineFullTypeInfo( SqlScalarExpression expression, RawData rawData )
        {
            switch ( expression )
            {
                case SqlColumnRefExpression columnRef:
                {
                    var field = Helper.GetTableColumn( columnRef, rawData );
                    return new FullTypeInfo { DbDataType = field.Column.DbDataType, NetDataType = field.Column.NetDataType };
                }
                case SqlScalarVariableRefExpression variableRef:
                {
//                    Helper.GetValueFromParameter( variableRef.VariableName, rawData.Parameters );
                    return null;
                }
                case SqlScalarRefExpression scalarRef:
                {
                    var field = Helper.GetTableColumn( (SqlObjectIdentifier)scalarRef.MultipartIdentifier, rawData );
                    return new FullTypeInfo { DbDataType = field.Column.DbDataType, NetDataType = field.Column.NetDataType };
                }
                case SqlAggregateFunctionCallExpression functionCall:
                {
                    var selectFunction = new SelectDataBuilder(  ).Build( functionCall, rawData );
                    return new FullTypeInfo { DbDataType = selectFunction.DbType, NetDataType = selectFunction.ReturnType };
                }
                case SqlSearchedCaseExpression caseExpression:
                {
                    var selectFunction =  new SelectDataFromCaseExpression( caseExpression, rawData );
                    return new FullTypeInfo { DbDataType = selectFunction.DbType, NetDataType = selectFunction.ReturnType };
                }
                case SqlLiteralExpression literalExpression:
                {
                    return new FullTypeInfo { DbDataType = GetDbTypeFromLiteralType( literalExpression.Type ), NetDataType = GetTypeFromLiteralType( literalExpression.Type ) };
                }
            }

            return null;
        }

        private static Type GetTypeFromLiteralType( LiteralValueType literalExpressionType )
        {
            switch ( literalExpressionType )
            {
                case LiteralValueType.Binary: return typeof(byte[]);
                case LiteralValueType.Identifier: return typeof(int);
                case LiteralValueType.Integer: return typeof(int);
                case LiteralValueType.Image: return typeof(byte[]);
                case LiteralValueType.Money: return typeof(decimal);
                case LiteralValueType.Null: return typeof(string);
                case LiteralValueType.Numeric: return typeof(double);
                case LiteralValueType.Real: return typeof(float);
                case LiteralValueType.Default: 
                case LiteralValueType.String:
                case LiteralValueType.UnicodeString:
                default:
                    return typeof(string);
            }
        }

        private static DbType? GetDbTypeFromLiteralType( LiteralValueType literalExpressionType )
        {
            switch ( literalExpressionType )
            {
                case LiteralValueType.Binary: return DbType.Binary;
                case LiteralValueType.Identifier: return DbType.Int32;
                case LiteralValueType.Integer: return DbType.Int32;
                case LiteralValueType.Image: return DbType.Binary;
                case LiteralValueType.Money: return DbType.Decimal;
                case LiteralValueType.Null: return null;
                case LiteralValueType.Numeric: return DbType.Decimal;
                case LiteralValueType.Real: return DbType.Single;
                case LiteralValueType.Default: 
                case LiteralValueType.String:
                case LiteralValueType.UnicodeString:
                default:
                    return DbType.String;
            }
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

        public static string GetLikeRegEx( string pattern )
        {
            var patternBuilder = new StringBuilder();
            if ( pattern.StartsWith( "%" ) == false )
            {
                patternBuilder.Append( "^" );
            }

            foreach ( var character in pattern )
            {
                switch ( character )
                {
                    case '%': patternBuilder.Append( ".*" ); break;
                    case '_': patternBuilder.Append( ".{1}" ); break;
                    default:
                        patternBuilder.Append( character );
                        break;
                }
            }
            if ( pattern.EndsWith( "%" ) == false )
            {
                patternBuilder.Append( "$" );
            }
            return patternBuilder.ToString();
        }

    }
}
