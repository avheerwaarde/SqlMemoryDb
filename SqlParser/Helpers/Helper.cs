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

        private static Dictionary< string, string> _DbType2SqlType = new Dictionary<string, string>
        {
            [DbType.AnsiString.ToString()           ] = "VARCHAR(MAX)",
            [DbType.Binary.ToString()               ] = "VARBINARY(MAX)",
            [DbType.Byte.ToString()                 ] = "BYTE",
            [DbType.Boolean.ToString()              ] = "BIT",
            [DbType.Currency.ToString()             ] = "MONEY",
            [DbType.Date.ToString()                 ] = "DATE",
            [DbType.DateTime.ToString()             ] = "DATETIME",
            [DbType.Decimal.ToString()              ] = "NUMERIC",
            [DbType.Double.ToString()               ] = "FLOAT",
            [DbType.Guid.ToString()                 ] = "GUID",
            [DbType.Int16.ToString()                ] = "SMALLINT",
            [DbType.Int32.ToString()                ] = "INT",
            [DbType.Int64.ToString()                ] = "BIGINT",
            [DbType.Object.ToString()               ] = "",
            [DbType.SByte.ToString()                ] = "CHAR",
            [DbType.Single.ToString()               ] = "REAL",
            [DbType.String.ToString()               ] = "NVARCHAR(MAX)",
            [DbType.Time.ToString()                 ] = "TIME",
            [DbType.UInt16.ToString()               ] = "SMALLINT",
            [DbType.UInt32.ToString()               ] = "INT",
            [DbType.UInt64.ToString()               ] = "BIGINT",
            [DbType.VarNumeric.ToString()           ] = "NUMERIC",
            [DbType.AnsiStringFixedLength.ToString()] = "CHAR(MAX)",
            [DbType.StringFixedLength.ToString()    ] = "VARCHAR(MAX)",
            [DbType.Xml.ToString()                  ] = "XML",
            [DbType.DateTime2.ToString()            ] = "DATETIME2",
            [DbType.DateTimeOffset.ToString()       ] = "DATETIMEOFFSETT"
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
                case TypeCode.Single  : return float.Parse( source, CultureInfo.InvariantCulture  );
                case TypeCode.Double  : return double.Parse( source, CultureInfo.InvariantCulture );
                case TypeCode.Decimal : return decimal.Parse( source , CultureInfo.InvariantCulture);
                case TypeCode.String  : return source;
                case TypeCode.DateTime: return DateTime.Parse( source, CultureInfo.InvariantCulture  );
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

        public static object GetValueFromParameter( string value, DbParameterCollection parameters, DbParameterCollection variables )
        {
            var name = value.TrimStart( new []{'@'} );
            if ( parameters.Contains( name ) )
            {
                var parameter = parameters[ name ];
                return parameter.Value;
            }
            else if ( variables.Contains( value ) )
            {
                var variable = variables[ value ];
                return variable.Value;
            }

            throw new SqlInvalidParameterNameException( value );
        }

        public static MemoryDbParameter GetParameter( string name, DbParameterCollection parameters )
        {
            if ( parameters.Contains( name ) == false )
            {
                throw new SqlInvalidParameterNameException( name );
            }

            return (MemoryDbParameter)parameters[ name ];
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
                    var select = new SelectDataFromColumn( field, rawData );
                    return GetReturnValue( select, row );
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
                    return GetValueFromParameter( variableRef.VariableName, rawData.Parameters, rawData.Command.Variables );
                }

                case SqlScalarRefExpression scalarRef:
                {
                    var field = GetTableColumn( (SqlObjectIdentifier)scalarRef.MultipartIdentifier, rawData );
                    var select = new SelectDataFromColumn( field, rawData );
                    return GetReturnValue( select, row );
                }
                case SqlGlobalScalarVariableRefExpression globalRef:
                {
                    var select = new SelectDataFromGlobalVariables( globalRef.VariableName, rawData );
                    return GetReturnValue( select, row );
                }

                case SqlBuiltinScalarFunctionCallExpression functionCall:
                {
                    var select = new SelectDataBuilder(  ).Build( functionCall, rawData );
                    return GetReturnValue( select, row );
                }

                case SqlSearchedCaseExpression caseExpression:
                {
                    var select = new SelectDataFromCaseExpression( caseExpression, rawData );
                    return GetReturnValue( select, row );
                }

                case SqlScalarSubQueryExpression subQuery:
                {
                    var database = new MemoryDbConnection().GetMemoryDatabase( );
                    var command = new MemoryDbCommand( rawData.Command.Connection, rawData.Command.Parameters, rawData.Command.Variables );
                    return database.ExecuteSqlScalar( subQuery.QueryExpression.Sql, command );
                }

                case SqlBinaryScalarExpression binaryScalarExpression:
                {
                    var select = new SelectDataFromBinaryScalarExpression( binaryScalarExpression, rawData );
                    return GetReturnValue( select, row );
                }
                default:
                    throw new NotImplementedException( $"Unsupported scalarExpression : '{ expression.GetType(  ) }'" );
            }
        }

        private static object GetReturnValue( ISelectData select, List<RawData.RawDataRow> row )
        {
            var value = select.Select( row );
            return IsParentUnaryNegate( select.Expression ) ? HelperReflection.Negate( value ) : value;
        }

        public static bool IsParentUnaryNegate( SqlScalarExpression expression )
        {
            return expression?.Parent is SqlUnaryScalarExpression unaryScalarExpression &&
                   unaryScalarExpression.Operator == SqlUnaryScalarOperatorType.Negative;
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

        public static MemoryDbDataReader.ReaderFieldData BuildFieldFromLiteral( LiteralValueType literalType, string name, int fieldsCount )
        {
            var readerField = new MemoryDbDataReader.ReaderFieldData
            {
                Name = name,
                DbType = GetDbTypeFromLiteralType( literalType ).Value.ToString(),
                NetType = GetTypeFromLiteralType( literalType ),
                FieldIndex = fieldsCount
            };
            return readerField;
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
            if (  expression is SqlUnaryScalarExpression unaryScalarExpression )
            {
                expression = unaryScalarExpression.Expression;
            }
            switch ( expression )
            {
                case SqlColumnRefExpression columnRef:
                {
                    var field = Helper.GetTableColumn( columnRef, rawData );
                    return field.Column.NetDataType;
                }
                case SqlScalarVariableRefExpression variableRef:
                {
                    var parameter = GetParameter( variableRef.VariableName, rawData.Command.Variables );
                    return parameter.NetDataType;
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
                case SqlLiteralExpression literalExpression:
                {
                    return GetTypeFromLiteralType( literalExpression.Type );
                }
                case SqlBinaryScalarExpression binaryScalarExpression:
                {
                    var selectFunction = new SelectDataFromBinaryScalarExpression( binaryScalarExpression, rawData );
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

        public static Type GetTypeFromLiteralType( LiteralValueType literalExpressionType )
        {
            switch ( literalExpressionType )
            {
                case LiteralValueType.Binary: return typeof(byte[]);
                case LiteralValueType.Identifier: return typeof(int);
                case LiteralValueType.Integer: return typeof(int);
                case LiteralValueType.Image: return typeof(byte[]);
                case LiteralValueType.Money: return typeof(decimal);
                case LiteralValueType.Null: return typeof(Object);
                case LiteralValueType.Numeric: return typeof(double);
                case LiteralValueType.Real: return typeof(float);
                case LiteralValueType.Default: 
                case LiteralValueType.String:
                case LiteralValueType.UnicodeString:
                default:
                    return typeof(string);
            }
        }

        public static DbType? GetDbTypeFromLiteralType( LiteralValueType literalExpressionType )
        {
            switch ( literalExpressionType )
            {
                case LiteralValueType.Binary: return DbType.Binary;
                case LiteralValueType.Identifier: return DbType.Int32;
                case LiteralValueType.Integer: return DbType.Int32;
                case LiteralValueType.Image: return DbType.Binary;
                case LiteralValueType.Money: return DbType.Decimal;
                case LiteralValueType.Null: return DbType.Object;
                case LiteralValueType.Numeric: return DbType.Decimal;
                case LiteralValueType.Real: return DbType.Single;
                case LiteralValueType.Default: 
                case LiteralValueType.String:
                case LiteralValueType.UnicodeString:
                default:
                    return DbType.String;
            }
        }

        public static bool IsTempTable( SqlObjectIdentifier intoTarget )
        {
            return IsTempTable( intoTarget.ObjectName.Value );
        }

        public static bool IsTempTable( string tableName )
        {
            return tableName[ 0 ] == '#';
        }

        public static  bool IsLocalTempTable( SqlObjectIdentifier intoTarget )
        {
            return IsLocalTempTable( intoTarget.ObjectName.Value );
        }

        public static  bool IsLocalTempTable( string tableName )
        {
            return tableName.Length > 1 && tableName[ 0 ] == '#' && tableName[ 1 ] != '#';
        }

        public static  bool IsGlobalTempTable( SqlObjectIdentifier intoTarget )
        {
            return IsGlobalTempTable( intoTarget.ObjectName.Value );
        }

        public static  bool IsGlobalTempTable( string tableName )
        {
            return tableName.Length > 2 && tableName[ 0 ] == '#' && tableName[ 1 ] == '#';
        }

        public static Table GetTableFromObjectId( SqlObjectIdentifier identifier, Dictionary<string, Table> tables, Dictionary<string, Table> tempTables, bool throwException = true )
        {
            var tableName = Helper.GetQualifiedName( identifier );
            if ( tempTables.ContainsKey( tableName ) )
            {
                return tempTables[ tableName ];
            }
            
            if ( tables.ContainsKey( tableName ) )
            {
                return tables[ tableName ];
            }

            var foundTables = tables.Where( t => t.Value.Name == identifier.ObjectName.Value ).ToList( );
            if ( foundTables.Count( ) != 1 )
            {
                if ( foundTables.Count == 0 && throwException == false )
                {
                    return null;
                }
                throw new SqlInvalidTableNameException( tableName );
            }

            return foundTables.First( ).Value;
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

        public static Column FindColumn( TableAndColumn tableAndColumn, string columnName, Dictionary<string, Table> tables )
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
                    var parameterName = variableRef.VariableName.TrimStart( new []{'@'} );
                    if ( command.Parameters.Contains( parameterName ) )
                    {
                        return ( MemoryDbParameter ) command.Parameters[ parameterName ];
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

        public static string DbType2SqlType( string dbType )
        {
            return _DbType2SqlType[ dbType ];
        }
    }
}
