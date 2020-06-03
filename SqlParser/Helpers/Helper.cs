using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Exceptions;
using SqlMemoryDb.SelectData;
using SqlParser;

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

        public static string GetColumnName( SqlScalarRefExpression expression )
        {
            return ((SqlObjectIdentifier)expression.MultipartIdentifier).ObjectName.Value;
        }

        public static string GetColumnAlias( SqlSelectScalarExpression scalarExpression )
        {
            return scalarExpression.Alias != null
                ? scalarExpression.Alias.Value
                : GetColumnName( ( SqlScalarRefExpression)scalarExpression.Expression );
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

        public static TableColumn GetTableColumn( SqlObjectIdentifier objectIdentifier, ExecuteSelectStatement.RawData rawData )
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

        public static object GetValue( SqlScalarExpression expression, Type type, ExecuteSelectStatement.RawData rawData, List<ExecuteSelectStatement.RawData.RawDataRow> row )
        {
            switch ( expression )
            {
                case SqlColumnRefExpression columnRef:
                {
                    var field = GetTableColumn( columnRef, rawData );
                    return new SelectDataFromColumn( field ).Select( row );
                }

                case SqlLiteralExpression literal : 
                    return GetValueFromString( type, literal.Value ); 

                case SqlScalarVariableRefExpression variableRef:
                    return GetValueFromParameter( variableRef.VariableName, rawData.Parameters );

                case SqlScalarRefExpression scalarRef:
                {
                    var field = GetTableColumn( (SqlObjectIdentifier)scalarRef.MultipartIdentifier, rawData );
                    return new SelectDataFromColumn( field ).Select( row );
                }

                default:
                    throw new NotImplementedException( );
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

        public static Type DetermineType( SqlScalarExpression expressionLeft, SqlScalarExpression expressionRight, ExecuteSelectStatement.RawData rawData )
        {
            return DetermineType( expressionLeft, rawData ) ?? DetermineType( expressionRight, rawData );
        }

        public static Type DetermineType( SqlScalarExpression expression, ExecuteSelectStatement.RawData rawData )
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

            }

            return null;
        }

        public static IRowFilter GetRowFilter( SqlBooleanExpression booleanExpression, ExecuteSelectStatement.RawData rawData )
        {
            switch ( booleanExpression )
            {
                case SqlComparisonBooleanExpression compareExpression: return new RowFilterComparison( rawData, compareExpression );
                case SqlBinaryBooleanExpression binaryExpression     : return new RowFilterBinary( rawData, binaryExpression );
                default :
                    throw new NotImplementedException();
            }
        }

        public static bool IsTrue( SqlBooleanOperatorType booleanOperator, bool leftIsValid, bool rightIsValid )
        {
            if ( booleanOperator == SqlBooleanOperatorType.Or )
            {
                return leftIsValid || rightIsValid;
            }
            return leftIsValid && rightIsValid;
        }

        public static bool IsPredicateCorrect( object left, object right, SqlComparisonBooleanExpressionType comparisonOperator )
        {
            var comparison = ( ( IComparable ) left ).CompareTo( ( IComparable ) right );

            switch ( comparisonOperator )
            {
                case SqlComparisonBooleanExpressionType.Equals: return comparison == 0;
                case SqlComparisonBooleanExpressionType.LessThan: return comparison < 0;
                case SqlComparisonBooleanExpressionType.ValueEqual: return comparison == 0;
                case SqlComparisonBooleanExpressionType.NotEqual: return comparison != 0;
                case SqlComparisonBooleanExpressionType.GreaterThan: return comparison > 0;
                case SqlComparisonBooleanExpressionType.GreaterThanOrEqual: return comparison >= 0;
                case SqlComparisonBooleanExpressionType.LessOrGreaterThan: return comparison != 0;
                case SqlComparisonBooleanExpressionType.LessThanOrEqual: return comparison <= 0;
                case SqlComparisonBooleanExpressionType.NotLessThan: return comparison >= 0;
                case SqlComparisonBooleanExpressionType.NotGreaterThan: return comparison <= 0;
                default:
                    throw new NotImplementedException();
            }

        }
    }
}
