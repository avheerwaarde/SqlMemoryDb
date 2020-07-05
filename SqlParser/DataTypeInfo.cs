using System;
using System.Collections.Generic;
using System.Data;
using System.Text.RegularExpressions;

namespace SqlMemoryDb
{
    public class DataTypeInfo
    {
        public DbType DbDataType { get ; set ; }
        public Type NetDataType { get ; set ; }
        public int Precision;
        public int Scale;
        public int Size;
        public bool IsFixedSize { get ; set ; }

        private readonly Dictionary<string, Action<DataTypeInfo, string>> _DataTypes = new Dictionary<string, Action<DataTypeInfo, string>>
        {
            { "BIGINT"          , InitBigInt },
            { "BINARY"          , InitBinary },
            { "BIT"             , InitBit },
            { "CHAR"            , InitChar },
            { "DATE "           , InitDate  },
            { "DATETIME"        , InitDateTime},
            { "DATETIME2"       , InitDateTime2 },
            { "DATETIMEOFFSET"  , InitDateTimeOffset },
            { "DECIMAL"         , InitDecimal },
            { "FLOAT"           , InitFloat },
            { "IMAGE"           , InitVarBinary },
            { "INT"             , InitInt },
            { "MONEY"           , InitDecimal },
            { "NCHAR"           , InitNChar },
            { "NTEXT"           , InitNVarChar },
            { "NUMERIC"         , InitDecimal },
            { "NVARCHAR"        , InitNText },
            { "REAL"            , InitReal },
            { "ROWVERSION"      , InitTimeStamp },
            { "SMALLDATETIME"   , InitDateTime },
            { "SMALLINT"        , InitSmallInt },
            { "SMALLMONEY"      , InitDecimal },
            { "SQL_VARIANT"     , InitObject },
            { "TEXT"            , InitNVarChar },
            { "TIME"            , InitTime },
            { "TIMESTAMP"       , InitTimeStamp },
            { "TINYINT"         , InitTinyInt },
            { "UNIQUEIDENTIFIER", InitUniqueIdentifier},
            { "VARBINARY"       , InitVarBinary },
            { "VARCHAR"         , InitVarChar },
            { "XML"             , InitXml }
        };

        public DataTypeInfo( string sqlType )
        {
            if ( sqlType.StartsWith( "\"" ) && sqlType.EndsWith( "\"" )  )
            {
                sqlType = sqlType.Substring( 1, sqlType.Length - 2 );
            }
            var match = Regex.Match(sqlType.ToUpper(), @"^([^\(]+)?(\([^\)]+\))?");
            if (match.Success)
            {
                var dataTypeString = match.Groups[1].Value.Trim(new[] { '[', ']', ' ' });
                var sizeOrPrecision = match.Groups[2].Value;
                if ( _DataTypes.ContainsKey( dataTypeString ) == false )
                {
                    throw new NotImplementedException( $"DataType [{dataTypeString}] is not implemented");
                }
                _DataTypes[dataTypeString].Invoke( this, sizeOrPrecision );
            }
        }

        private static void InitBigInt( DataTypeInfo info, string sizeOrPrecision )
        {
            info.DbDataType = DbType.Int64;
            info.NetDataType = typeof(Int64);
        }

        private static void InitBinary( DataTypeInfo info, string sizeOrPrecision )
        {
            info.IsFixedSize = true;
            InitVarBinary( info, sizeOrPrecision );
        }

        private static void InitBit( DataTypeInfo info, string sizeOrPrecision )
        {
            info.DbDataType = DbType.Boolean;
            info.NetDataType = typeof(Boolean);
        }

        private static void InitChar( DataTypeInfo info, string sizeOrPrecision )
        {
            info.IsFixedSize = true;
            info.DbDataType = DbType.AnsiString;
            info.NetDataType = typeof(String);
            info.SetSize( sizeOrPrecision );
        }

        private static void InitDate( DataTypeInfo info, string sizeOrPrecision )
        {
            info.DbDataType = DbType.Date;
            info.NetDataType = typeof(DateTime);
        }

        private static void InitDateTime( DataTypeInfo info, string sizeOrPrecision )
        {
            info.DbDataType = DbType.DateTime;
            info.NetDataType = typeof(DateTime);
        }

        private static void InitDateTime2( DataTypeInfo info, string sizeOrPrecision )
        {
            info.DbDataType = DbType.DateTime2;
            info.NetDataType = typeof(DateTime);
        }

        private static void InitDateTimeOffset( DataTypeInfo info, string sizeOrPrecision )
        {
            info.DbDataType = DbType.DateTimeOffset;
            info.NetDataType = typeof(DateTimeOffset);
        }

        private static void InitFloat( DataTypeInfo info, string sizeOrPrecision )
        {
            info.DbDataType = DbType.Double;
            info.NetDataType = typeof(Double);
        }


        private static void InitInt( DataTypeInfo info, string sizeOrPrecision )
        {
            info.DbDataType = DbType.Int32;
            info.NetDataType = typeof(Int32);
        }

        private static void InitNChar( DataTypeInfo info, string sizeOrPrecision )
        {
            info.IsFixedSize = true;
            info.DbDataType = DbType.String;
            info.NetDataType = typeof(String);
            info.SetSize( sizeOrPrecision );
        }

        private static void InitNText( DataTypeInfo info, string sizeOrPrecision )
        {
            info.DbDataType = DbType.String;
            info.NetDataType = typeof(String);
            info.SetSize( sizeOrPrecision );
        }

        private static void InitReal( DataTypeInfo info, string sizeOrPrecision )
        {
            info.DbDataType = DbType.Single;
            info.NetDataType = typeof(Single);
        }

        private static void InitSmallInt( DataTypeInfo info, string sizeOrPrecision )
        {
            info.DbDataType = DbType.Int16;
            info.NetDataType = typeof(Int16);
        }

        private static void InitDecimal( DataTypeInfo info, string sizeOrPrecision )
        {
            info.DbDataType = DbType.Decimal;
            info.NetDataType = typeof(Decimal);
        }

        private static void InitObject( DataTypeInfo info, string sizeOrPrecision )
        {
            info.DbDataType = DbType.Object;
            info.NetDataType = typeof(Object);
        }

        private static void InitTime( DataTypeInfo info, string sizeOrPrecision )
        {
            info.DbDataType = DbType.Time;
            info.NetDataType = typeof(TimeSpan);
        }

        private static void InitTinyInt( DataTypeInfo info, string sizeOrPrecision )
        {
            info.DbDataType = DbType.Byte;
            info.NetDataType = typeof(Byte);
        }

        private static void InitUniqueIdentifier( DataTypeInfo info, string sizeOrPrecision )
        {
            info.DbDataType = DbType.Guid;
            info.NetDataType = typeof(Guid);
        }

        private static void InitTimeStamp( DataTypeInfo info, string sizeOrPrecision )
        {
            info.DbDataType = DbType.Binary;
            info.NetDataType = typeof(Byte[]);
            info.Size = 8;
        }

        private static void InitVarBinary( DataTypeInfo info, string sizeOrPrecision )
        {
            info.DbDataType = DbType.Binary;
            info.NetDataType = typeof(Byte[]);
            info.SetSize( sizeOrPrecision );
        }

        private static void InitVarChar( DataTypeInfo info, string sizeOrPrecision )
        {
            info.DbDataType = DbType.AnsiString;
            info.NetDataType = typeof(string);
            info.SetSize( sizeOrPrecision );
        }

        private static void InitNVarChar( DataTypeInfo info, string sizeOrPrecision )
        {
            info.DbDataType = DbType.String;
            info.NetDataType = typeof(string);
            info.SetSize( sizeOrPrecision );
        }

        private static void InitXml( DataTypeInfo info, string sizeOrPrecision )
        {
            info.DbDataType = DbType.Xml;
            info.NetDataType = typeof(string);
        }

        private void SetPrecision( string value )
        {
            value = value.Trim(new[] { '(', ')' });
            var parts = value.Split( new[] {','} );
            if (int.TryParse(parts[0], out Precision) == false)
            {
                Precision = 0;
            }
            if (int.TryParse(parts[0], out Scale) == false)
            {
                Scale = 0;
            }
        }

        private void SetSize( string sizeString )
        {
            sizeString = sizeString.Trim( new[] {'(', ')'} );
            if ( int.TryParse( sizeString, out Size ) == false )
            {
                Size = -1;
            }
        }

    }
}
