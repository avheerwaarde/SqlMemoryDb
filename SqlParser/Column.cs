using System;
using System.Collections.Generic;
using System.Data;
using System.Text.RegularExpressions;

namespace SqlMemoryDb
{
    public class Column
    {
        public class ColumnIdentity
        {
            public int Seed;
            public int Increment;
        }

        public string Name { get ; set ; }
        public DbType DbDataType { get ; set ; }
        public Type NetDataType { get ; set ; }
        public int Precision;
        public int Scale;
        public int Size;

        public ColumnIdentity Identity;

        public bool IsNullable { get ; set ; }
        public string DefaultValue { get ; set ; }
        public bool IsFixedSize { get ; set ; }

        public bool IsIdentity => Identity != null;
        public bool HasDefault => string.IsNullOrWhiteSpace( DefaultValue );
        public bool IsUnique { get ; set ; }
        public bool IsPrimaryKey { get ; set ; }

        private Dictionary<string, Action<Column, string>> _DataTypes = new Dictionary<string, Action<Column, string>>
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


        public Column( string name, string sqlType )
        {
            Name = name;
            InitDbType( sqlType );
            IsNullable = true;
        }

        private void InitDbType( string sqlType )
        {
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


        private static void InitBigInt( Column column, string sizeOrPrecision )
        {
            column.DbDataType = DbType.Int64;
            column.NetDataType = typeof(Int64);
        }

        private static void InitBinary( Column column, string sizeOrPrecision )
        {
            column.IsFixedSize = true;
            InitVarBinary( column, sizeOrPrecision );
        }

        private static void InitBit( Column column, string sizeOrPrecision )
        {
            column.DbDataType = DbType.Boolean;
            column.NetDataType = typeof(Boolean);
        }

        private static void InitChar( Column column, string sizeOrPrecision )
        {
            column.IsFixedSize = true;
            column.DbDataType = DbType.AnsiString;
            column.NetDataType = typeof(String);
            column.SetSize( sizeOrPrecision );
        }

        private static void InitDate( Column column, string sizeOrPrecision )
        {
            column.DbDataType = DbType.Date;
            column.NetDataType = typeof(DateTime);
        }

        private static void InitDateTime( Column column, string sizeOrPrecision )
        {
            column.DbDataType = DbType.DateTime;
            column.NetDataType = typeof(DateTime);
        }

        private static void InitDateTime2( Column column, string sizeOrPrecision )
        {
            column.DbDataType = DbType.DateTime2;
            column.NetDataType = typeof(DateTime);
        }

        private static void InitDateTimeOffset( Column column, string sizeOrPrecision )
        {
            column.DbDataType = DbType.DateTimeOffset;
            column.NetDataType = typeof(DateTimeOffset);
        }

        private static void InitFloat( Column column, string sizeOrPrecision )
        {
            column.DbDataType = DbType.Double;
            column.NetDataType = typeof(Double);
        }


        private static void InitInt( Column column, string sizeOrPrecision )
        {
            column.DbDataType = DbType.Int32;
            column.NetDataType = typeof(Int32);
        }

        private static void InitNChar( Column column, string sizeOrPrecision )
        {
            column.IsFixedSize = true;
            column.DbDataType = DbType.String;
            column.NetDataType = typeof(String);
            column.SetSize( sizeOrPrecision );
        }

        private static void InitNText( Column column, string sizeOrPrecision )
        {
            column.DbDataType = DbType.String;
            column.NetDataType = typeof(String);
            column.SetSize( sizeOrPrecision );
        }

        private static void InitReal( Column column, string sizeOrPrecision )
        {
            column.DbDataType = DbType.Single;
            column.NetDataType = typeof(Single);
        }

        private static void InitSmallInt( Column column, string sizeOrPrecision )
        {
            column.DbDataType = DbType.Int16;
            column.NetDataType = typeof(Int16);
        }

        private static void InitDecimal( Column column, string sizeOrPrecision )
        {
            column.DbDataType = DbType.Decimal;
            column.NetDataType = typeof(Decimal);
        }

        private static void InitObject( Column column, string sizeOrPrecision )
        {
            column.DbDataType = DbType.Object;
            column.NetDataType = typeof(Object);
        }

        private static void InitTime( Column column, string sizeOrPrecision )
        {
            column.DbDataType = DbType.Time;
            column.NetDataType = typeof(TimeSpan);
        }

        private static void InitTinyInt( Column column, string sizeOrPrecision )
        {
            column.DbDataType = DbType.Byte;
            column.NetDataType = typeof(Byte);
        }

        private static void InitUniqueIdentifier( Column column, string sizeOrPrecision )
        {
            column.DbDataType = DbType.Guid;
            column.NetDataType = typeof(Guid);
        }

        private static void InitTimeStamp( Column column, string sizeOrPrecision )
        {
            column.DbDataType = DbType.Binary;
            column.NetDataType = typeof(Byte[]);
            column.Size = 8;
        }

        private static void InitVarBinary( Column column, string sizeOrPrecision )
        {
            column.DbDataType = DbType.Binary;
            column.NetDataType = typeof(Byte[]);
            column.SetSize( sizeOrPrecision );
        }

        private static void InitVarChar( Column column, string sizeOrPrecision )
        {
            column.DbDataType = DbType.AnsiString;
            column.NetDataType = typeof(string);
            column.SetSize( sizeOrPrecision );
        }

        private static void InitNVarChar( Column column, string sizeOrPrecision )
        {
            column.DbDataType = DbType.String;
            column.NetDataType = typeof(string);
            column.SetSize( sizeOrPrecision );
        }

        private static void InitXml( Column column, string sizeOrPrecision )
        {
            column.DbDataType = DbType.Xml;
            column.NetDataType = typeof(string);
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
