using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Helpers;

namespace SqlMemoryDb.SelectData
{
    class SelectDataFromFunctionDate : ISelectDataFunction
    {
        public bool IsAggregate => false;
        Type ISelectDataFunction.ReturnType => _ReturnType;
        string ISelectDataFunction.DbType => _DbType;
        
        private readonly Type _ReturnType = typeof(DateTime);
        private readonly string _DbType = "datetime";

        private readonly SqlBuiltinScalarFunctionCallExpression _FunctionCall;
        private readonly RawData _RawData;

        public SelectDataFromFunctionDate( SqlBuiltinScalarFunctionCallExpression functionCall, RawData rawData, SelectDataFunctionInfo info )
        {
            _FunctionCall = functionCall;
            _RawData = rawData;
            if ( string.IsNullOrWhiteSpace( info.ReturnDbType ) == false )
            {
                _ReturnType = info.ReturnType;
                _DbType = info.ReturnDbType;
            }
        }

        public object Select( List<RawData.RawDataRow> rows )
        {
            switch ( _FunctionCall.FunctionName.ToUpper( ) )
            {
                case "GETDATE": return DateTime.Now;
                case "DATEADD": return FunctionDateAdd( rows );
                case "DATEDIFF": return FunctionDateDiff( rows );
                case "DATENAME": return FunctionDateName( rows );
                case "DATEPART": return FunctionDatePart( rows );
                default:
                    throw new NotImplementedException();
            }
        }

        private DateTime? FunctionDateAdd( List<RawData.RawDataRow> rows )
        {
            var date = (DateTime?)Helper.GetValue( _FunctionCall.Arguments[2], typeof( DateTime ), _RawData, rows );
            if ( date == null )
            {
                return null;
            }
            int increment = ( int ) Helper.GetValue( _FunctionCall.Arguments[1], typeof( int ), _RawData, rows );
            string datePart = _FunctionCall.Arguments[0].Sql;
            switch ( datePart.ToLower() )
            {
                case "year":
                case "yyyy":
                case "yy":
                    return date.Value.AddYears( increment );
                case "quarter":
                case "qq":
                case "q":
                    return date.Value.AddMonths( increment * 3 );
                case "month":
                case "mm":
                case "m":
                    return date.Value.AddMonths( increment );
                case "dayofyear":
                case "day":
                case "dy":
                case "y":
                case "d":
                    return date.Value.AddDays( increment );
                case "week":
                case "ww":
                case "wk":
                    return date.Value.AddDays( increment * 7 );
                case "weekday":
                case "dw":
                case "w":
                    return date.Value.AddDays( increment );
                case "hour":
                case "hh":
                    return date.Value.AddHours( increment );
                case "minute":
                case "mi":
                case "n":
                    return date.Value.AddMinutes( increment );
                case "second":
                case "ss":
                case "s":
                    return date.Value.AddSeconds( increment );
                case "millisecond":
                case "ms":
                    return date.Value.AddMilliseconds( increment );
                case "microsecond":
                case "mcs":
                    return date.Value.AddMilliseconds( (double)increment / 10 );
                case "nanosecond":
                case "ns":
                    return date.Value.AddMilliseconds( (double)increment / 100 );
                default:
                    throw new NotImplementedException( $"Datepart {datePart} is not implemented for DateAdd()");
            }
        }

        private int? FunctionDateDiff( List<RawData.RawDataRow> rows )
        {
            var date1 = (DateTime?)Helper.GetValue( _FunctionCall.Arguments[1], typeof( DateTime ), _RawData, rows );
            if ( date1 == null )
            {
                return null;
            }
            var date2 = (DateTime?)Helper.GetValue( _FunctionCall.Arguments[2], typeof( DateTime ), _RawData, rows );
            if ( date2 == null )
            {
                return null;
            }
            string datePart = _FunctionCall.Arguments[0].Sql;
            switch ( datePart.ToLower() )
            {
                case "year":
                case "yyyy":
                case "yy":
                    return date2.Value.Year - date1.Value.Year;
                case "quarter":
                case "qq":
                case "q":
                    return (((date2.Value.Year - date1.Value.Year) * 12) + date2.Value.Month - date1.Value.Month) / 3;
                case "month":
                case "mm":
                case "m":
                    return ((date2.Value.Year - date1.Value.Year) * 12) + date2.Value.Month - date1.Value.Month;
                case "dayofyear":
                case "day":
                case "dy":
                case "y":
                case "d":
                case "weekday":
                case "dw":
                case "w":
                    return (date2.Value - date1.Value).Days;
                case "week":
                case "ww":
                case "wk":
                    return (date2.Value - date1.Value).Days / 7;
                case "hour":
                case "hh":
                    return (int)((date2.Value - date1.Value).TotalHours);
                case "minute":
                case "mi":
                case "n":
                    return (int)((date2.Value - date1.Value).TotalMinutes);
                case "second":
                case "ss":
                case "s":
                    return (int)((date2.Value - date1.Value).TotalSeconds);
                case "millisecond":
                case "ms":
                    return (int)((date2.Value - date1.Value).TotalMilliseconds);
                case "microsecond":
                case "mcs":
                    return (int)((date2.Value - date1.Value).TotalMilliseconds * 10);
                case "nanosecond":
                case "ns":
                    return (int)((date2.Value - date1.Value).TotalMilliseconds * 100);
                default:
                    throw new NotImplementedException( $"Datepart {datePart} is not implemented for DateDiff()");
            }
        }

        private string FunctionDateName( List<RawData.RawDataRow> rows )
        {
            var date1 = (DateTime?)Helper.GetValue( _FunctionCall.Arguments[1], typeof( DateTime ), _RawData, rows );
            if ( date1 == null )
            {
                return null;
            }
            string datePart = _FunctionCall.Arguments[0].Sql;
            switch ( datePart.ToLower() )
            {
                case "year":
                case "yyyy":
                case "yy":
                    return date1.Value.Year.ToString();
                case "quarter":
                case "qq":
                case "q":
                    return (1 + (date1.Value.Month / 3)).ToString();
                case "month":
                case "mm":
                case "m":
                    return date1.Value.ToString( "MMMM", CultureInfo.InvariantCulture );
                case "weekday":
                case "dw":
                    return date1.Value.ToString("dddd", CultureInfo.InvariantCulture );
                case "dayofyear":
                case "day":
                case "dy":
                case "y":
                case "d":
                    return date1.Value.Day.ToString();
                case "week":
                case "ww":
                case "wk":
                    return CultureInfo.InvariantCulture.Calendar.GetWeekOfYear(date1.Value, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday).ToString();
                case "iso_week":
                case "isowk":
                case "isoww":
                    return GetIso8601WeekOfYear( date1.Value ).ToString( );
                case "hour":
                case "hh":
                    return date1.Value.Hour.ToString();
                case "minute":
                case "mi":
                case "n":
                    return date1.Value.Minute.ToString();
                case "second":
                case "ss":
                case "s":
                    return date1.Value.Second.ToString();
                case "millisecond":
                case "ms":
                    return date1.Value.Millisecond.ToString();
                default:
                    throw new NotImplementedException( $"Datepart {datePart} is not implemented for DateName()");
            }
        }

        private int? FunctionDatePart( List<RawData.RawDataRow> rows )
        {
            var date1 = (DateTime?)Helper.GetValue( _FunctionCall.Arguments[1], typeof( DateTime ), _RawData, rows );
            if ( date1 == null )
            {
                return null;
            }
            string datePart = _FunctionCall.Arguments[0].Sql;
            switch ( datePart.ToLower() )
            {
                case "year":
                case "yyyy":
                case "yy":
                    return date1.Value.Year;
                case "quarter":
                case "qq":
                case "q":
                    return (1 + (date1.Value.Month / 3));
                case "month":
                case "mm":
                case "m":
                    return date1.Value.Month;
                case "weekday":
                case "dw":
                case "dayofyear":
                case "day":
                case "dy":
                case "y":
                case "d":
                    return date1.Value.Day;
                case "week":
                case "ww":
                case "wk":
                    return CultureInfo.InvariantCulture.Calendar.GetWeekOfYear(date1.Value, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
                case "iso_week":
                case "isowk":
                case "isoww":
                    return GetIso8601WeekOfYear( date1.Value );
                case "hour":
                case "hh":
                    return date1.Value.Hour;
                case "minute":
                case "mi":
                case "n":
                    return date1.Value.Minute;
                case "second":
                case "ss":
                case "s":
                    return date1.Value.Second;
                case "millisecond":
                case "ms":
                    return date1.Value.Millisecond;
                default:
                    throw new NotImplementedException( $"Datepart {datePart} is not implemented for DateName()");
            }
        }

        // This presumes that weeks start with Monday.
        // Week 1 is the 1st week of the year with a Thursday in it.
        public static int GetIso8601WeekOfYear(DateTime time)
        {
            // Seriously cheat.  If its Monday, Tuesday or Wednesday, then it'll 
            // be the same week# as whatever Thursday, Friday or Saturday are,
            // and we always get those right
            DayOfWeek day = CultureInfo.InvariantCulture.Calendar.GetDayOfWeek(time);
            if (day >= DayOfWeek.Monday && day <= DayOfWeek.Wednesday)
            {
                time = time.AddDays(3);
            }

            // Return the week of our adjusted day
            return CultureInfo.InvariantCulture.Calendar.GetWeekOfYear(time, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
        } 

        public object Select( List<List<RawData.RawDataRow>> rows )
        {
            throw new NotImplementedException( );
        }
    }
}
