using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using SqlMemoryDb;

namespace DatabaseTests
{
    class SqlScripts
    {

        public static async Task InitDbAsync( )
        {

            await using var connection = new MemoryDbConnection( );
            connection.GetMemoryDatabase( ).Clear(  );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = SqlStatements.SqlCreateTableApplication + "\n" 
                                  + SqlStatements.SqlCreateTableApplicationFeature + "\n" 
                                  + SqlStatements.SqlCreateTableApplicationAction + "\n"
                                  + SqlStatements.SqlCreateTableTexts;
            await command.PrepareAsync( );
            await command.ExecuteNonQueryAsync( );

            command.CommandText = "INSERT INTO application ([Name],[User],[DefName]) VALUES (N'Name String', N'User String', N'DefName String')";
            await command.PrepareAsync( );
            await command.ExecuteNonQueryAsync( );
            await command.ExecuteNonQueryAsync( );
            await command.ExecuteNonQueryAsync( );

            for ( int applicationId = 1; applicationId <= 3; applicationId++ )
            {
                for ( int index = 1; index <= 4; index++ )
                {
                    command.CommandText = $"INSERT INTO application_action ([Name],[Action],[Order],[fk_application]) VALUES (N'Action {applicationId}-{index}', N'Do Something {applicationId}-{index}', {index}, {applicationId})";
                    await command.PrepareAsync( );
                    await command.ExecuteNonQueryAsync( );
                }

                command.CommandText = $"INSERT INTO application_feature ([fk_application]) VALUES ({applicationId})";
                await command.PrepareAsync( );
                await command.ExecuteNonQueryAsync( );
            }
        }

        public static void InitDb( )
        {
            var createTablesSql = SqlStatements.SqlCreateTableApplication + "\n" 
                                  + SqlStatements.SqlCreateTableApplicationFeature + "\n" 
                                  + SqlStatements.SqlCreateTableApplicationAction + "\n"
                                  + SqlStatements.SqlCreateTableTexts;

            using var connection = new MemoryDbConnection( );
            connection.GetMemoryDatabase( ).Clear(  );
            connection.Execute( createTablesSql );

            for ( int appIndex = 0; appIndex < 3; appIndex++ )
            {
                connection.Execute( "INSERT INTO application ([Name],[User],[DefName]) VALUES (N'Name String', N'User String', N'DefName String')" );
            }
            for ( int applicationId = 1; applicationId <= 3; applicationId++ )
            {
                for ( int index = 1; index <= 4; index++ )
                {
                    connection.Execute( $"INSERT INTO application_action ([Name],[Action],[Order],[fk_application]) VALUES (N'Action {applicationId}-{index}', N'Do Something {applicationId}-{index}', {index}, {applicationId})" );
                }

                connection.Execute( $"INSERT INTO application_feature ([fk_application]) VALUES ({applicationId})");
            }
        }

        public static void InitNorthWindDatabase( )
        {
            using var connection = new MemoryDbConnection( );
            connection.GetMemoryDatabase( ).Clear(  );
            connection.Execute( SqlStatements.SqlCreateNorthWindCustom );
        }
    }
}
