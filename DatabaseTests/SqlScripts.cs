using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using SqlMemoryDb;

namespace DatabaseTests
{
    class SqlScripts
    {
        public static async Task InitDbAsync( )
        {
            MemoryDbConnection.GetMemoryDatabase( ).Tables.Clear(  );

            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = SqlStatements.SqlCreateTableApplication + "\n" 
                                  + SqlStatements.SqlCreateTableApplicationFeature + "\n" 
                                  + SqlStatements.SqlCreateTableApplicationAction ;
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
    }
}
