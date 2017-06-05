#r "System.Data"
using System;
using System.Data;
using System.Data.SqlClient;

public class SqlUtility
{
    public static string InsertRunId(string sqlConnectionString, string tableName)
    {
        using (SqlConnection conn = new SqlConnection(sqlConnectionString))
        {
            conn.Open();
            SqlCommand command = new SqlCommand($"INSERT INTO {tableName} OUTPUT inserted.id DEFAULT VALUES");
            command.Connection = conn;
            var dataReader = command.ExecuteReader();
            dataReader.Read();
            string runId = dataReader[0].ToString();
            conn.Close();
            return runId;
        }
    }

    public static void BulkInsert(string connString, DataTable table, string tableName)
    {
        try
        {
            using (SqlBulkCopy bulk = new SqlBulkCopy(connString))
            {
                bulk.BatchSize = 1000;
                bulk.DestinationTableName = tableName;
                bulk.WriteToServer(table);
                bulk.Close();
            }
        }
        catch
        {
            throw new Exception("overflow during batch insert in table " + tableName);
        }
    }

    public static void RunStoredProcWithRunId(string sqlConnectionString, string storedProc, string runId)
    {
        using (SqlConnection conn = new SqlConnection(sqlConnectionString))
        {
            using (
                SqlCommand command = new SqlCommand(storedProc, conn)
                {
                    CommandType = CommandType.StoredProcedure
                })
            {
                conn.Open();
                command.Parameters.Add(new SqlParameter("runId", runId));
                var obj = command.ExecuteScalar();
                 conn.Close();
            }
        }
    }

    public static string[] GetPages(string sqlConnectionString, string schema)
    {
        string pagesCommaSeperated = null;
        using (SqlConnection conn = new SqlConnection(sqlConnectionString))
        {
            conn.Open();
            SqlCommand command = new SqlCommand($"SELECT TOP 1 [value] FROM {schema}.[configuration] WHERE [configuration_group] = 'SolutionTemplate' AND [configuration_subgroup] = 'ETL' AND [name] = 'PagesToFollow'");
            command.Connection = conn;
            var dataReader = command.ExecuteReader();
            dataReader.Read();
            pagesCommaSeperated = dataReader[0].ToString();
            conn.Close();
        }

        return pagesCommaSeperated.Split(',');
    }
}

