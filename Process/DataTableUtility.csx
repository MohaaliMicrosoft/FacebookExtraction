using Newtonsoft.Json.Linq;
using System;
using System.Data;

public class DataTableUtility
{
    public static DataTable GetCommentsDataTable()
    {
        DataTable table = new DataTable();
        table.Columns.Add("Id1", typeof(long));
        table.Columns.Add("Id2", typeof(long));
        table.Columns.Add("Original Id");
        table.Columns.Add("Created Date", typeof(DateTime));
        table.Columns.Add("Message");
        table.Columns.Add("From Id", typeof(long));
        table.Columns.Add("From Name");
        table.Columns.Add("BulkInsertId", typeof(int));
        table.Columns.Add("Post Id1", typeof(long));
        table.Columns.Add("Post Id2", typeof(long));
        table.Columns.Add("Original Post Id");
         table.Columns.Add("Page");
        return table;
    }

    public static DataTable GetReactionsDataTable()
    {
        DataTable table = new DataTable();
        table.Columns.Add("Id1", typeof(long));
        table.Columns.Add("Id2", typeof(long));
        table.Columns.Add("Original Id");
        table.Columns.Add("Reaction Type");
        table.Columns.Add("From Id", typeof(long));
        table.Columns.Add("From Name");
        table.Columns.Add("BulkInsertId", typeof(int));
        return table;
    }

    public static DataTable GetPostsDataTable()
    {
        DataTable table = new DataTable();
        table.Columns.Add("Id1", typeof(long));
        table.Columns.Add("Id2", typeof(long));
        table.Columns.Add("Original Id");
        table.Columns.Add("Created Date", typeof(DateTime));
        table.Columns.Add("Message");
        table.Columns.Add("From Id", typeof(long));
        table.Columns.Add("From Name");
        table.Columns.Add("Media");
        table.Columns.Add("Total Likes", typeof(int));
        table.Columns.Add("Total Shares", typeof(int));
        table.Columns.Add("Total Reactions", typeof(int));
        table.Columns.Add("Page");
        table.Columns.Add("Total Comments", typeof(int));
        table.Columns.Add("BulkInsertId", typeof(int));
        return table;
    }


    public static DataTable GetSentimentDataTable()
    {
        DataTable table = new DataTable();
        table.Columns.Add("Id1", typeof(long));
        table.Columns.Add("Id2", typeof(long));
        table.Columns.Add("Original Id");
        table.Columns.Add("BulkInsertId");
        table.Columns.Add("Sentiment", typeof(float));
        return table;
    }

    public static DataTable GetKeyPhraseDataTable()
    {
        DataTable table = new DataTable();
        table.Columns.Add("Id1", typeof(long));
        table.Columns.Add("Id2", typeof(long));
        table.Columns.Add("Original Id");
        table.Columns.Add("BulkInsertId");
        table.Columns.Add("KeyPhrase");
        return table;
    }

    public static DataTable GetHashTagDataTable()
    {
        DataTable table = new DataTable();
        table.Columns.Add("Id1", typeof(long));
        table.Columns.Add("Id2", typeof(long));
        table.Columns.Add("Original Id");
        table.Columns.Add("BulkInsertId");
        table.Columns.Add("HashTags");
        return table;
    }

    public static DataTable GetErrorDataTable()
    {
        DataTable table = new DataTable();
        table.Columns.Add("Date", typeof(DateTime));
        table.Columns.Add("Error");
        table.Columns.Add("Posts");
        return table;
    }
}

