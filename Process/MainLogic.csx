#r "System.Data"
#r "Newtonsoft.Json"

#load "CognitiveUtility.csx"
#load "DataTableUtility.csx"
#load "DateUtility.csx"
#load "FacebookUtility.csx"
#load "SqlUtility.csx"
#load "Utility.csx"

using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

public class MainLogic
{
    public static async Task<bool> PopulateAll(string sqlConn, string schema, string cognitiveKey, string client, string secret, string date)
    {
        string token = await FacebookUtility.GetAccessTokenAsync(client, secret);
        string runId = SqlUtility.InsertRunId(sqlConn, schema + ".[StagingInsert]");
        string[] pages = SqlUtility.GetPages(sqlConn, schema);
        var errorDataTable = DataTableUtility.GetErrorDataTable();
        List<JObject> posts = new List<JObject>();

        foreach(var pageToSearch in pages)
        {
            string page = pageToSearch.Replace(" ", "");
            try
            {
                var pageObj = await FacebookUtility.GetPage(page, token);
                
                 // Get Facebook Posts
                posts = await FacebookUtility.GetPostsAsync(page, date, token);

                // Get All Data Tables
                var commentsDataTable = DataTableUtility.GetCommentsDataTable();
                var hashTagDataTable = DataTableUtility.GetHashTagDataTable();
                var keyPhraseDataTable = DataTableUtility.GetKeyPhraseDataTable();
                var postDataTable = DataTableUtility.GetPostsDataTable();
                var reactionsDataTable =  DataTableUtility.GetReactionsDataTable();
                var sentimentDataTable = DataTableUtility.GetSentimentDataTable();

                PopulatePostCommentsAndReactions(postDataTable, commentsDataTable, reactionsDataTable, posts, runId, page, pageObj);

                // Populate Sentiment
                Dictionary<string, string> items = new Dictionary<string, string>();
                CognitiveUtility.PopulateDictionary(items, postDataTable);
                CognitiveUtility.PopulateDictionary(items, commentsDataTable);
                var payloads = CognitiveUtility.GetPayloads(items);

                await CognitiveUtility.GetSentimentAsync(payloads, sentimentDataTable, cognitiveKey, runId);
                await CognitiveUtility.GetKeyPhraseAsync(payloads, keyPhraseDataTable, cognitiveKey, runId);
                CognitiveUtility.GetHashTags(postDataTable, hashTagDataTable, runId);
                CognitiveUtility.GetHashTags(commentsDataTable, hashTagDataTable, runId);

                // Bulk Insert
                SqlUtility.BulkInsert(sqlConn, postDataTable, schema + "." + "StagingPosts");
                SqlUtility.BulkInsert(sqlConn, sentimentDataTable, schema + "." + "StagingSentiment");
                SqlUtility.BulkInsert(sqlConn, commentsDataTable, schema + "." + "StagingComments");
                SqlUtility.BulkInsert(sqlConn, keyPhraseDataTable, schema + "." + "StagingKeyPhrase");
                SqlUtility.BulkInsert(sqlConn, reactionsDataTable, schema + "." + "StagingReactions");
                SqlUtility.BulkInsert(sqlConn, hashTagDataTable, schema + "." + "StagingHashTags");
            }
            catch(Exception e)
            {    
                DataRow errorRow = errorDataTable.NewRow();
                errorRow["Date"] = date;
                errorRow["Error"] = page + e.Message;
                errorRow["Posts"] = JToken.FromObject(posts).ToString();
                errorDataTable.Rows.Add(errorRow);
                SqlUtility.BulkInsert(sqlConn, errorDataTable, schema + "." + "StagingError");
                throw;
            }
        }
        return true;
    }

    public static void PopulatePostCommentsAndReactions(DataTable postsDataTable, DataTable commentsDataTable, 
        DataTable reactionsDataTable, List<JObject> posts, string runId, string page, JObject pageObj)
    {
        foreach (var postPayload in posts)
        {
            foreach (var post in postPayload["data"])
            {

                string[] split = post["id"].ToString().Split('_');
                DataRow postRow = postsDataTable.NewRow();
                postRow["Id1"] = Utility.ConvertToLong(split[0]);
                if(split.Length > 1)
                {
                    postRow["Id2"] = Utility.ConvertToLong(split[1]);
                }
                postRow["Original Id"] = post["id"];
                postRow["Created Date"] = post["created_time"];
                postRow["Message"] = post["message"];
                postRow["From Id"] = Utility.ConvertToLong(post["from"]["id"]);
                
                int maxLength = Math.Min(post["from"]["name"].ToString().Length, 100);
                postRow["From Name"] = post["from"]["name"].ToString().Substring(0, maxLength);
               
                postRow["Media"] = post["picture"];
                postRow["Total Likes"] = Utility.ConvertToLong(post["likes"]["summary"]["total_count"]);
                postRow["Total Reactions"] = Utility.ConvertToLong(post["reactions"]["summary"]["total_count"]);
                postRow["Total Comments"] = Utility.ConvertToLong(post["comments"]["summary"]["total_count"]);
                postRow["Total Shares"] = DBNull.Value;
                postRow["Page"] = page;
                postRow["PageId"] = pageObj["id"];
                postRow["PageDisplayName"] = pageObj["name"];
                postRow["BulkInsertId"] = runId;
                postsDataTable.Rows.Add(postRow);


                foreach (var comment in post["comments"]["data"])
                {
                    string[] split2 = comment["id"].ToString().Split('_');
                    DataRow commentRow = commentsDataTable.NewRow();
                    commentRow["Id1"] = Utility.ConvertToLong(split2[0]);
                     if (split2.Length > 1)
                    {
                        commentRow["Id2"] = Utility.ConvertToLong(split2[1]);
                    }
                    
                    commentRow["Original Id"] = comment["id"];
                    commentRow["Created Date"] = comment["created_time"];
                    commentRow["Message"] = comment["message"];
                    commentRow["From Id"] = Utility.ConvertToLong(comment["from"]["id"]);
                   

                    maxLength = Math.Min(comment["from"]["name"].ToString().Length, 100);
                    commentRow["From Name"] = comment["from"]["name"].ToString().Substring(0, maxLength);
                    commentRow["BulkInsertId"] = runId;
                    commentRow["Post Id1"] = Utility.ConvertToLong(split[0]);
                    commentRow["Post Id2"] = Utility.ConvertToLong(split[1]);
                    commentRow["Original Post Id"] = post["id"];
                    commentRow["Page"] = page;
                    commentRow["PageId"] = pageObj["id"];
                    commentRow["PageDisplayName"] = pageObj["name"];
                    commentsDataTable.Rows.Add(commentRow);
                }

                foreach (var reaction in post["reactions"]["data"])
                {
                    DataRow reactionsRow = reactionsDataTable.NewRow();
                    reactionsRow["Id1"] = Utility.ConvertToLong(split[0]);
                    if (split.Length > 1)
                    {
                        reactionsRow["Id2"] = Utility.ConvertToLong(split[1]);
                    }
                    reactionsRow["Original Id"] = post["id"];
                    reactionsRow["Reaction Type"] = reaction["type"];
                    reactionsRow["From Id"] = Utility.ConvertToLong(reaction["id"]);

                    maxLength = Math.Min(reaction["name"].ToString().Length, 100);
                    reactionsRow["From Name"] = reaction["name"].ToString().Substring(0, maxLength);
                  
                    reactionsRow["BulkInsertId"] = runId;
                    reactionsDataTable.Rows.Add(reactionsRow);
                }
            }
        }
    }
}

