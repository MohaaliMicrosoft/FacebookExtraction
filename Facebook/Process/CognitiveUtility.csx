#r "Newtonsoft.Json"

#load "Utility.csx"

using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

public class CognitiveUtility
{
    public static void PopulateDictionary(Dictionary<string, string> dictionary, DataTable table)
    {
        foreach (DataRow row in table.Rows)
        {
            if(!dictionary.ContainsKey(row["Original Id"].ToString()) && !string.IsNullOrEmpty(row["message"]?.ToString()))
            {
                dictionary.Add(row["Original Id"].ToString(), row["message"].ToString());
            }
        }
    }

    public static List<JObject> GetPayloads(Dictionary<string, string> dictionary)
    {
        JObject objTemp = new JObject();
        List<JObject> objDocArray = new List<JObject>();
        var objArray = new JArray();

        for (int i = 0; i < dictionary.Count; i++)
        {
            var item = dictionary.Keys.ElementAt(i);

            if (i % 1000 == 0 && i != 0)
            {
                objTemp.Add("documents", objArray);
                objArray = new JArray();
                objDocArray.Add(objTemp);
                objTemp = new JObject();
            }

            if (!(string.IsNullOrEmpty(item) && string.IsNullOrEmpty(dictionary[item]) && string.IsNullOrWhiteSpace(dictionary[item])))
            {
                JObject doc = new JObject();
                doc.Add("id", item);
                doc.Add("text", dictionary[item]);
                objArray.Add(doc);
            }
        }

        if (objArray.Count > 0)
        {
            objTemp.Add("documents", objArray);
            objDocArray.Add(objTemp);
        }

        return objDocArray;
    }

    public static async Task GetSentimentAsync(List<JObject> payloads, DataTable sentiment, string cognitiveKey, string runId)
    {
        foreach (var payload in payloads)
        {
            HttpClient client = new HttpClient();
            client.DefaultRequestHeaders.Add("Ocp-Apim-Subscription-Key", cognitiveKey);
            HttpContent content = new StringContent(payload.ToString(), System.Text.Encoding.UTF8, "application/json");
            var response = await client.PostAsync("https://westus.api.cognitive.microsoft.com/text/analytics/v2.0/sentiment", content);
            string result = await response.Content.ReadAsStringAsync();
            if (response.IsSuccessStatusCode)
            {
                JObject responseObj = JObject.Parse(result);

                foreach (var doc in responseObj["documents"])
                {
                    string[] split = doc["id"].ToString().Split('_');
                    DataRow row = sentiment.NewRow();
                    row["Id1"] = Utility.ConvertToLong(split[0]);
                    row["Id2"] = Utility.ConvertToLong(split[1]);
                    row["Original Id"] = doc["id"];
                    row["BulkInsertId"] = runId;
                    row["Sentiment"] = double.Parse(doc["score"].ToString());
                    sentiment.Rows.Add(row);
                }
            }
        }
    }

    public static async Task GetKeyPhraseAsync(List<JObject> payloads, DataTable keyPhrase, string cognitiveKey, string runId)
    {
        foreach (var payload in payloads)
        {
            HttpClient client = new HttpClient();
            client.DefaultRequestHeaders.Add("Ocp-Apim-Subscription-Key", cognitiveKey);
            HttpContent content = new StringContent(payload.ToString(), System.Text.Encoding.UTF8, "application/json");
            var response = await client.PostAsync("https://westus.api.cognitive.microsoft.com/text/analytics/v2.0/keyPhrases", content);
            if (response.IsSuccessStatusCode)
            {
                string result = await response.Content.ReadAsStringAsync();
                JObject responseObj = JObject.Parse(result);
                foreach (var doc in responseObj["documents"])
                {
                    foreach (var keyword in doc["keyPhrases"])
                    {
                        if (string.IsNullOrEmpty(keyword?.ToString()))
                        {
                            continue;
                        }

                        string[] split = doc["id"].ToString().Split('_');
                        DataRow row = keyPhrase.NewRow();
                        row["Id1"] = Utility.ConvertToLong(split[0]);
                        row["Id2"] = Utility.ConvertToLong(split[1]);
                        row["Original Id"] = doc["id"];
                        row["BulkInsertId"] = runId;

                        int maxLength = Math.Min(keyword.ToString().Length, 100);
                        row["KeyPhrase"] = keyword.ToString().ToLowerInvariant().Substring(0, maxLength);
                        keyPhrase.Rows.Add(row);
                    }
                }
            }
        }
    }

    public static void GetHashTags(DataTable postsOrComments, DataTable hashTagsDataTable,  string runId)
    {
        foreach (DataRow post in postsOrComments.Rows)
        {
            var hashTags = Utility.ExtractHashTag(post["message"]?.ToString());
            foreach (var hashTag in hashTags)
            {
                string[] split = post["Original Id"].ToString().Split('_');
                DataRow row = hashTagsDataTable.NewRow();
                row["Id1"] = Utility.ConvertToLong(split[0]);
                row["Id2"] = Utility.ConvertToLong(split[1]);
                row["Original Id"] = post["Original Id"];
                row["BulkInsertId"] = runId;
                int maxLength = Math.Min(hashTag.ToString().Length, 100);
                row["HashTags"] = hashTag.ToString().ToLowerInvariant().Substring(0, maxLength);
                hashTagsDataTable.Rows.Add(row);
            }
        }
    }
}
