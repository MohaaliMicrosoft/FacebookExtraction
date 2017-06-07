#r "Newtonsoft.Json"
#load "DateUtility.csx"

using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;

public class FacebookUtility
{
    public static async Task<JObject> GetPage(string page, string accessToken)
    {
        string requestUri = $"https://graph.facebook.com/{page}?access_token={accessToken}";
        HttpClient client = new HttpClient();
        var response = await client.GetAsync(requestUri);
        string responseObj = await response.Content.ReadAsStringAsync();
        return JObject.Parse(responseObj);
    }
    
    public static async Task<List<JObject>> GetPostsAsync(string page, string untilDateTime, string accessToken)
    {
        List<JObject> posts = new List<JObject>();
        JObject post = null;
        string until = DateUtility.GetUnixFromDate(untilDateTime);
        string since = DateUtility.GetUnixFromDate(DateUtility.GetDateTimeRelativeFromNow(untilDateTime, -1));
        string requestUri = $"https://graph.facebook.com/{page}/feed?access_token={accessToken}&fields=message%2Cupdated_time%2Ccreated_time%2Cstory%2Cshares%2Ccomments.limit%28100%29.summary%28true%29%2Clikes.limit%280%29.summary%28true%29%2Creactions.limit%28100%29.summary%28true%29%2Cfrom%2Cpicture&limit=25&until={until}&since={since}";

        do
        {
            HttpClient client = new HttpClient();
            var response = await client.GetAsync(requestUri);
            string responseObj = await response.Content.ReadAsStringAsync();
            if (!response.IsSuccessStatusCode)
            {
                throw new Exception();
            }


            post = JObject.Parse(responseObj);
            posts.Add(post);

            if(post?["paging"] != null)
            {
                requestUri = post["paging"]["next"].ToString();
            }
        }
        while (post != null && post?["paging"] != null);

        return posts;

    }

    public static async Task<string> GetAccessTokenAsync(string clientId, string clientSecret)
    {
        string requestUri = $"https://graph.facebook.com/oauth/access_token?grant_type=client_credentials&client_id={clientId}&client_secret={clientSecret}";
        HttpClient client = new HttpClient();
        var response = await client.GetAsync(requestUri);
        string responseObj = await response.Content.ReadAsStringAsync();
        if (!response.IsSuccessStatusCode)
        {
            throw new Exception();
        }

        string accessToken = JObject.Parse(responseObj)["access_token"].ToString();
        return accessToken;
    }
}

